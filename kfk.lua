local util = require "util";

local conf = require "lkfk.conf";
local broker = require "lkfk.broker";
local meta = require "lkfk.meta";
local topic = require "lkfk.topic";
local func = require "lkfk.func";
local const = require "lkfk.const";
local list = require "lkfk.list";
local msg = require "lkfk.msg";

local pairs = pairs;
local ipairs = ipairs;
local setmetatable = setmetatable;

local ngxspawn = ngx.thread.spawn;
local costatus = coroutine.status
local threadwait = ngx.thread.wait
local ngxlog = ngx.log;
local ngxnow = ngx.now

local _M = util.new_tab(4, 8);
_M._VERSION = "1.0";

local function init_conf(cf)
	local default = conf.default_conf;
	
	for k, v in pairs(cf) do
		if not default[k] then
			return nil, "not valid kafka conf: " .. k;
		end
	end
	
	for k, v in pairs(default) do
		if not cf[k] then
			cf[k] = v;
		end		
	end
	return cf;
end

local mt = {__index = _M};
-- public api

for k, v in pairs(conf.level) do
	_M[k] = v;
end

function _M.new(cf)
	local cf, err = init_conf(cf or {});
	if not cf then
		return nil, err;
	end

    local fp, err = io.open(cf.backlog, "a+");
    if not fp then
        return nil, err;
    end
    fp:setvbuf("line");

	local kfk = {
		cf = cf,
       	
        fp = fp,
        tcp = ngx.socket.tcp(),
		
		quit = ngx.worker.exiting,
		-- all topics list
		kfk_topics = util.new_tab(4, 0),
		topic_cnt = 0,
		-- all broker list
		kfk_brokers = list.new("kfk_broker_link"),
		down_cnt = 0,
		
		threads = list.new("kfk_thread_link"),
        new_brokers = util.new_tab(4, 0),
        meta_query_topic = util.new_tab(4, 0),

		metadata_refresh_timeout = ngxnow() + cf.metadata_refresh_interval_ms / 1000,
        
		msg_cnt = 0,
        succ_msg_cnt = 0,
        fail_msg_cnt = 0

	};
	
	-- init broker
	local brokers = {};
	for _, tmp in ipairs(cf.metadata_broker_list) do
		local ip_port = util.split(tmp, ":");
		if ip_port[1] and tonumber(ip_port[2]) then
			brokers[#brokers + 1] = {
				host = ip_port[1],
				port = ip_port[2],
				nodeid = const.KFK_BROKER_ID_UA 
			}
		end
	end
	local cnt = broker.kfk_broker_adds(kfk, brokers);
	if cnt <= 0 then
		return nil, "broker is invalid: please check the config of metadata_broker_list";
	end
    
    if util.debug then
        ngxlog(ngx.DEBUG, "kfk start up");
    end

	-- init topic
	for _, name in ipairs(cf.topics) do
		topic.kfk_new_topic(kfk, name);
	end
	
	kfk.meta_query_topic = kfk.kfk_topics;

    --init shared ditc
    local fp = io.open("kfk.shr", "r");
    if fp then
        for line in fp:lines() do
            local k, v = unpack(util.split(line));
            ngx.shared.kfk:set(k, v);
        end
        fp:close();
    end

	return setmetatable(kfk, mt);
end


local function _kfk_destroy_kfk(kfk)
    for _, kfk_topic in ipairs(kfk.kfk_topics) do
        topic.kfk_destroy_topic(kfk_topic, const.KFK_RSP_ERR_DESTROY);
    end
    
    if kfk.cf.kfk_status then
        local buf = {};
        local fp = io.open("kfk.shr", "w");
        local keys = ngx.shared.kfk:get_keys();
        for _, key in ipairs(keys) do
            buf[#buf + 1] = key .. " " .. ngx.shared.kfk:get(key);
        end
        fp:write(table.concat(buf, "\n"));
        fp:close();
    end

    kfk.kfk_topics = nil;
    kfk.kfk_brokers = nil;
end

local function _check_runerror(threads, kfk_thread)
    local kfk_broker = kfk_thread.kfk_broker;
    ngxlog(ngx.ERR, kfk_broker.name, " thread end of failure");

    kfk_broker.fails = kfk_broker.fails + 1;
    kfk_broker.last_fail_time = ngxnow();
    if kfk_broker.fails < 3 then
        broker.kfk_broker_resume(kfk_thread);
    else
        broker.kfk_broker_produce_fail(kfk_broker, const.KFK_RSP_ERR_DESTROY,
                                    "lua abort: runtime error");
        threads:remove(kfk_thread);
    end

end

local function _check_zombie(kfk, quit, errcall)
    local threads = kfk.threads;
    local head = threads.head;
    local thread = head[threads.key].next;
    while thread ~= head do

        local tmp = threads:next(thread);
        if quit or costatus(thread.co) == "zombie" then
            local ok, res = threadwait(thread.co)
            if not ok and errcall then
                errcall(threads, thread);
            else
                threads:remove(thread);
            end	
        end
        thread = tmp;
    end
end

function _M.loop(self)
    local md_flush = self.cf.metadata_refresh_interval_ms / 1000;
    local k = math.floor(md_flush);
    local i = 2;
	while not self.quit() do
        local n = 0;

        if self.metadata_refresh_timeout <= ngxnow() then
            self.metadata_refresh_timeout = ngxnow() + md_flush;
            self.meta_query_topic = self.kfk_topics;
        end

        if #self.new_brokers > 0 then
            broker.kfk_broker_adds(self, self.new_brokers);
            self.new_brokers = {};
            n = n + 1;
        end
        
        if #self.meta_query_topic > 0 then
            n = n + 1;
            meta.kfk_metadata_req(self);
        end

        for _, kfk_topic in ipairs(self.kfk_topics) do
            n = n + topic.kfk_topic_assign_ua(kfk_topic);
        end
       
        _check_zombie(self, false, _check_runerror);
        
        --TODO
        --[[
            resend failure message 
        --]]
        ngx.sleep(i)
        if n == 0 then
            i = (i + 1 ) % k;
        else
            i = 2;
        end
	end

    _check_zombie(self, true, nil);

    _kfk_destroy_kfk(self);
    ngx.update_time();
    ngx.log(ngx.DEBUG, "yaowei step4 time: ", ngx.now());
end

function _M.log(self, level, topic, key, str)
	local ok, err = msg.kfk_new_msg(self, level, topic, key, str);
    if not ok then
        ngxlog(ngx.ERR, err);
    end
    return ok, err;
end


return _M;
