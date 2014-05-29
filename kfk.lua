local util = require "util";

local conf = require "lkfk.conf";
local broker = require "lkfk.broker1";
local func = require "lkfk.func";
local const = require "lkfk.const";
local list = require "lkfk.list";

local pairs = pairs;
local ipairs = ipairs;
local setmetatable = setmetatable;

local ngxspawn = ngx.thread.spawn;
local costatus = coroutine.status
local threadwait = ngx.thread.wait


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

    local backfile, err = io.open(cf.backlog, "a+");
    if not backfile then
        return nil, err;
    end
    backfile:setvbuf("line");

	local kfk = {
		cf = cf,
       	
        backfile = backfile,
		type = const.KFK_PRODUCE,
		flag = 0, -- the only use is for metadata query, avoid all broker update metadata
		
		quit = ngx.worker.exiting,
		-- all topics list
		kfk_topics = util.new_tab(4, 0),
		topic_cnt = 0,
		-- all broker list
		kfk_brokers = list.new("kfk_broker_link"),
		down_cnt = 0,
		
		threads = list.new("kfk_thread_link"),
        new_brokers = util.new_tab(4, 0),

        metadata_broker = const.KFK_BROKER_ID_UA,
		metadata_refresh_timeout = ngx.now() - cf.metadata_refresh_interval_ms / 1000;
        
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
	
    ngx.log(ngx.DEBUG, "kfk start up");

	-- init topic
	for _, name in ipairs(cf.topics) do
		func.kfk_new_topic(kfk, name);
	end
	
	return setmetatable(kfk, mt);
end

local function printtab(res)
    for k, v in pairs(res) do
        ngx.log(ngx.INFO, k, ": table");
        if type(v) == "table" then
            printtab(v);
        end
    end
end

local function kfk_destroy_kfk(kfk)
    for _, kfk_topic in ipairs(kfk.kfk_topics) do
        broker.kfk_destroy_topic(kfk_topic);
    end
    kfk.kfk_topics = nil;

    kfk.kfk_brokers = nil;

    ngx.log(ngx.DEBUG, "success produce ", kfk.succ_msg_cnt, " messages");
    ngx.log(ngx.DEBUG, "failed produce ", kfk.fail_msg_cnt, " messages");

end

function _M.loop(self)
	while not self.quit() do

        if #self.new_brokers > 0 then
            broker.kfk_broker_adds(self, self.new_brokers);
            self.new_brokers[1] = nil;
        end

		local threads = self.threads;
		local head = threads.head;
		local thread = head[threads.key].next;
		while thread ~= head do
            ngx.log(ngx.DEBUG, "thread check: ", costatus(thread.co));

            local tmp = threads:next(thread);
			if costatus(thread.co) == "zombie" then
                local ok, res = threadwait(thread.co)
				if not ok then
                    local kfk_broker = thread.kfk_broker;
					ngx.log(ngx.ERR, kfk_broker.name, " thread end of failure");
                    kfk_broker.fails = kfk_broker.fails + 1;
                    kfk_broker.last_fail_time = ngx.now();
                    if kfk_broker.fails < 3 then
					    broker.kfk_broker_resume(thread);
                    else
                        broker.kfk_broker_produce_fail(kfk_broker, const.KFK_RSP_ERR_DESTROY,
                                                       "lua abort: runtime error");
                        threads:remove(thread);
                    end
				end	
			end
			thread = tmp;
		end

        ngx.sleep(1);
	end

    local threads = self.threads;
    local head = threads.head;
    local thread = head[threads.key].next;

    while thread ~= head do

        threadwait(thread.co);

        threads:remove(thread);

        thread = head[threads.key].next;
    end

    kfk_destroy_kfk(self);
end

function _M.log(self, level, topic, key, str)
	return func.kfk_new_message(self, level, topic, key, str);
end


return _M;
