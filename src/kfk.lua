local global = require "global";
local util = require "lkfk.util";
local alarm = require "lkfk.alarm";
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
local tonumber = tonumber

local ngxspawn = ngx.thread.spawn
local ngxwait = ngx.thread.wait
local coyield = coroutine.yield
local corunning = coroutine.running
local ngxlog = ngx.log;
local ngxnow = ngx.now
local ngxsleep = ngx.sleep


local _M = util.new_tab(0, 3);
_M._VERSION = "1.0";

local function _init_conf(cf)
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

local function _kfk_destroy_kfk(kfk, init)
    for _, kfk_topic in ipairs(kfk.kfk_topics) do
        topic.kfk_destroy_topic(kfk_topic, const.KFK_RSP_ERR_DESTROY);
    end
    
    for _, fp in pairs(kfk.fp) do
        fp:close();
    end

    kfk.tcp:close();
    
    if kfk.cf.kfk_status and not init then
        local buf = {};
        local keys = ngx.shared.kfk:get_keys();
        for _, key in ipairs(keys) do
            buf[#buf + 1] = key .. ":" .. ngx.shared.kfk:get(key);
        end
        ngxlog(ngx.WARN, "[kafka] [statistics] ", table.concat(buf, ","));
    end
	
end

local function _loop(kfk)
    local meta_flush = kfk.cf.metadata_refresh_interval_ms / 1000;
    local self = corunning();

	while not kfk.quit() do

        if kfk.meta_refresh_timeout <= ngxnow() then
            kfk.meta_refresh_timeout = ngxnow() + meta_flush;
			func.add_query_topic(kfk.kfk_topics);
        end

		meta.kfk_metadata_req(kfk);
	    
        for _, kfk_topic in ipairs(kfk.kfk_topics) do
            topic.kfk_topic_assign_ua(kfk_topic);
        end
        --[[ 
        local ok, err = alarm.send(kfk.tcp);
        if not ok then
            ngx.log(ngx.ERR, err);
        end
        --]]
        
        --TODO
        --[[
        for _, kfk_topic in ipairs(kfk.kfk_topics) do
            local boffset = ngx.shared.kfk:get(kfk_topic.name .. "boff");
            local eoffset = ngx.shared.kfk:get(kfk_topic.name .. "eoff");
            local fp = kfk.fp[kfk_topic.name];
            if boffset < eoffset then
                fp:seek(boffset);    
            end

            for line in file:lines() do
                
            end
        end
            resend failure message
        --]]
        local s = ngxnow();
        coyield(self);
        local t = ngxnow();
        if t - s < 2 then
            ngx.sleep(2);
        end
	end
	
	for _, co in ipairs(kfk.cos) do 
		ngxwait(co);
	end
    --[[
    local ok, err = alarm.send(kfk.tcp);
    if not ok then
        ngx.log(ngx.ERR, err);
    end
    --]]
    _kfk_destroy_kfk(kfk);
end


local function _new(cf)
	local cf, err = _init_conf(cf or {});
	if not cf then
		return nil, err;
	end

	local kfk = {
		cf = cf,
       	
        fp = util.new_tab(0, 4),
        tcp = ngx.socket.tcp(),
		
		quit = ngx.worker.exiting,
		
		--last time metadata update
		update_time = 0,
		
		-- all topics list
		kfk_topics = util.new_tab(4, 0),
		
		-- all broker list
		kfk_brokers = list.new("kfk_broker_link"),
		down_cnt = 0,
        
        meta_pending_topic = util.new_tab(4, 0),
		meta_refresh_timeout = ngxnow(),
		
        main_co = false,
        cos = util.new_tab(4, 0),
        
		msg_cnt = 0,
	}
    
	-- init topic
    for _, name in ipairs(cf.topics) do
        local fp, err = io.open(cf.backpath .. name, "a+");
        if not fp then
            _kfk_destroy_kfk(kfk, true);
            return nil, err;
        end
        fp:setvbuf("line");
        
        kfk.fp[name] = fp;
        
		topic.kfk_new_topic(kfk, name);
	end
	
	local body = meta.kfk_metadata_pretest(kfk);
	if not body then
        _kfk_destroy_kfk(kfk, true);
		return nil, "broker is invalid: please check the config of metadata_broker_list";
	end

	local co, err = ngxspawn(_loop, kfk);
	if not co then
        _kfk_destroy_kfk(kfk, true);
		return nil, err;
	end
	kfk.main_co = co;
	
	return setmetatable(kfk, {__inde = _M});
end


local function _kafka_startup(premature)
    if premature then
        return;
    end

    local err;
    local kfk, err = _new();
    if not kfk then
        error("kafka start failure: " .. err);
    end
    global.kfk = kfk;
end


function _M.init()
    local ok, err = ngx.timer.at(0, _kafka_startup);
    if not ok then
        error("start a timer failure: " .. err);
    end
end

function _M.log(self, topic, key, str)
	local ok, err = msg.kfk_new_msg(self, topic, key, str);
    if not ok then
        if self.cf.kfk_status then
            func.kfk_status_add("fail_msg_cnt", 1);
        end
        self.cf.failed_cb(self, {topic = topic, key = key, str = str});
    end
end

return _M;
