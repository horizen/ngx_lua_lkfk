local const = require "lkfk.const";
local list = require "lkfk.list";

local ipairs = ipairs;
local type = type

local ngxlog = ngx.log;
local ERR = ngx.ERR;
local WARN = ngx.WARN
local DEBUG = ngx.DEBUG

local corunning = coroutine.running
local costatus = coroutine.status
local coresume = coroutine.resume

local kfk_status = ngx.shared.kfk;

local function kfk_status_add(key, value)
    local size, err = kfk_status:incr(key, value);
    if not size and err == "not found" then
        local ok, err = kfk_status:add(key, 0);
        if not ok and err ~= "exists" then
            ngxlog(ERR, "[kafka] shared dict error: ", err);
        else
            kfk_status:incr(key, value);
        end
    elseif not size then
        ngxlog(ERR, "[kafka] shared dict error: ", err);
    end
end

local function kfk_status_set(key, value)
    local size, err = kfk_status:set(key, value);
    if not size then
        ngxlog(ERR, "[kafka] shared dict error: ", err);
    end
end

local function kfk_topic_find(kfk, name)
	local topics = kfk.kfk_topics;
	for _, kfk_topic in ipairs(topics) do
		if kfk_topic.name == name then
			return kfk_topic;
		end
	end
	return nil;
end

local function kfk_toppar_get(kfk_topic, partition)
	if partition >= 0 and partition < kfk_topic.toppar_cnt then
		return kfk_topic.kfk_toppars[partition + 1];
	else
		return kfk_topic.toppar_ua;
	end
end

local function kfk_destroy_msgq(kfk, msgq, err)
    local cf = kfk.cf;

    kfk.msg_cnt = kfk.msg_cnt - msgq.size;
    if cf.kfk_status then
        kfk_status_add("msg_cnt", -msgq.size);
    end

    if err == 0 then
        if cf.kfk_status then
            kfk_status_add("succ_msg_cnt", msgq.size);
        end
    else
        ngxlog(ERR, "[kafka] backup ", msgq.size, " message with error:", const.errstr[err]);

        if cf.kfk_status then
            kfk_status_add("fail_msg_cnt", msgq.size);
        end
        local eoffset;

        --[[ TODO resend failure message
        local topic = kfk_msg.topic;
        kfk_status_set(topic .. "eoff", eoffset);
        --]]

        local head = msgq.head;
        local kfk_msg = head[msgq.key].next;
        while kfk_msg ~= head do
            eoffset = cf.failed_cb(kfk, kfk_msg);
            msgq:remove(kfk_msg);
            kfk_msg = head[msgq.key].next;
        end

    end
    list.init(msgq);
end

local function add_meta_query(kfk)
    if not kfk.meta_lock then
        kfk.meta_lock = true;
    end

    if costatus(kfk.main_co) == "suspended" then
        coresume(kfk.main_co);
    end
end

local func = {
    kfk_destroy_msgq = kfk_destroy_msgq,

    kfk_status_add = kfk_status_add,
    kfk_status_set = kfk_status_set,

	kfk_toppar_get = kfk_toppar_get,
	kfk_topic_find = kfk_topic_find,

    add_meta_query = add_meta_query
}

return func
