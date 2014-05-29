local util = require "util";
local bit = require "bit";
local uuid = require "uuid";
local list = require "lkfk.list";
local base = require "lkfk.base";
local const = require "lkfk.const";

local concat = table.concat;
local ipairs = ipairs;
local pairs = pairs;
local setmetatable = setmetatable;
local error = error;
local tonumber = tonumber;

local ngxnow = ngx.now;
local ngxlog = ngx.log;
local ngxsleep = ngx.sleep;
local floor = math.floor;
local strlen = string.len
local strsub = string.sub



local function kfk_topic_find(kfk, name)
	local topics = kfk.kfk_topics;
	for _, kfk_topic in ipairs(topics) do
		if kfk_topic.name == name then
			return kfk_topic;
		end
	end
	return nil;
end

local function kfk_new_toppar(kfk_topic, partition)
	local cf = kfk_topic.kfk.cf;
	
	local toppar = {
        type = "toppar",
		kfk_broker_toppar_link = list.LIST_ENTRY(),
		kfk_topic = kfk_topic,
		leader = nil,
		partition = partition,
		msgq = list.new("kfk_msg_link"),
		fail_retry = 0,
		last_send_time = 0
	}
	
	return toppar;
end

local function kfk_new_topic(kfk, name, partitioner)
	local kfk_topic = kfk_topic_find(kfk, name);
	if kfk_topic then
		return kfk_topic;
	end
	
	kfk_topic = {
        type = "topic",
		kfk_topic_link = list.LIST_ENTRY(),
        
        kfk = kfk,
        name = name,
        state = const.KFK_TOPIC_INIT,
        
        partitioner = partitioner or kfk.cf.partitioner,
        
        kfk_toppars = util.new_tab(4, 0),
        toppar_cnt = 0,

        flag = 0
	};
	kfk_topic.toppar_ua = kfk_new_toppar(kfk_topic, const.KFK_PARTITION_UA);
	
	local topics = kfk.kfk_topics;
	topics[#topics + 1] = kfk_topic;
	return kfk_topic;
--	kfk.topic_cnt = kfk.topic_cnt + 1;
	
    --[[
	ngx.log(ngx.DEBUG, "add topic: ", name);
    --]]
end

local function kfk_toppar_get(kfk_topic, partition)
	if partition >= 0 and partition < kfk_topic.toppar_cnt then
		return kfk_topic.kfk_toppars[partition + 1];
	else
		return kfk_topic.toppar_ua;
	end
end

local function kfk_message_partitioner(kfk_topic, kfk_msg)
	if kfk_topic.state == const.KFK_TOPIC_UNKNOWN then
		return "topic unknown";
	end
	
	local partition;
	if kfk_topic.partition_cnt == 0 then
		partition = const.KFK_PARTITION_UA;
	else
		partition = kfk_topic.partitioner(kfk_msg.key, kfk_topic.toppar_cnt);
	end
	
	local kfk_toppar = kfk_toppar_get(kfk_topic, partition);
	kfk_toppar.msgq:insert_tail(kfk_msg);
    
    --[[
    ngx.log(ngx.DEBUG, "topic[", kfk_topic.name, ":", kfk_toppar.partition, 
			"] have ", kfk_toppar.msgq.size, " messages");
    --]]
end


local function kfk_new_message(kfk, level, topic_name, key, str, prefix)
	local cf = kfk.cf;
	if level > cf.level then
		return true;
	end
	
	if kfk.msg_cnt + 1 > cf.queue_buffering_max_messages then
        ngxlog(ngx.INFO, "nobufs");
		return false, "nobufs";
	end
	
    local prefix = prefix or uuid.unparse(uuid.generate("random"));
	local kfk_msg = {
		kfk_msg_link  = list.LIST_ENTRY(),
		
		ts_timeout = cf.message_timeout_ms + ngxnow(),
		
		level = level,
		topic = topic_name,
		key = key or prefix,
		str = str
	}
	
	local kfk_topic =  kfk_topic_find(kfk, topic_name);
	if not kfk_topic then
		ngxlog(ngx.ERR, "topic unknown: ", topic_name);
		return false, "topic unknown";
	end
	
	local err = kfk_message_partitioner(kfk_topic, kfk_msg);
	if not err then
		kfk.msg_cnt = kfk.msg_cnt + 1;
		return true;
	end
	
	ngxlog(ngx.ERR, "topic unknown: ", topic_name);
	return false, err;
end


local func = {
	kfk_new_topic = kfk_new_topic,
	kfk_topic_find = kfk_topic_find,
	kfk_new_toppar = kfk_new_toppar,
	kfk_new_message = kfk_new_message,
	kfk_message_partitioner = kfk_message_partitioner,
	kfk_toppar_get = kfk_toppar_get
}

return func
