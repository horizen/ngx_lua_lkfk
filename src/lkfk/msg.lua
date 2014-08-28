local uuid = require "uuid";
local util = require "lkfk.util";
local list = require "lkfk.list";
local const = require "lkfk.const";
local func = require "lkfk.func";

local ngxnow = ngx.now;
local ngxlog = ngx.log;


local function kfk_message_partitioner(kfk_topic, kfk_msg)
    if kfk_topic.state == const.KFK_TOPIC_UNKNOWN then
        return "topic unknown";
    end
    
    local kfk_toppar;
    local toppar_ua = func.kfk_toppar_get(kfk_topic, const.KFK_PARTITION_UA);
    local cnt = kfk_topic.toppar_cnt;
    if cnt ~= 0 then
        local partition = kfk_topic.partitioner(kfk_msg.key, cnt);
        for i = 0, cnt - 1 do
            kfk_toppar = func.kfk_toppar_get(kfk_topic, 
                                             (partition + i) % cnt);
            if kfk_toppar.leader then
                break;
            end
        end
        if not kfk_toppar.leader then
            kfk_toppar = toppar_ua;
        end
    end
    
    kfk_toppar.msgq:insert_tail(kfk_msg);

    if kfk_topic.kfk.cf.kfk_status then
        func.kfk_status_add(kfk_topic.name .. kfk_toppar.partition, 1);
    end

end

local function kfk_new_msg(kfk, topic_name, key, str)
    local cf = kfk.cf;
    
    if kfk.msg_cnt + 1 > cf.queue_buffering_max_messages then
        return false, "nobufs";
    end
    
    local kfk_msg = {
        kfk_msg_link  = list.LIST_ENTRY(),
        
        topic = topic_name,
        key = key or uuid.unparse(uuid.generate("random")),
        str = str
    }
    
    local kfk_topic =  func.kfk_topic_find(kfk, topic_name);
    if not kfk_topic then
        return false, "topic unknown";
    end
    
    local err = kfk_message_partitioner(kfk_topic, kfk_msg);
    if not err then
        kfk.msg_cnt = kfk.msg_cnt + 1;

        if cf.kfk_status then
            func.kfk_status_add("msg_cnt", 1);
        end
        return true;
    end
    
    return false, err;
end

local msg = {
	kfk_new_msg = kfk_new_msg,
	kfk_message_partitioner = kfk_message_partitioner
}

return msg
