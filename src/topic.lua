local util = require "lkfk.util";
local alarm = require "lkfk.alarm";
local list = require "lkfk.list";
local msg = require "lkfk.msg";
local const = require "lkfk.const";
local func = require "lkfk.func";
local broker = require "lkfk.broker";

local ipairs = ipairs;
local ngxlog = ngx.log;
local DEBUG = ngx.DEBUG;
local ERR = ngx.ERR;
local CRIT = ngx.CRIT;

local function _kfk_new_toppar(kfk_topic, partition)
	local cf = kfk_topic.kfk.cf;
	
	local kfk_toppar = {
		kfk_broker_toppar_link = list.LIST_ENTRY(),
		kfk_topic = kfk_topic,
		leader = false,
		partition = partition,
		msgq = list.new("kfk_msg_link"),
		fail_retry = 0,
		last_send_time = 0
	}
	
	return kfk_toppar;
end

local function _kfk_destroy_toppar(kfk_toppar, err)
	local kfk = kfk_toppar.kfk_topic.kfk;
	local msgq = kfk_toppar.msgq;

    if kfk.cf.kfk_status then
        func.kfk_status_add(kfk_toppar.kfk_topic.name .. kfk_toppar.partition, -msgq.size);
    end
   
    if msgq.size > 0 then
        func.kfk_destroy_msgq(kfk, msgq, err);
    end
end

local function kfk_new_topic(kfk, name, partitioner)
	local kfk_topic = func.kfk_topic_find(kfk, name);
	if kfk_topic then
		return kfk_topic;
	end
	
	kfk_topic = {
		kfk_topic_link = list.LIST_ENTRY(),
        
        kfk = kfk,
        name = name,
        state = const.KFK_TOPIC_INIT,
        
        partitioner = partitioner or kfk.cf.partitioner,
        
        kfk_toppars = util.new_tab(4, 0),
        toppar_cnt = 0,
	};
	kfk_topic.toppar_ua = _kfk_new_toppar(kfk_topic, const.KFK_PARTITION_UA);
	
	local topics = kfk.kfk_topics;
	topics[#topics + 1] = kfk_topic;
	return kfk_topic;
end

local function kfk_destroy_topic(kfk_topic, err)
	for _, kfk_toppar in ipairs(kfk_topic.kfk_toppars) do
		_kfk_destroy_toppar(kfk_toppar, err);
	end
	
	_kfk_destroy_toppar(kfk_topic.toppar_ua, err);
end

local function _kfk_topic_partition_cnt_update(kfk_topic, part_cnt)
	if kfk_topic.toppar_cnt == part_cnt then
		return 0;
	end
	
	for i = kfk_topic.toppar_cnt + 1, part_cnt do
		kfk_topic.kfk_toppars[i] = _kfk_new_toppar(kfk_topic, i - 1);
	end
	
	local toppar_ua = func.kfk_toppar_get(kfk_topic, const.KFK_PARTITION_UA);
	
	for i = part_cnt + 1, kfk_topic.toppar_cnt do
        if kfk_topic.kfk.cf.kfk_status then
            func.kfk_status_add(kfk_topic.name .. kfk_topic.kfk_toppars[i].partition, 
                                -kfk_topic.kfk_toppars[i].msgq.size)
            func.kfk_status_add(kfk_topic.name .. toppar_ua.partition, 
                                kfk_topic.kfk_toppars[i].msgq.size)
        end

		list.concat(toppar_ua.msgq, kfk_topic.kfk_toppars[i].msgq);
		kfk_topic.kfk_toppars[i] = nil;
	end
	
	kfk_topic.toppar_cnt = part_cnt;
	return 1;
end

local function _kfk_toppar_broker_delegate(kfk_toppar, kfk_broker)
	if kfk_toppar.leader == kfk_broker then
		return;
	end
    local kfk_topic = kfk_toppar.kfk_topic;

	if kfk_toppar.leader then
		local old = kfk_toppar.leader;
		old.kfk_toppars:remove(kfk_toppar);

		kfk_toppar.leader = false;
	end
	
	if kfk_broker then
		kfk_broker.kfk_toppars:insert_tail(kfk_toppar);
		kfk_toppar.leader = kfk_broker;

    	ngxlog(DEBUG, "[kafka] [", kfk_topic.name, ":", kfk_toppar.partition, 
    		    	 "] move to broker ", kfk_broker.nodeid);
	end
end

local function _kfk_topic_leader_update(kfk, kfk_topic, part_id, kb)
	local kfk_toppar = func.kfk_toppar_get(kfk_topic, part_id);
	if not kfk_toppar then
		ngxlog(ERR, "[kafka] [", kfk_topic.name, "] no partition: ", part_id);
		return 0;	
	end	

	if not kb then
        if kfk_toppar.leader then
            --[[
            alarm.add(kfk_topic.name .. ":" .. kfk_toppar.partition .. 
                      " lose leader" .. kfk_toppar.leader.nodeid);
            --]]
        end
        _kfk_toppar_broker_delegate(kfk_toppar, nil);

        local toppar_ua = func.kfk_toppar_get(kfk_topic, const.KFK_PARTITION_UA);

        if kfk.cf.kfk_status then
            func.kfk_status_add(kfk_topic.name .. kfk_toppar.partition, -kfk_toppar.msgq.size)
            func.kfk_status_add(kfk_topic.name .. toppar_ua.partition, kfk_toppar.msgq.size)
        end

        list.concat(toppar_ua.msgq, kfk_toppar.msgq);

		ngxlog(CRIT, "[kafka] [", kfk_topic.name, ":", kfk_toppar.partition, 
                	 "] lose leader");
        return kfk_toppar.leader and 1 or 0;
    else
        if not kfk_toppar.leader then
            --[[
            alarm.add(kfk_topic.name .. ":" .. kfk_toppar.partition .. " leader " .. kb.nodeid);
            --]]
        end
	end
	
    _kfk_toppar_broker_delegate(kfk_toppar, kb);
	return 1;
end

local function kfk_topic_assign_ua(kfk_topic)
	local kfk = kfk_topic.kfk;
	local toppar_ua = func.kfk_toppar_get(kfk_topic, const.KFK_PARTITION_UA);
	if not toppar_ua then
		ngxlog(ERR, "[kafka] [", kfk_topic.name, "] no unassigned partition");
		return;
	end
    
    local found = false;
    for _, kfk_toppar in ipairs(kfk_topic.kfk_toppars) do
        if kfk_toppar.leader then
            found = true;
            break;
        end
    end

    if not found then
        local msgq = toppar_ua.msgq;
        if util.debug then
            ngxlog(DEBUG, "[kafka] [", kfk_topic.name, "] have ", msgq.size, " ua messages");
        end
        ngxlog(CRIT, "[kafka] [", kfk_topic.name, "] all partitoin lose leader");

        if kfk.cf.kfk_status then
            func.kfk_status_add(kfk_topic.name .. toppar_ua.partition, -msgq.size)
        end
        func.kfk_destroy_msgq(kfk, msgq, const.KFK_RSP_ERR_LEADER_NOT_AVAILABLE);
        --[[
        alarm.add(kfk_topic.name .. "all partition lose leader ");
        --]]
        return;
    end

    local tmpq = list.new("kfk_msg_link");
    list.concat(tmpq, toppar_ua.msgq);

    if tmpq.size > 0 then 

        if kfk.cf.kfk_status then
            func.kfk_status_add(kfk_topic.name .. toppar_ua.partition, -tmpq.size)
        end

        local head = tmpq.head;
        local kfk_msg = head[tmpq.key].next;
        while kfk_msg ~= head do
            tmpq:remove(kfk_msg);
            msg.kfk_message_partitioner(kfk_topic, kfk_msg);
            kfk_msg = head[tmpq.key].next;
        end
    end
end


local function kfk_topic_metadata_update(kfk, tm)
	local kfk_topic = func.kfk_topic_find(kfk, tm.name);
	if not kfk_topic then
		-- we skip the topic we not know
		return;
	end
	
	if tm.err ~= const.KFK_RSP_NOERR then
		ngxlog(ERR, "[kafka] [", kfk_topic.name, "] metadata error: ", 
                     const.errstr[tm.err]);
                     
		kfk_topic.state = const.KFK_TOPIC_UNKNOWN;
		kfk_destroy_topic(kfk_topic, tm.err);
		return;
	end
    
    if util.debug then
	    ngxlog(DEBUG, "[kafka] update topic: ", tm.name);
    end

	kfk_topic.state = const.KFK_TOPIC_EXIST;
	
	for i = 1, tm.part_cnt do
		if tm.part_meta[i].leader ~= -1 then
			tm.part_meta[i].kb = broker.kfk_broker_find_byid(kfk, tm.part_meta[i].leader);
		end
	end
	
	local upd = 0;
	upd = upd + _kfk_topic_partition_cnt_update(kfk_topic,
												tm.part_cnt);
												
	for i = 1, tm.part_cnt do
		upd = upd + _kfk_topic_leader_update(kfk, kfk_topic,
										     tm.part_meta[i].id,
										     tm.part_meta[i].kb)	
	end
	
	if upd > 0 then
		kfk_topic_assign_ua(kfk_topic);
	end

end

local topic = {
    kfk_topic_assign_ua = kfk_topic_assign_ua,
    kfk_topic_metadata_update = kfk_topic_metadata_update,
    kfk_new_topic = kfk_new_topic,
    kfk_destroy_topic = kfk_destroy_topic
}

return topic
