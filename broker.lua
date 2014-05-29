local util = require "util";
local bit = require "bit";

local list = require "lkfk.list";
local base = require "lkfk.base";
local const = require "lkfk.const";
local func = require "lkfk.func";

local concat = table.concat;
local ipairs = ipairs;
local pairs = pairs;
local setmetatable = setmetatable;
local error = error;
local tonumber = tonumber;

local ngxnow = ngx.now;
local ngxlog = ngx.log;
local ngxsleep = ngx.sleep;
local ngxspawn = ngx.thread.spawn;
local costatus = coroutine.status
local threadwait = ngx.thread.wait
local ngxcrc32 = ngx.crc32_long
local ngxtcp = ngx.socket.tcp
local ngxtimer = ngx.timer.at;
local floor = math.floor;
local strlen = string.len
local strbyte = string.byte
local strchar = string.char
local strfind = string.find
local strrep = string.rep
local strsub = string.sub

local band = bit.band
local bxor = bit.bxor
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift
local tohex = bit.tohex

local function kfk_destroy_msg(kfk, kfk_msg, err)
	if err == 0 then
		kfk.succ_msg_cnt = kfk.succ_msg_cnt + 1;
	else
		kfk.fail_msg_cnt = kfk.fail_msg_cnt + 1;
		kfk.cf.failed_cb(kfk, kfk_msg);
        if err ~= 0 then
            ngx.log(ngx.DEBUG, "produde messages error: ", err);
        end
	end
	
	kfk.msg_cnt = kfk.msg_cnt -1;
end

local function kfk_destroy_toppar(kfk_toppar)
	local kfk = kfk_toppar.kfk_topic.kfk;

	local msgq = kfk_toppar.msgq;
	local head = msgq.head;
	local kfk_msg = head[msgq.key].next;
	while kfk_msg ~= head do
		kfk_destroy_msg(kfk, kfk_msg, 1);
		msgq:remove(kfk_msg);
		kfk_msg = head[msgq.key].next;
	end
end

local function kfk_destroy_topic(kfk_topic)
	for _, kfk_toppar in ipairs(kfk_topic.kfk_toppars) do
		kfk_destroy_toppar(kfk_toppar);
	end
	
	kfk_destroy_toppar(kfk_topic.toppar_ua);
end

local function kfk_destroy_buf(kfk, kfk_buf, err)
	local msgq = kfk_buf.msgq;
	local head = msgq.head;
	local kfk_msg = head[msgq.key].next;
	while kfk_msg ~= head do
		kfk_destroy_msg(kfk, kfk_msg, err);
		msgq:remove(kfk_msg);
		kfk_msg = head[msgq.key].next;
	end
end

local function kfk_destroy_kfk(kfk)
    if kfk.kfk_topics then
    	for _, kfk_topic in ipairs(kfk.kfk_topics) do
    		kfk_destroy_topic(kfk_topic);
    	end
        kfk.kfk_topics = nil;
        ngxlog(ngx.DEBUG, "success produce ", kfk.succ_msg_cnt, " messages");
        ngxlog(ngx.DEBUG, "failed produce ", kfk.fail_msg_cnt, " messages");
	end
end

local function kfk_buf_new()
	return {
		kfk_buf_link = list.LIST_ENTRY(),
		
		msgq = list.new("kfk_msg_link"),
		
		callback = nil,
		opaque = nil,
		api_key = 0,
            
		tries = 0,
		corr_id = 0,
		
		len = 0,
		of = 0,
		kfk_req = nil
	}
end

local function kfk_broker_new(kfk, ip, port, nodeid)
	return {
        kfk_broker_link = list.LIST_ENTRY(),
		ip = ip,
		port = port,
		name = ip .. ":" .. port,
		nodeid = nodeid,
		state = const.KFK_BROKER_DOWN,
		valid = 1,
		
		outbuf = list.new("kfk_buf_link"),
		retrybuf = list.new("kfk_buf_link"),
		waitrspbuf = list.new("kfk_buf_link"),
		
		recvbuf = {
			body = "",
			of = 0,
			size = 0
		},
		
        threads = list.new("kfk_thread_link"),

		kfk_toppars = list.new("kfk_broker_toppar_link"),
		toppar_cnt = 0,
		
		tcp = ngxtcp(),
		kfk = kfk,
		
		corr_id = 0
	}
end

local function kfk_broker_buf_enq(kfk_broker, kfk_buf, msgbody, api_key, 
									callback, opaque)
	local cf = kfk_broker.kfk.cf;
	
	local kfk_req = {
		base.zero4,							-- 1 req size
		base.set_byte2(api_key),			-- 2 api key
		base.zero2,							-- 3 api version
	    base.set_byte4(kfk_broker.corr_id % 2147483648),	-- 4 corrlation id
		base.pack_kfk_string(cf.client_id),	-- 5 client id
		msgbody	        					-- 6 msg body
	}
	local size = 8 + strlen(kfk_req[5]) + strlen(kfk_req[6]);
	kfk_req[1] = base.set_byte4(size);
	
	kfk_buf.callback = callback;
	kfk_buf.opaque = opaque;
    kfk_buf.api_key = api_key;
	kfk_buf.corr_id = kfk_broker.corr_id;	
	kfk_buf.len = size + 4;
    kfk_buf.kfk_req = concat(kfk_req);
	
	kfk_broker.corr_id = kfk_broker.corr_id + 1;
	
	local outbuf = kfk_broker.outbuf;
	if api_key == const.KFK_PRODUCE_REQ then
		outbuf:insert_tail(kfk_buf);
	else 
		outbuf:insert_head(kfk_buf);
	end
	
    
	ngx.log(ngx.DEBUG, "broker[", kfk_broker.name, "] ",  kfk_buf.msgq.size, 
			" messages into outbuf");
    
end

local function kfk_broker_find_byname(kfk, name)
    local brokers = kfk.kfk_brokers;
    local head = brokers.head;
    local kfk_broker = head[brokers.key].next;
    while kfk_broker ~= head do
		if kfk_broker.name == name then
			return kfk_broker;
		end
        kfk_broker = brokers:next(kfk_broker);
	end
	return nil;
end

local function kfk_broker_find_byid(kfk, nodeid)
    local brokers = kfk.kfk_brokers;
    local head = brokers.head;
    local kfk_broker = head[brokers.key].next;
    while kfk_broker ~= head do
		if kfk_broker.nodeid == nodeid then
			return kfk_broker;
		end
        kfk_broker = brokers:next(kfk_broker);
	end
	return nil;
end

local function kfk_broker_any(kfk, state)
    local brokers = kfk.kfk_brokers;
    local head = brokers.head;
    local kfk_broker = head[brokers.key].next;
    while kfk_broker ~= head do
		if kfk_broker.state == state then
			return kfk_broker;
		end
        kfk_broker = brokers:next(kfk_broker);
	end
	return nil;
end


local kfk_topic_metadata_update

local kfk_broker_update

local function kfk_metadata_handle(kfk_broker, err, rspbuf, reqbuf)
	-- req may be kfk_topic or kfk
	local req = reqbuf.opaque;
	if req then
		req.flag = 0;
	end
    if err ~= 0 then
        ngxlog(ngx.ERR, kfk_broker.name, " metadata request error");
        return;
    end
	-- we skip size and corr id
	local of = 9;
    local data = rspbuf.body;
	local broker_cnt;
	broker_cnt, of = base.get_byte4(data, of);
	local brokers = util.new_tab(broker_cnt, 0);
	
	for i = 1, broker_cnt do
		local tmp = {};
		tmp.nodeid, of = base.get_byte4(data, of);
		tmp.host, of = base.get_kfk_string(data, of);
		tmp.port, of = base.get_byte4(data, of);
		brokers[#brokers + 1] = tmp;
	end
	
	local top_cnt;
	top_cnt, of = base.get_byte4(data, of);
    local topic_metas = util.new_tab(top_cnt, 0);
	for i = 1, top_cnt do
		local tmp = {};
		tmp.err, of = base.get_byte2(data, of);
		tmp.name, of = base.get_kfk_string(data, of);

		tmp.part_cnt, of = base.get_byte4(data, of);
		local part_meta = util.new_tab(tmp.part_cnt, 0);
		for j = 1, tmp.part_cnt do
			local tmp2 = {};
			tmp2.err, of = base.get_byte2(data, of);
			tmp2.id, of = base.get_byte4(data, of);
			tmp2.leader, of = base.get_byte4(data, of);
			
			tmp2.rep_size, of = base.get_byte4(data, of);
			local tmp3 = util.new_tab(tmp2.rep_size, 0);
			for r = 1, tmp2.rep_size do
				tmp3[r], of = base.get_byte4(data, of);
			end
			tmp2.rep = tmp3;
			
			tmp2.isr_size, of = base.get_byte4(data, of);
			local tmp3 = util.new_tab(tmp2.isr_size, 0);
			for r = 1, tmp2.isr_size do
				tmp3[r], of = base.get_byte4(data, of);
			end
			tmp2.isr = tmp3;
			
			part_meta[#part_meta + 1] = tmp2;
		end
		tmp.part_meta = part_meta;
		
		topic_metas[#topic_metas + 1] = tmp;
	end
    
    local kfk = kfk_broker.kfk;
	for _, tmp in ipairs(brokers) do
		kfk_broker_update(kfk_broker, tmp);
        
	end

	for _, tm in ipairs(topic_metas) do
		kfk_topic_metadata_update(kfk_broker, tm);
	end

    rspbuf.body = "";
    rspbuf.of = 0;
    rspbuf.size = 0;
end


--[[
-- all_topic := if true: retreive all topics&partitions from the broker
--              if false: just retrieve the topics we know about.
-- kfk_topic := all_topic=false && topics is set: only ask for specified topic.
--]]
local function kfk_metadata_req(kfk, kfk_broker, kfk_topic)
    if kfk.quit() then
        return;
    end
    if not kfk_broker or kfk_broker.state == const.KFK_BROKER_DOWN then
        kfk_broker = kfk_broker_any(kfk, const.KFK_BROKER_UP);
        if not kfk_broker then
            ngxlog(ngx.INFO, "all broker down");
            return;
        end
    end
    
    
	ngx.log(ngx.DEBUG, "broker[", kfk_broker.name, "] start a metadata query");
    
	local buf = {
		-- 1 topic count
		-- 2...n topic
	}
	
	if kfk_topic then
		if kfk_topic.flag == const.KFK_TOPIC_F_LEADER_QUERY then
			return;
		end
		kfk_topic.flag = const.KFK_TOPIC_F_LEADER_QUERY;
		buf[1] = base.set_byte4(1);
		buf[2] = base.pack_kfk_string(kfk_topic.name);
	else
		if kfk.flag == const.KFK_TOPIC_F_LEADER_QUERY then
			return;
		end
		kfk.flag = const.KFK_TOPIC_F_LEADER_QUERY;
		buf[1] = base.set_byte4(#kfk.kfk_topics);
		for _, kfk_topic in ipairs(kfk.kfk_topics) do
			buf[#buf + 1] = base.pack_kfk_string(kfk_topic.name);
		end
	end
	
	local msgbody = concat(buf, "");
	local kfk_buf = kfk_buf_new();
	kfk_broker_buf_enq(kfk_broker, kfk_buf, msgbody, const.KFK_METADATA_REQ,
						kfk_metadata_handle, kfk_topic or kfk);
end

local function kfk_topic_partition_cnt_update(kfk_topic, part_cnt)
	if kfk_topic.toppar_cnt == part_cnt then
		return 0;
	end
	
	for i = 1, part_cnt do
		if i > kfk_topic.toppar_cnt then
			kfk_topic.kfk_toppars[i] = func.kfk_new_toppar(kfk_topic, i - 1);
		end
	end
	
	local toppar_ua = func.kfk_toppar_get(kfk_topic, const.KFK_PARTITION_UA);
	
	for i = part_cnt + 1, kfk_topic.toppar_cnt do
		list.concat(toppar_ua.msgq, kfk_topic.kfk_toppar[i].msgq);
		kfk_topic.kfk_toppars[i] = nil;
	end
	
	kfk_topic.toppar_cnt = part_cnt;
	return 1;
end


local function kfk_toppar_broker_delegate(kfk_toppar, kfk_broker)
	if kfk_toppar.leader == kfk_broker then
		return;
	end
	
	if kfk_toppar.leader then
		local old = kfk_toppar.leader;
		old.kfk_toppars:remove(kfk_toppar);
--		old.toppar_cnt = old.toppar_cnt - 1;
		kfk_toppar.leader = nil;
	end
	
	if kfk_broker then
		kfk_broker.kfk_toppars:insert_tail(kfk_toppar);
--		kfk_broker.toppar_cnt = kfk_broker.toppar_cnt + 1;
		kfk_toppar.leader = kfk_broker;
	else
		ngxlog(ngx.WARN, "topic[", kfk_toppar.kfk_topic.name, ":", kfk_toppar.partition, "] lose leader");
	end
end

local function kfk_topic_leader_update(kfk_broker, kfk_topic, part_id, rkb)
	local toppar = func.kfk_toppar_get(kfk_topic, part_id);
	if not toppar then
		ngxlog(ngx.ERR, "topic[", kfk_topic.name, "] no partition: ", part_id);
		return 0;	
	end	
	
	if not rkb then
        ngxlog(ngx.DEBUG, "topic[", kfk_topic.name, ":", toppar.partition, 
        	   "] lose leader, remove partition from ", 
        		toppar.leader and toppar.leader.nodeid);

		kfk_toppar_broker_delegate(toppar, nil);
		if toppar.fail_retry < 3 then
			toppar.fail_retry = toppar.fail_retry + 1;
			kfk_metadata_req(kfk_broker.kfk, kfk_broker, kfk_topic);
		else
			-- broker may be down, we need get a notice
			kfk_destroy_toppar(toppar);
		end
		return toppar.leader and 1 or 0;
	end
	
	toppar.fail_retry = 0;
	
	if toppar.leader then
		if toppar.leader == rkb then
			return 0;
		end
		
		ngxlog(ngx.DEBUG, "topic[", kfk_topic.name, ":", toppar.partition, 
				"] move from broker ", toppar.leader.nodeid, " to ", rkb.nodeid);
    else
    	ngxlog(ngx.DEBUG, "topic[", kfk_topic.name, ":", toppar.partition, 
    		    "] move to broker ", rkb.nodeid);
	end
	
	kfk_toppar_broker_delegate(toppar, rkb);
	return 1;
end

local function kfk_topic_assign_ua(kfk_topic)
	local kfk = kfk_topic.kfk;
	local toppar_ua = func.kfk_toppar_get(kfk_topic, const.KFK_PARTITION_UA);
	if not toppar_ua then
		ngxlog(ngx.ERR, "topic[", kfk_topic.name, "] no unassigned partition");
		return;
	end
	
	local msgq = toppar_ua.msgq;
    
    ngx.log(ngx.DEBUG, "topic[", kfk_topic.name, "] have ", msgq.size, " ua messages")
    
	local head = msgq.head;
	local kfk_msg = head[msgq.key].next;
	while kfk_msg ~= head do
		msgq:remove(kfk_msg);
		func.kfk_message_partitioner(kfk_topic, kfk_msg);
		kfk_msg = head[msgq.key].next;
	end

end

local function kfk_topic_metadata_failed(kfk_broker, kfk_topic, err)
    -- when kfk broker enable create topic option when topic unexist, then the err is happened
	if err == const.KFK_RSP_ERR_LEADER_NOT_AVAILABLE then
		kfk_topic.state = const.KFK_TOPIC_INIT;
		kfk_metadata_req(kfk_broker.kfk, kfk_broker, kfk_topic);
	else
		kfk_topic.state = const.KFK_TOPIC_UNKNOWN;
		kfk_destroy_topic(kfk_topic);
	end
end

kfk_topic_metadata_update = function (kfk_broker, tm)
    local kfk = kfk_broker.kfk;
	local kfk_topic = func.kfk_topic_find(kfk, tm.name);
	if not kfk_topic then
		-- we skip the topic we not know
		return;
	end
	
	ngx.log(ngx.DEBUG, "broker[", kfk_broker.name, "] update topic: ", tm.name);
    
	if tm.err ~= const.KFK_RSP_NOERR then
		ngxlog(ngx.ERR, "broker[", kfk_broker.name, "] topic[", kfk_topic.name, 
              "] metadata reply: ", tm.err);
		kfk_topic_metadata_failed(kfk_broker, kfk_topic, tm.err);
		return;
	end
	
	kfk_topic.state = const.KFK_TOPIC_EXIST;
	
	for i = 1, tm.part_cnt do
		if tm.part_meta[i].leader ~= -1 then
			tm.part_meta[i].rkb = kfk_broker_find_byid(kfk, tm.part_meta[i].leader);
		end
	end
	
	local upd = 0;
	upd = upd + kfk_topic_partition_cnt_update(kfk_topic,
												tm.part_cnt);
												
	for i = 1, tm.part_cnt do
		upd = upd + kfk_topic_leader_update(kfk_broker, kfk_topic,
										     tm.part_meta[i].id,
										     tm.part_meta[i].rkb)	
	end
	
	if upd > 0 then
		kfk_topic_assign_ua(kfk_topic);
	end

    
    ngx.log(ngx.DEBUG, "topic[", kfk_topic.name, "] have ", kfk_topic.toppar_cnt, " partitions");
    for _, kfk_toppar in ipairs(kfk_topic.kfk_toppars) do
        ngx.log(ngx.DEBUG, "topic[", kfk_topic.name, ":", kfk_toppar.partition, 
			"] have ", kfk_toppar.msgq.size, " messages");
        ngx.log(ngx.DEBUG, "topic[", kfk_topic.name, ":", kfk_toppar.partition, 
			"] have leader: ", kfk_toppar.leader and kfk_toppar.leader.name);
    end
    
end


local function kfk_broker_set_state(kfk_broker, state)
    local kfk = kfk_broker.kfk;
	if state == const.KFK_BROKER_DOWN then
		kfk.down_cnt = kfk.down_cnt + 1;
		if kfk.kfk_brokers.size <= 0 then
			ngxlog(ngx.ERR, "all broker down");
		end
	elseif kfk_broker.state == const.KFK_BROKER_DOWN then
		kfk.down_cnt = kfk.down_cnt - 1;
	end
	
	kfk_broker.state = state;
end

local function kfk_broker_connect(kfk_broker)
	local tcp = kfk_broker.tcp;
	local i = 1;
	local ok, err;
    tcp:settimeout(3000);
	while i <= 3 do
		ok, err = tcp:connect(kfk_broker.ip, kfk_broker.port);
		if ok then
			kfk_broker_set_state(kfk_broker, const.KFK_BROKER_UP);
			return true;
		end
        ngxsleep(i);
		i = i + 1;
	end
	return nil, err;
end

local function kfk_broker_buf_retry(kfk_broker, kfk_buf)
	local cf=  kfk_broker.kfk.cf;
	
	kfk_buf.retry_timeout = cf.retry_backoff_ms / 1000 + ngxnow();
	kfk_buf.of = 0;
	kfk_buf.tries = kfk_buf.tries + 1;
	
	kfk_broker.retrybuf:insert_tail(kfk_buf); 
end

local function kfk_broker_produce_handle(kfk_broker, rspbuf, of)
	local data = rspbuf.body;
	
	local top_cnt, of = base.get_byte4(data, of);
	if top_cnt ~= 1 then
		return const.KFK_RSP_ERR_BAD_MSG;
	end
	
	local topic, of = base.get_kfk_string(data, of);
	local part_cnt, of = base.get_byte4(data, of);
	if part_cnt ~= 1 then
		return const.KFK_RSP_ERR_BAD_MSG;
	end
	
	local partition, of = base.get_byte4(data, of);
	local err, of = base.get_byte2(data, of);
	local offset, of = base.get_byte8(data, of);
	
	return err;
end


local function kfk_broker_produce_reply(kfk_broker, err, rspbuf, reqbuf)
	if err == 0 and rspbuf then
		-- we skip size and corr id
        ngx.log(ngx.DEBUG, "broker[", kfk_broker.name, "] ", reqbuf.corr_id, " send success");
        
		err = kfk_broker_produce_handle(kfk_broker, rspbuf, 9);
        if err == 0 then
            
            ngx.log(ngx.DEBUG, "broker[", kfk_broker.name, "] ", 
                    reqbuf.corr_id, ": ", reqbuf.msgq.size, " produce success");
        end
	end

	--destroy recvbuf 
    if rspbuf then
	    rspbuf.body = "";
	    rspbuf.of = 0;
	    rspbuf.size = 0;
    end
    
	local kfk = kfk_broker.kfk;
	local cf = kfk.cf;
	local kfk_toppar = reqbuf.opaque;
	if err ~= 0 then
		ngxlog(ngx.ERR, "broker[", kfk_broker.name, "] partition ", kfk_toppar.partition,
                " message set ", reqbuf.corr_id, " with ", reqbuf.msgq.size, 
                " messages encountered error: ", err);
				
		if err == const.KFK_RSP_ERR_REQUEST_TIMED_OUT or
		   err == const.KFK_RSP_ERR_MESSAGE_TIMED_OUT then

			if (reqbuf.tries + 1 < cf.message_send_max_retries) then
				kfk_broker_buf_retry(kfk_broker, reqbuf);
				return;
			end
		elseif err == const.KFK_RSP_ERR_UNKNOWN_TOPIC_OR_PART or
			   err == const.KFK_RSP_ERR_LEADER_NOT_AVAILABLE or
			   err == const.KFK_RSP_ERR_NOT_LEADER_FOR_PARTITION or
			   err == const.KFK_RSP_ERR_BROKER_NOT_AVAILABLE or
			   err == const.KFK_RSP_ERR_REPLICA_NOT_AVAILABLE or
			   err == const.KFK_RSP_ERR_TRANSPORT then
			   
			   	list.concat_head(kfk_toppar.msgq, reqbuf.msgq);
			   	kfk_metadata_req(kfk, kfk_broker, kfk_toppar.kfk_topic);		   	
			    return;
		else --[[
			err == const.KFK_RSP_ERR_UNKNOWN or
			err == const.KFK_RSP_ERR_INVALID_MSG
			err == const.KFK_RSP_ERR_INVALID_MSG_SIZE or
			err == const.KFK_RSP_ERR_MSG_SIZE_TOO_LARGE then
            err == const.KFK_RSP_ERR_BAD_MSG
            err == const.KFK_RSP_ERR_DESTROY
			--]]
            
			ngx.log(ngx.DEBUG, "broker[", kfk_broker.name, "] ",
				    reqbuf.msgq.size, "  messages send failure");
            
		end
	end
	
	kfk_destroy_buf(kfk_broker.kfk, reqbuf, err);
end

local function kfk_broker_produce_fail(kfk_broker, err, errstr)
	ngxlog(ngx.ERR, "broker[", kfk_broker.name, "] produce failed: ", errstr);
	
	kfk_broker_set_state(kfk_broker, const.KFK_BROKER_DOWN);
	
	local tcp = kfk_broker.tcp;
	tcp:close();
    
	local tmpq = list.new("kfk_buf_link");
	list.concat(tmpq, kfk_broker.waitrspbuf);
	list.concat(tmpq, kfk_broker.outbuf);

	local head = tmpq.head;
	local kfk_buf = head[tmpq.key].next;
	while kfk_buf ~= head do
		kfk_buf.callback(kfk_broker, err, nil, kfk_buf);
		kfk_buf = tmpq:next(kfk_buf);
	end
    
    local kfk = kfk_broker.kfk;

    local toppars = kfk_broker.kfk_toppars;
    head = toppars.head;
    local kfk_toppar = head[toppars.key].next;
    while kfk_toppar ~= head do
        kfk_toppar_broker_delegate(kfk_toppar, nil);
        kfk_toppar = toppars:next(kfk_toppar);
    end
   
    kfk.metadata_broker = kfk.kfk_brokers:next(kfk_broker).nodeid;
    
    ngx.log(ngx.WARN, "metadata query broker change to", kfk.metadata_broker);
    

    if err ~= const.KFK_RSP_ERR_DESTROY then
		kfk_metadata_req(kfk, kfk_broker, nil);
	else
        ngxlog(ngx.WARN, "broker[", kfk_broker.name, "] remove from kfk cluster");
        kfk.kfk_brokers:remove(kfk_broker);
	end
end

local function kfk_broker_waitrsp_find(kfk_broker, corr_id)
	local waitrspbuf = kfk_broker.waitrspbuf;
	local head = waitrspbuf.head;
	local kfk_buf = head[waitrspbuf.key].next;
	while kfk_buf ~= head do
		if kfk_buf.corr_id == corr_id then
			kfk_broker.waitrspbuf:remove(kfk_buf);
			return kfk_buf;
		elseif kfk_buf.corrid > corr_id then
			return nil;
		end
		kfk_buf = waitrspbuf:next(kfk_buf);
	end
	return nil;
end

local function kfk_broker_send(kfk_broker)
	local kfk = kfk_broker.kfk;
	local cf = kfk.cf;
	
	local outbuf = kfk_broker.outbuf;
	local tcp = kfk_broker.tcp;
    
	local head = outbuf.head;
	local kfk_buf = head[outbuf.key].next;
	while kfk_buf ~= head do
        if kfk.quit() then
            return false;
        end
		local kfk_req = strsub(kfk_buf.kfk_req, kfk_buf.of + 1);
		
		tcp:settimeout(cf.queue_buffering_max_ms);
		local bytes, err = tcp:send(kfk_req);
		if not bytes then
			-- for timeout and closed error, we do not think is a failure
			-- but ngx_lua close socket when timeout,so we need reconnect
			kfk_broker_produce_fail(kfk_broker, const.KFK_RSP_ERR_TRANSPORT, 
							 "send failed: " .. err);
			return false;
		end
		kfk_buf.of = kfk_buf.of + bytes;
		
		ngx.log(ngx.DEBUG, "broker[", kfk_broker.name, "] send done");
        
		outbuf:remove(kfk_buf);
		
		kfk_buf.waitrsp_timeout = ngxnow() + cf.message_timeout_ms / 1000;
		if kfk_buf.api_key == const.KFK_METADATA_REQ or
           cf.request_required_acks > 0 then
			kfk_broker.waitrspbuf:insert_tail(kfk_buf);
		else
			kfk_buf.callback(kfk_broker, 0, nil, kfk_buf);
		end
		
		kfk_buf = head[outbuf.key].next;
	end
    return true;
end


local function kfk_broker_recv(kfk_broker)
	local cnt = 0;
	local cf = kfk_broker.kfk.cf;
	local tcp = kfk_broker.tcp;

	local recvbuf = kfk_broker.recvbuf;
	
	tcp:settimeout(cf.queue_buffering_max_ms);
	if recvbuf.size == 0 then 
		local data, err, partial = tcp:receive(20);
		if not data then
			if err ~= "timeout" then
				kfk_broker_produce_fail(kfk_broker, const.KFK_RSP_ERR_TRANSPORT, 
								"receive failed: " .. err or "");
				return 0;
			end

			if partial and partial ~= "" then
				recvbuf.of = recvbuf.of + strlen(partial);
				recvbuf.body = recvbuf.body .. partial;
                --[[
		        ngx.log(ngx.DEBUG, "broker[", kfk_broker.name, "] receive timeout ",
                       "already receive ", recvbuf.of, " bytes");	
                --]]
			end
			if recvbuf.of >= 4 then
				recvbuf.size = base.get_byte4(recvbuf.body, 1);
                --[[
                ngx.log(ngx.DEBUG, "broker[", kfk_broker.name, "] response ", 
                        recvbuf.size, " bytes");
                --]]
                if recvbuf.size < 24 then
                    kfk_broker_produce_fail(kfk_broker, const.KFK_RSP_ERR_BAD_MSG,
                                     "invalid message size");
                    return 0;
                end
			end
            if partial then
                return strlen(partial);
            end
			return 0;
		end
		recvbuf.of = recvbuf.of + strlen(data);
		recvbuf.body = recvbuf.body .. data;
		recvbuf.size = base.get_byte4(data, 1);
	end
	
	local size = recvbuf.size - recvbuf.of + 4;
	if size > 0 then
		local data, err, partial = tcp:receive(size);
		if not data then
			if err ~= "timeout" then
				kfk_broker_produce_fail(kfk_broker, const.KFK_RSP_ERR_TRANSPORT, 
								"receive failed: " .. err);
				return 0;
			end
			if partial and partial ~= "" then
				recvbuf.of = recvbuf.of + strlen(partial);
				recvbuf.body = recvbuf.body .. partial;
			end
            if partial then
                return strlen(partial);
            end
            return 0;
		end
		recvbuf.of = recvbuf.of + strlen(data);
		recvbuf.body = recvbuf.body .. data;
	else
        
		ngx.log(ngx.DEBUG, "broker[", kfk_broker.name, "] receive done");
        
		local corr_id = base.get_byte4(recvbuf.body, 5);	
		
		local kfk_buf = kfk_broker_waitrsp_find(kfk_broker, corr_id);
		if not kfk_buf then
			ngxlog(ngx.ERR, "broker[", kfk_broker.name, 
                   "] overtime response because unknown corrid: ", corr_id);
			return 1;
		end
		kfk_buf.callback(kfk_broker, 0, recvbuf, kfk_buf);
		
	end
	return 1;
end

local function kfk_broker_io_server(kfk_broker)
	
	if not kfk_broker_send(kfk_broker) then
        return;
    end

    local n = kfk_broker_recv(kfk_broker);
    while n > 0 do
        n = kfk_broker_recv(kfk_broker);
    end
end

local function kfk_broker_retry_buf_move(kfk_broker)
	local retrybuf = kfk_broker.retrybuf;
	if retrybuf.size <= 0 then
		return;
	end
	local outbuf = kfk_broker.outbuf;
	
	local tmp = list.new("kfk_buf_link");
	local head = retrybuf.head;
	local kfk_buf = head[retrybuf.key].next;
	local now = ngxnow();
	while kfk_buf ~= head do
		if kfk_buf.retry_timeout > now then
			break;
		end
		retrybuf:remove(kfk_buf);
		tmp:insert_tail(kfk_buf);
		kfk_buf = head[retrybuf.key].next;
	end
	if tmp.size > 0 then
		list.concat_head(outbuf, tmp);
	end
end

local function kfk_broker_waitrsp_timeout_scan(kfk_broker)
	local now = ngxnow();
	
	local waitrspbuf = kfk_broker.waitrspbuf;
	if waitrspbuf.size <= 0 then
		return;
	end
	local head = waitrspbuf.head;
	local kfk_buf = head[waitrspbuf.key].next;
	while kfk_buf ~= head do
		if kfk_buf.waitrsp_timeout > now then
			return;
		end
		
		waitrspbuf:remove(kfk_buf);
		kfk_buf.callback(kfk_broker, const.KFK_RSP_ERR_MESSAGE_TIMED_OUT, 
						 nil, kfk_buf);
		kfk_buf = head[waitrspbuf.key].next;
	end
end

local function kfk_broker_produce_toppar(kfk_broker, kfk_toppar)
	local cf = kfk_broker.kfk.cf;

	local kfk_topic = kfk_toppar.kfk_topic;
	
	local part1 = {
		base.set_byte2(cf.request_required_acks), -- 1 required_acks
		base.set_byte4(cf.request_timeout_ms),	-- 2 timeout
		base.set_byte4(1),						-- 3 topic count

		base.pack_kfk_string(kfk_topic.name), 	-- 4 topic_name
		base.set_byte4(1),					    -- 5 part_cnt
		base.set_byte4(kfk_toppar.partition),	-- 6 partition
		base.zero4							    -- 7 msgset size
	}
	local kfk_buf = kfk_buf_new();
	
	local msgset_size = 0;	
	local msgq = kfk_toppar.msgq;
	local n = msgq.size;
	local msgcnt = n < cf.batch_num_messages and n or cf.batch_num_messages;
    
	local buf = {};
	local head = msgq.head;
	local kfk_msg = head[msgq.key].next;
	while msgcnt > 0 and not kfk_broker.kfk.quit() do
	    
		local part2 = {
			base.zero8,					-- 1 offset 
			base.zero4,					-- 2 msg_size 
			base.zero4,					-- 3 crc 
			base.zero1,					-- 4 magic_byte 
			base.zero1,					-- 5 attr 
		 	nil,					-- 6 key 
		 	nil						-- 7 value 
		};
		local key = base.pack_kfk_bytes(nil);
		local value = base.pack_kfk_bytes(kfk_msg.str);
		local crc = ngxcrc32(part2[4] .. 
						  	part2[5] ..
						  	key ..
						  	value);
		local msg_size = strlen(value) +
						 strlen(key) +
						 6;
						 
		part2[2] = base.set_byte4(msg_size);
		part2[3] = base.set_byte4(crc);
		part2[6] = key;
		part2[7] = value;
		
		buf[#buf + 1] = concat(part2, "");
		msgset_size = msgset_size +
					  msg_size +
					  12;
					  
		msgcnt = msgcnt - 1;
	    msgq:remove(kfk_msg);
        kfk_buf.msgq:insert_tail(kfk_msg);

		kfk_msg = head[msgq.key].next;
	end
	
	part1[7] = base.set_byte4(msgset_size);
	
	local msgbody = concat(part1, "") .. concat(buf, "");
	kfk_broker_buf_enq(kfk_broker, kfk_buf, msgbody, const.KFK_PRODUCE_REQ,
						kfk_broker_produce_reply, kfk_toppar);
end

local function kfk_broker_producer_server(kfk_broker)
	local kfk = kfk_broker.kfk;
	local cf = kfk.cf;
    local queue_time_s = cf.queue_buffering_max_ms / 1000;
    local meta_time_s = cf.metadata_refresh_interval_ms / 1000;

	while not kfk.quit() and kfk_broker.state == const.KFK_BROKER_UP do
		if bxor(kfk_broker.valid, 0) ~= kfk_broker.valid then
			break;
		end
		local toppars = kfk_broker.kfk_toppars;

		local head = toppars.head;
		local kfk_toppar = head[toppars.key].next;
		while kfk_toppar ~= head do
			local elsape = kfk_toppar.last_send_time +
		 			   		queue_time_s -
		 			   		ngxnow();
		 	if elsape <= 0 or kfk_toppar.msgq.size >= cf.batch_num_messages then
		 	    kfk_toppar.last_send_time = ngxnow();
		 		while kfk_toppar.msgq.size > 0 do
		 			kfk_broker_produce_toppar(kfk_broker, kfk_toppar);
		 		end	
		 	end 	
		 	kfk_toppar = toppars:next(kfk_toppar);
		end
		
		kfk_broker_retry_buf_move(kfk_broker);
		kfk_broker_io_server(kfk_broker);
		kfk_broker_waitrsp_timeout_scan(kfk_broker);
		
        if not kfk.metadata_broker or
           kfk.metadata_broker == const.KFK_BROKER_ID_UA then
            kfk.metadata_broker = kfk_broker.nodeid;
        end

        if kfk.metadata_broker == kfk_broker.nodeid and
           kfk.metadata_refresh_timeout <= ngxnow() then

        	kfk.metadata_refresh_timeout = ngxnow() + meta_time_s;
        	kfk_metadata_req(kfk, kfk_broker, nil);
        end

        if kfk_broker.fails > 0 and kfk_broker.last_fail_time + 120 < ngxnow() then
            kfk_broker.fails = 0;
        end
	end
end

local function kfk_broker_main_loop(kfk_broker)
	local kfk = kfk_broker.kfk;

	local source = "worker exiting";
	local i = 1;
	while not kfk.quit() do
		if bxor(kfk_broker.valid, 0) ~= kfk_broker.valid then
			source = "not valid";
			break;
		end
		if kfk_broker.state ~= const.KFK_BROKER_UP then
			local ok, err = kfk_broker_connect(kfk_broker);
			if not ok then
                source = "connect failure";
				break;
			end
		else
			kfk_broker_producer_server(kfk_broker);
		end
	end
    
    kfk_broker_produce_fail(kfk_broker, const.KFK_RSP_ERR_DESTROY, source);

    if kfk.quit() then
    	kfk_destroy_kfk(kfk_broker.kfk);
    end
end

local function kfk_broker_add(kfk, parent, ip, port, nodeid)
	local brokers = kfk.kfk_brokers;
	local kfk_broker = kfk_broker_new(kfk, ip, port, nodeid);
	
	local co, err = ngxspawn(kfk_broker_main_loop, kfk_broker);
	if not co then
		ngxlog(ngx.ERR, "broker[", kfk_broker.name, "] thread start failure: ", err);
		return nil, err;
	end 
    
    local thread = {
        kfk_thread_link = list.LIST_ENTRY(),
    	co = co,
    	kfk_broker = kfk_broker
    }
    parent.threads:insert_tail(thread);
    
    ngx.log(ngx.DEBUG, "add broker: ", kfk_broker.name);	
    
    brokers:insert_tail(kfk_broker);
    kfk.down_cnt = kfk.down_cnt + 1;
	return kfk_broker;
end

kfk_broker_update = function(kfk_broker, broker)
    local kfk = kfk_broker.kfk;
    local name = broker.host .. ":" .. broker.port;
	local tmp = kfk_broker_find_byname(kfk, name);
	
	if not tmp then
	    kfk_broker_add(kfk, kfk_broker, broker.host, broker.port, broker.nodeid);  
	else
		tmp.valid = bxor(tmp.valid, 0);
		if tmp.nodeid ~= broker.nodeid then
            
            ngx.log(ngx.DEBUG, "update broker nodeid: ", broker.host, ":", broker.port);
            
			tmp.nodeid = broker.nodeid;
		end
	end
end

local function kfk_broker_adds(kfk, brokers)
	local cnt = 0;
	for _, broker in ipairs(brokers) do
		local name = broker.host .. ":" .. broker.port;
		if kfk_broker_find_byname(kfk, name) or
		   kfk_broker_add(kfk, kfk, broker.host, 
                          broker.port, broker.nodeid) then	   			   
			cnt = cnt + 1;
		end
	end
	return cnt;
end

local function kfk_broker_resume(thread)
	local kfk_broker = thread.kfk_broker;
	local co, err = ngxspawn(kfk_broker_main_loop, kfk_broker);
	if not co then
		ngxlog(ngx.ERR, "broker[", kfk_broker.name, "] thread resume failure: ", err);
		return nil, err;
	end
	thread.co = co;
    
    ngx.log(ngx.DEBUG, "resume broker: ", kfk_broker.name);	

    list.init(kfk_broker.threads);
end

local broker = {
	kfk_broker_adds = kfk_broker_adds,
	kfk_broker_resume = kfk_broker_resume
}
return broker
