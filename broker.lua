local util = require "util";
local bit = require "bit";

local list = require "lkfk.list";
local base = require "lkfk.base";
local const = require "lkfk.const";
local func = require "lkfk.func";
local topic = require "lkfk.topic";
local msg = require "lkfk.msg";

local concat = table.concat;
local ipairs = ipairs;
local pairs = pairs;
local tonumber = tonumber;

local ngxnow = ngx.now;
local ngxlog = ngx.log;
local ngxsleep = ngx.sleep;
local ngxspawn = ngx.thread.spawn;
local ngxcrc32 = ngx.crc32_long
local ngxtcp = ngx.socket.tcp
local ERR = ngx.ERR
local DEBUG = ngx.DEBUG

local strlen = string.len
local strsub = string.sub

local bxor = bit.bxor


local function _kfk_destroy_buf(kfk, kfk_buf, err)
    func.kfk_destroy_msgq(kfk, kfk_buf.msgq, err);
end

local function _kfk_buf_new()
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

local function _kfk_broker_new(kfk, host, port, nodeid)
	return {
        kfk_broker_link = list.LIST_ENTRY(),
		host = host,
		port = port,
		name = host .. ":" .. port,
		nodeid = nodeid,
		state = const.KFK_BROKER_DOWN,
		valid = 1,

        fails = 0,
        last_fail_time = 0,
		
		outbuf = list.new("kfk_buf_link"),
		retrybuf = list.new("kfk_buf_link"),
		waitrspbuf = list.new("kfk_buf_link"),
		
		recvbuf = {
			body = "",
			of = 0,
			size = 0
		},
		
		kfk_toppars = list.new("kfk_broker_toppar_link"),
		
		tcp = ngxtcp(),
		kfk = kfk,
		
		corr_id = 1
	}
end

local function _kfk_broker_buf_enq(kfk_broker, kfk_buf, msgbody, api_key, 
									callback, opaque)
	local cf = kfk_broker.kfk.cf;
    local corr_id = kfk_broker.corr_id;

	local kfk_req = {
		base.zero4,							-- 1 req size
		base.set_byte2(api_key),			-- 2 api key
		base.zero2,							-- 3 api version
	    base.set_byte4(corr_id),	-- 4 corrlation id
		base.pack_kfk_string(cf.client_id),	-- 5 client id
		msgbody	        					-- 6 msg body
	}
	local size = 8 + strlen(kfk_req[5]) + strlen(kfk_req[6]);
	kfk_req[1] = base.set_byte4(size);
	
	kfk_buf.callback = callback;
	kfk_buf.opaque = opaque;
    kfk_buf.api_key = api_key;
	kfk_buf.corr_id = corr_id;
	kfk_buf.len = size + 4;
    kfk_buf.kfk_req = concat(kfk_req);
	
	kfk_broker.corr_id = (corr_id + 1) % 2147483648;
	
	local outbuf = kfk_broker.outbuf;
	outbuf:insert_tail(kfk_buf);
    
end

local function _kfk_broker_find_byname(kfk, name)
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

local function _kfk_broker_set_state(kfk_broker, state)
    local kfk = kfk_broker.kfk;
	if state == const.KFK_BROKER_DOWN then
		kfk.down_cnt = kfk.down_cnt + 1;
		if kfk.kfk_brokers.size <= 0 then
			ngxlog(ERR, "all broker down");
		end
	elseif kfk_broker.state == const.KFK_BROKER_DOWN then
		kfk.down_cnt = kfk.down_cnt - 1;
	end
	
	kfk_broker.state = state;
end

local function _kfk_broker_connect(kfk_broker)
	local tcp = kfk_broker.tcp;
	local i = 1;
	local ok, err;
    tcp:settimeout(3000);
	while i <= kfk_broker.kfk.cf.conn_retry_limit do
		ok, err = tcp:connect(kfk_broker.host, kfk_broker.port);
		if ok then
			_kfk_broker_set_state(kfk_broker, const.KFK_BROKER_UP);
			return true;
		end
        ngxsleep(i);
		i = i + 1;
	end
	return nil, err;
end

local function _kfk_broker_buf_retry(kfk_broker, kfk_buf)
	local cf =  kfk_broker.kfk.cf;
	
	kfk_buf.retry_timeout = cf.retry_backoff_ms / 1000 + ngxnow();
	kfk_buf.of = 0;
	kfk_buf.tries = kfk_buf.tries + 1;
	
	kfk_broker.retrybuf:insert_tail(kfk_buf); 

end

local function _kfk_broker_produce_handle(kfk_broker, rspbuf, of)
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


local function _kfk_broker_produce_reply(kfk_broker, err, rspbuf, reqbuf)
	if err == 0 and rspbuf then
		err = _kfk_broker_produce_handle(kfk_broker, rspbuf, 9);
        if err == 0 then
            if util.debug then
                ngxlog(DEBUG, "broker[", kfk_broker.name, "] ", 
                        reqbuf.corr_id, ": ", reqbuf.msgq.size, " produce success");
            end
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
		ngxlog(ERR, "broker[", kfk_broker.name, "] partition ", kfk_toppar.partition,
                " message set ", reqbuf.corr_id, " with ", reqbuf.msgq.size, 
                " messages encountered error: ", const.errstr[err]);
				
		if err == const.KFK_RSP_ERR_REQUEST_TIMED_OUT or
		   err == const.KFK_RSP_ERR_MESSAGE_TIMED_OUT then

			if (reqbuf.tries + 1 < cf.message_send_max_retries) then
				_kfk_broker_buf_retry(kfk_broker, reqbuf);
				return;
			end
			
		elseif err == const.KFK_RSP_ERR_UNKNOWN_TOPIC_OR_PART or
			   err == const.KFK_RSP_ERR_LEADER_NOT_AVAILABLE or
			   err == const.KFK_RSP_ERR_NOT_LEADER_FOR_PARTITION or
			   err == const.KFK_RSP_ERR_BROKER_NOT_AVAILABLE or
			   err == const.KFK_RSP_ERR_REPLICA_NOT_AVAILABLE or
			   err == const.KFK_RSP_ERR_TRANSPORT then
			   
            if cf.kfk_status then
                func.kfk_status_add(kfk_toppar.kfk_topic.name .. kfk_toppar.partition, 
                                    reqbuf.msgq.size)
            end
			list.concat_head(kfk_toppar.msgq, reqbuf.msgq);
			   	
			--start a metadata query
			kfk.meta_query_topic[#kfk.meta_query_topic + 1] = kfk_toppar.kfk_topic;	   	
			return;
		elseif err == const.KFK_BROKER_NOT_VALID then
		
            if cf.kfk_status then
                func.kfk_status_add(kfk_toppar.kfk_topic.name .. kfk_toppar.partition, 
                                    reqbuf.msgq.size)
            end

			list.concat_head(kfk_toppar.msgq, reqbuf.msgq);
            return;
		else
			--[[
			err == const.KFK_RSP_ERR_UNKNOWN or
			err == const.KFK_RSP_ERR_INVALID_MSG
			err == const.KFK_RSP_ERR_INVALID_MSG_SIZE or
			err == const.KFK_RSP_ERR_MSG_SIZE_TOO_LARGE then
            err == const.KFK_RSP_ERR_BAD_MSG
            err == const.KFK_RSP_ERR_DESTROY
			--]]
            
		end
	end
	
	_kfk_destroy_buf(kfk_broker.kfk, reqbuf, err);
end

local function kfk_broker_produce_fail(kfk_broker, err, errstr)
	ngxlog(ERR, "broker[", kfk_broker.name, "] produce failed: ", errstr);
	
    local kfk = kfk_broker.kfk;
	_kfk_broker_set_state(kfk_broker, const.KFK_BROKER_DOWN);
	
	local tcp = kfk_broker.tcp;
	tcp:close();
 
    local toppars = kfk_broker.kfk_toppars;
    local head = toppars.head;
    local kfk_toppar = head[toppars.key].next;
    while kfk_toppar ~= head do
        topic.kfk_toppar_broker_delegate(kfk_toppar, nil);
        kfk_toppar = head[toppars.key].next;
    end
       
	local tmpq = list.new("kfk_buf_link");
    if kfk.cf.kfk_status then
        func.kfk_status_set("waitrspbuf", 0);
    end
    
    if kfk_broker.waitrspbuf.size > 0 then
    	list.concat(tmpq, kfk_broker.waitrspbuf);
    end
    if kfk_broker.outbuf.size > 0 then
	    list.concat(tmpq, kfk_broker.outbuf);
	end

	head = tmpq.head;
	local kfk_buf = head[tmpq.key].next;
	while kfk_buf ~= head do
		kfk_buf.callback(kfk_broker, err, nil, kfk_buf);
		kfk_buf = tmpq:next(kfk_buf);
	end
    
    if err ~= const.KFK_RSP_ERR_DESTROY and err ~= const.KFK_BROKER_NOT_VALID then
		-- start a metadta req
	    kfk.meta_query_topic = kfk.kfk_topics;
	else
        ngxlog(ngx.WARN, "broker[", kfk_broker.name, "] remove from kfk cluster");
        kfk.kfk_brokers:remove(kfk_broker);
	end
end

local function _kfk_broker_waitrsp_find(kfk_broker, key)
	local waitrspbuf = kfk_broker.waitrspbuf;
	local head = waitrspbuf.head;
	local kfk_buf = head[waitrspbuf.key].next;
	while kfk_buf ~= head do

		if kfk_buf.corr_id == key then
			kfk_broker.waitrspbuf:remove(kfk_buf);

            if kfk_broker.kfk.cf.kfk_status then
                func.kfk_status_add("waitrspbuf", -kfk_buf.msgq.size);
            end

            return kfk_buf;
		elseif kfk_buf.corr_id > key then
			return nil;
		end
		kfk_buf = waitrspbuf:next(kfk_buf);
	end
	return nil;
end

local function _kfk_broker_send(kfk_broker)
	local waitrspbuf = kfk_broker.waitrspbuf;
	local kfk = kfk_broker.kfk;
	local cf = kfk.cf;
	
	local outbuf = kfk_broker.outbuf;
	local tcp = kfk_broker.tcp;
    
	local head = outbuf.head;
	local kfk_buf = head[outbuf.key].next;
	while kfk_buf ~= head do
		if kfk_buf.corr_id == 0 and kfk_broker.waitrspbuf.size > 0 then
			return true;
		end
        --[[		
		local kfk_req = strsub(kfk_buf.kfk_req, kfk_buf.of + 1);
		--]]
        local kfk_req = kfk_buf.kfk_req;
		local bytes, err = tcp:send(kfk_req);
		if not bytes then
			kfk_broker_produce_fail(kfk_broker, const.KFK_RSP_ERR_TRANSPORT, 
							 "send failed: " .. err);
			return false;
		end
        --[[
		kfk_buf.of = kfk_buf.of + bytes;
		--]]
		outbuf:remove(kfk_buf);

		kfk_buf.waitrsp_timeout = ngxnow() + cf.message_timeout_ms / 1000;
		if cf.request_required_acks > 0 then
			waitrspbuf:insert_tail(kfk_buf);

            if cf.kfk_status then
                func.kfk_status_add("waitrspbuf", kfk_buf.msgq.size);
            end
        else
			kfk_buf.callback(kfk_broker, 0, nil, kfk_buf);
		end
		
		kfk_buf = head[outbuf.key].next;
	end
    return true;
end

local function _kfk_recv(tcp, recvbuf, size)
    local data, err, partial = tcp:receive(size);
    if not data then
        if err ~= "timeout" then
            return 0, err;
        end
        
        if util.log then
            ngxlog(DEBUG, "receive timeout, already receive ",
                           recvbuf.of, " bytes");
        end

        local len = strlen(partital or "");
        if len > 0 then
            recvbuf.of = recvbuf.of + len;
            recvbuf.body = recvbuf.body .. partial;
        end
        return len, nil;
    end

    recvbuf.of = recvbuf.of + strlen(data);
    recvbuf.body = recvbuf.body .. data;

    return size, nil;
end

local function _kfk_broker_recv(kfk_broker)
	local recvbuf = kfk_broker.recvbuf;
    local tcp = kfk_broker.tcp;
	
	if recvbuf.size == 0 then 
        local size = 20 - recvbuf.of;
        local n, err = _kfk_recv(tcp, recvbuf, size);
        if err then
		    kfk_broker_produce_fail(kfk_broker, const.KFK_RSP_ERR_TRANSPORT, 
								"receive failed: " .. err or "");
			return 0;
        end

        if recvbuf.of >= 4 then
            recvbuf.size = base.get_byte4(recvbuf.body, 1);
        end
        if n ~= size then
            return 0;
        end
	end
	
	local size = recvbuf.size - recvbuf.of + 4;
	if size > 0 then
        local n, err = _kfk_recv(tcp, recvbuf, size);
        if err then
		    kfk_broker_produce_fail(kfk_broker, const.KFK_RSP_ERR_TRANSPORT, 
								"receive failed: " .. err);
		    return 0;
        end
    
	else    
		local corr_id = base.get_byte4(recvbuf.body, 5);	
		
		local kfk_buf = _kfk_broker_waitrsp_find(kfk_broker, corr_id);
		if not kfk_buf then
			ngxlog(ERR, "broker[", kfk_broker.name, 
                        "] out of time response because of unknown corrid: ", corr_id);
			return 1;
		end
        
		kfk_buf.callback(kfk_broker, 0, recvbuf, kfk_buf);
	end
	
	return 1;
end


local function _kfk_broker_io_server(kfk_broker)
    local flag = false;

    if kfk_broker.outbuf.size > 0 then
        flag = true;
        if not _kfk_broker_send(kfk_broker) then
            return;
        end
    end

    local waitrspbuf = kfk_broker.waitrspbuf;
    if waitrspbuf.size > 0 then
        flag = true;
        local n = _kfk_broker_recv(kfk_broker);
        while n > 0 and waitrspbuf.size > 0 do
            n = _kfk_broker_recv(kfk_broker);
        end
    end

    if not flag then
        ngx.sleep(kfk_broker.kfk.cf.queue_buffering_max_ms / 3000);
    end
end

local function _kfk_broker_retry_buf_move(kfk_broker)
    local cf = kfk_broker.kfk.cf;
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

local function _kfk_broker_waitrsp_timeout_scan(kfk_broker)
    local cf = kfk_broker.kfk.cf;
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

        if cf.kfk_status then
            func.kfk_status_add("waitrspbuf", -kfk_buf.msgq.size);
        end
        kfk_buf.callback(kfk_broker, const.KFK_RSP_ERR_MESSAGE_TIMED_OUT, 
						 nil, kfk_buf);
		kfk_buf = head[waitrspbuf.key].next;
	end
end

local function _kfk_broker_produce_toppar(kfk_broker, kfk_toppar)
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
	local kfk_buf = _kfk_buf_new();
	
	local msgset_size = 0;	
	local msgq = kfk_toppar.msgq;
	local n = msgq.size;
	local msgcnt = n < cf.batch_num_messages and n or cf.batch_num_messages;

    if cf.kfk_status then
        func.kfk_status_add(kfk_topic.name .. kfk_toppar.partition, -msgcnt);
    end
    
	local buf = {};
	local head = msgq.head;
	local kfk_msg = head[msgq.key].next;
	while msgcnt > 0 do
	    
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
	_kfk_broker_buf_enq(kfk_broker, kfk_buf, msgbody, const.KFK_PRODUCE_REQ,
						_kfk_broker_produce_reply, kfk_toppar);
end

local function _kfk_broker_send_msgs(cf, kfk_broker, time_s)
    local queue_time_s = time_s or cf.queue_buffering_max_ms / 1000;
    local toppars = kfk_broker.kfk_toppars;
    local now = ngxnow();
    local head = toppars.head;
    local kfk_toppar = head[toppars.key].next;
    while kfk_toppar ~= head do

        local elsape = kfk_toppar.last_send_time +
                       queue_time_s -
                       now;
        if elsape <= 0 or kfk_toppar.msgq.size >= cf.batch_num_messages then
            kfk_toppar.last_send_time = now;
            while kfk_toppar.msgq.size > 0 do
                _kfk_broker_produce_toppar(kfk_broker, kfk_toppar);
            end	
        end 	
        kfk_toppar = toppars:next(kfk_toppar);
    end
end

local function _kfk_broker_producer_server(kfk_broker)
	local kfk = kfk_broker.kfk;
	local cf = kfk.cf;
    
	kfk_broker.tcp:settimeout(cf.queue_buffering_max_ms / 3);
	while not kfk.quit() and kfk_broker.state == const.KFK_BROKER_UP do

		if bxor(kfk_broker.valid, 0) ~= kfk_broker.valid then
			break;
		end

        _kfk_broker_send_msgs(cf, kfk_broker);
		_kfk_broker_retry_buf_move(kfk_broker);
		_kfk_broker_io_server(kfk_broker);
		_kfk_broker_waitrsp_timeout_scan(kfk_broker);

        if kfk_broker.fails > 0 and kfk_broker.last_fail_time + 120 < ngxnow() then
            kfk_broker.fails = 0;
        end
	end
	
end

local function _kfk_broker_main_loop(kfk_broker)
	local kfk = kfk_broker.kfk;
	
	local err = const.KFK_RSP_ERR_DESTROY;
	local source = "worker exiting";
	while not kfk.quit() do
		if bxor(kfk_broker.valid, 0) ~= kfk_broker.valid then
			err = const.KFK_BROKER_NOT_VALID;
			source = "not valid";
			break;
		end
		if kfk_broker.state ~= const.KFK_BROKER_UP then
			local ok, err = _kfk_broker_connect(kfk_broker);
			if not ok then
                ngx.sleep(kfk.cf.conn_retry_timeout);
			end
		else
			_kfk_broker_producer_server(kfk_broker);
		end
	end
    
    if kfk.quit() then
        ngx.update_time();
        ngx.log(ngx.INFO, "yaowei step1 time: ", ngx.now());

        _kfk_broker_send_msgs(kfk.cf, kfk_broker, 0);

        kfk_broker.tcp:settimeout(200);
        for i = 1, 3 do
            if kfk_broker.outbuf.size > 0 or kfk_broker.waitrspbuf.size > 0 then
		        _kfk_broker_io_server(kfk_broker);
            else 
                break;
            end
        end
    end

    kfk_broker_produce_fail(kfk_broker, err, source);

end

local function _kfk_broker_startup(kfk_broker)
    local kfk = kfk_broker.kfk;
    local co, err = ngxspawn(_kfk_broker_main_loop, kfk_broker);
    if not co then
        ngxlog(ERR, "broker[", kfk_broker.name, "] thread start failure: ", err);
        return nil, err;
    end 
    local thread = {
        co = co, 
        kfk_broker = kfk_broker, 
        kfk_thread_link = list.LIST_ENTRY()
    }
    kfk.threads:insert_tail(thread);
    
    if util.debug then
        ngxlog(DEBUG, "start broker: ", kfk_broker.name);	
    end
end

local function _kfk_broker_add(kfk, startup, node)
	local brokers = kfk.kfk_brokers;
	local kfk_broker = _kfk_broker_new(kfk, node.host, node.port, node.nodeid);
	
    if startup then
        _kfk_broker_startup(kfk_broker);
    else
        kfk.new_brokers[#kfk.new_brokers + 1] = node;
    end
    
    if util.debug then
        ngxlog(DEBUG, "add broker: ", kfk_broker.name);	
    end

    brokers:insert_tail(kfk_broker);
    kfk.down_cnt = kfk.down_cnt + 1;
	return kfk_broker;
end

local function kfk_broker_update(kfk, broker)
    local name = broker.host .. ":" .. broker.port;
	local tmp = _kfk_broker_find_byname(kfk, name);
	
	if not tmp then
	    _kfk_broker_add(kfk, false, broker);  
	else
		tmp.valid = bxor(tmp.valid, 0);
		if tmp.nodeid ~= broker.nodeid then

            if util.debug then 
                ngxlog(DEBUG, "update broker nodeid: ", broker.host, ":", broker.port);
            end

			tmp.nodeid = broker.nodeid;
		end
	end
end

local function kfk_broker_adds(kfk, brokers)
	local cnt = 0;
	for _, broker in ipairs(brokers) do
		local name = broker.host .. ":" .. broker.port;
        local kfk_broker = _kfk_broker_find_byname(kfk, name);
        if not kfk_broker then
            if _kfk_broker_add(kfk, true, broker) then
			    cnt = cnt + 1;
            end
        elseif kfk_broker.state == const.KFK_BROKER_DOWN then
            if _kfk_broker_startup(kfk_broker) then
                cnt = cnt + 1;
            end
		end
	end
	return cnt;
end

local function kfk_broker_resume(thread)
	local kfk_broker = thread.kfk_broker;

    if util.debug then
        ngxlog(DEBUG, "resume broker: ", kfk_broker.name);	
    end

	local co, err = ngxspawn(_kfk_broker_main_loop, kfk_broker);
	if not co then
		ngxlog(ERR, "broker[", kfk_broker.name, "] thread resume failure: ", err);
		return nil, err;
	end
	thread.co = co;
    
end

local broker = {
	kfk_broker_update = kfk_broker_update,
    kfk_broker_produce_fail = kfk_broker_produce_fail,
	kfk_broker_adds = kfk_broker_adds,
	kfk_broker_resume = kfk_broker_resume
}
return broker
