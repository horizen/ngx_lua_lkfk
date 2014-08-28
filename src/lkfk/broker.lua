local bit = require "bit";
local util = require "lkfk.util";
local alarm = require "lkfk.alarm";
local list = require "lkfk.list";
local const = require "lkfk.const";
local func = require "lkfk.func";
local msg = require "lkfk.msg";

local concat = table.concat;
local ipairs = ipairs;
local pairs = pairs;
local tonumber = tonumber;
local tabrm = table.remove;

local ngxnow = ngx.now;
local ngxlog = ngx.log;
local ngxsleep = ngx.sleep;
local ngxspawn = ngx.thread.spawn;
local ngxcrc32 = ngx.crc32_long
local ngxtcp = ngx.socket.tcp
local ngxwait = ngx.thread.wait
local corunning = coroutine.running
local ERR = ngx.ERR
local WARN = ngx.WARN
local CRIT = ngx.CRIT
local INFO = ngx.INFO
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
		
		callback = false,
		data = "",
		api_key = 0,
            
		tries = 0,
		corr_id = 0,
		
		len = 0,
		of = 0,
		kfk_req = ""
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
		update_time = 0,
		
		outbuf = list.new("kfk_buf_link"),
		retrybuf = list.new("kfk_buf_link"),
		waitbuf = list.new("kfk_buf_link"),
		
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

local function _kfk_broker_buf_enq(kfk_broker, kfk_buf, msgbody, api_key, callback, data)
	local cf = kfk_broker.kfk.cf;
    local corr_id = kfk_broker.corr_id;

	local kfk_req = {
		util.zero4,							-- 1 req size
		util.set_byte2(api_key),			-- 2 api key
		util.zero2,							-- 3 api version
	    util.set_byte4(corr_id),	-- 4 corrlation id
		util.pack_kfk_string(cf.client_id),	-- 5 client id
		msgbody	        					-- 6 msg body
	}
	local size = 8 + strlen(kfk_req[5]) + strlen(kfk_req[6]);
	kfk_req[1] = util.set_byte4(size);
	
	kfk_buf.callback = callback;
	kfk_buf.data = data;
    kfk_buf.api_key = api_key;
	kfk_buf.corr_id = corr_id;
	kfk_buf.len = size + 4;
    kfk_buf.kfk_req = concat(kfk_req);
	
	kfk_broker.corr_id = (corr_id + 1) % 2147483648;
	
	kfk_broker.outbuf:insert_tail(kfk_buf);
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

local function kfk_broker_find_byid(kfk, id)
    local brokers = kfk.kfk_brokers;
    local head = brokers.head;
    local kfk_broker = head[brokers.key].next;
    while kfk_broker ~= head do
		if kfk_broker.nodeid == id then
			return kfk_broker;
		end
        kfk_broker = brokers:next(kfk_broker);
	end
	return nil;
end

local function _kfk_broker_set_state(kfk_broker, state)
    local kfk = kfk_broker.kfk;
	if state == const.KFK_BROKER_DOWN then
        kfk_broker.nodeid = const.KFK_BROKER_ID_UA;
		kfk.down_cnt = kfk.down_cnt + 1;
		if kfk.down_cnt >= kfk.kfk_brokers.size then
            --[[
            alarm.add("All kafka broker down");
            --]]
            if not kfk.quit() then
			    ngxlog(CRIT, "[kafka] all broker down");
            end
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
    tcp:settimeout(1000);
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
	
	local top_cnt, of = util.get_byte4(data, of);
	if top_cnt ~= 1 then
		return const.KFK_RSP_ERR_BAD_MSG;
	end
	
	local topic, of = util.get_kfk_string(data, of);
	local part_cnt, of = util.get_byte4(data, of);
	if part_cnt ~= 1 then
		return const.KFK_RSP_ERR_BAD_MSG;
	end
	
	local partition, of = util.get_byte4(data, of);
	local err, of = util.get_byte2(data, of);
	local offset, of = util.get_byte8(data, of);
	
	return err;
end


local function _kfk_broker_produce_reply(kfk_broker, err, rspbuf, reqbuf)
	if err == 0 and rspbuf then
		err = _kfk_broker_produce_handle(kfk_broker, rspbuf, 9);
        if err == 0 and util.debug then
            ngxlog(DEBUG, "[kafka] [", kfk_broker.name, "] ", 
                           reqbuf.msgq.size, " messages produce success");
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
	local kfk_toppar = reqbuf.data;
	if err ~= 0 then
		ngxlog(WARN, "[kafka] [", kfk_broker.name, "] with ", reqbuf.msgq.size, 
                	"messages encountered error: ", const.errstr[err]);
				
		if err == const.KFK_RSP_ERR_REQUEST_TIMED_OUT then

			if (reqbuf.tries + 1 <= cf.message_send_max_retries) then
				_kfk_broker_buf_retry(kfk_broker, reqbuf);
				return;
			end
			
		elseif err == const.KFK_RSP_ERR_UNKNOWN_TOPIC_OR_PART or
			   err == const.KFK_RSP_ERR_LEADER_NOT_AVAILABLE or
			   err == const.KFK_RSP_ERR_NOT_LEADER_FOR_PARTITION or
			   err == const.KFK_RSP_ERR_BROKER_NOT_AVAILABLE then
	   
            if cf.kfk_status then
                func.kfk_status_add(kfk_toppar.kfk_topic.name .. kfk_toppar.partition, 
                                    reqbuf.msgq.size)
            end
			list.concat_head(kfk_toppar.msgq, reqbuf.msgq);
			   	
			--start a metadata query
            func.add_meta_query(kfk);
			return;					   
		elseif err == const.KFK_RSP_ERR_SOCKET or
			   err == const.KFK_BROKER_NOT_VALID or
			   err == const.KFK_RSP_ERR_DESTROY then
			   
            if cf.kfk_status then
                func.kfk_status_add(kfk_toppar.kfk_topic.name .. kfk_toppar.partition, 
                                    reqbuf.msgq.size)
            end

			list.concat_head(kfk_toppar.msgq, reqbuf.msgq);
            return;
		else
			--[[
		    err == const.KFK_RSP_ERR_MESSAGE_TIMED_OUT
			err == const.KFK_RSP_ERR_UNKNOWN or
			err == const.KFK_RSP_ERR_INVALID_MSG
			err == const.KFK_RSP_ERR_INVALID_MSG_SIZE
			err == const.KFK_RSP_ERR_MSG_SIZE_TOO_LARGE
            err == const.KFK_RSP_ERR_BAD_MSG
			--]]
            
		end
	end
	
	_kfk_destroy_buf(kfk_broker.kfk, reqbuf, err);
end

local function kfk_broker_produce_fail(kfk_broker, err, ...)
    if err ~= 0 then
	    ngxlog(ERR, "[kafka] [", kfk_broker.name, "] produce failed: ", const.errstr[err], ":", ...);
	end

    local kfk = kfk_broker.kfk;
	_kfk_broker_set_state(kfk_broker, const.KFK_BROKER_DOWN);
	
	local tcp = kfk_broker.tcp;
	tcp:close();
    
    local retrybuf = kfk_broker.retrybuf;
    local outbuf = kfk_broker.outbuf;
    local waitbuf = kfk_broker.waitbuf;

    if waitbuf.size > 0 and kfk.cf.kfk_status then
        local head = waitbuf.head;
        local kfk_buf = head[waitbuf.key].next;
        while kfk_buf ~= head do
            func.kfk_status_add("waitbuf", -kfk_buf.msgq.size);
            kfk_buf = waitbuf:next(kfk_buf);
        end
    end

    local tmpbuf = list.new("kfk_buf_link");
    if waitbuf.size > 0 then
        list.concat(tmpbuf, waitbuf);
    end
    if outbuf.size > 0 then
        list.concat(tmpbuf, outbuf);
    end
    if retrybuf.size > 0 then
        list.concat(tmpbuf, retrybuf);
    end

	local head = tmpbuf.head;
	local kfk_buf = head[tmpbuf.key].next;
	while kfk_buf ~= head do
        kfk_buf.callback(kfk_broker, err, nil, kfk_buf);
		kfk_buf = tmpbuf:next(kfk_buf);
	end
    
    if err ~= const.KFK_RSP_ERR_DESTROY and err ~= const.KFK_BROKER_NOT_VALID then
		--start a metadata query
        func.add_meta_query(kfk);
        return;
    end

    ngxlog(WARN, "[kafka] [", kfk_broker.name, "] remove from kfk cluster");

    kfk.kfk_brokers:remove(kfk_broker);
    kfk.down_cnt = kfk.down_cnt - 1;

    if err == const.KFK_RSP_ERR_DESTROY then
        return;
    end

    for i = 1, #kfk.cos do
        if corunning() == kfk.cos[i] then
            tabrm(kfk.cos, i);
        end
    end

end

local function _kfk_broker_waitrsp_find(kfk_broker, key)
	local waitbuf = kfk_broker.waitbuf;
	local head = waitbuf.head;
	local kfk_buf = head[waitbuf.key].next;
	while kfk_buf ~= head do

		if kfk_buf.corr_id == key then
			kfk_broker.waitbuf:remove(kfk_buf);

            if kfk_broker.kfk.cf.kfk_status then
                func.kfk_status_add("waitbuf", -kfk_buf.msgq.size);
            end

            return kfk_buf;
		elseif kfk_buf.corr_id > key then
			return nil;
		end
		kfk_buf = waitbuf:next(kfk_buf);
	end
	return nil;
end


local function _kfk_recv(tcp, recvbuf, size)
    local data, err, partial = tcp:receive(size);
    if not data then
        if err ~= "timeout" then
            return nil, err;
        end
        
        local len = strlen(partial or "");
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
    local kfk = kfk_broker.kfk;

	while not kfk.quit() do
		if recvbuf.size == 0 then 
	        local size = 20 - recvbuf.of;
	        local n, err = _kfk_recv(tcp, recvbuf, size);
	        if not n then
			    kfk_broker_produce_fail(kfk_broker, const.KFK_RSP_ERR_SOCKET, "recv ", err);
				break;
	        end
	
	        if recvbuf.of >= 4 then
	            recvbuf.size = util.get_byte4(recvbuf.body, 1);
	        end
	        if n ~= size then
	            break;
	        end
		end
		
		local size = recvbuf.size - recvbuf.of + 4;
		if size > 0 then
	        local n, err = _kfk_recv(tcp, recvbuf, size);
	        if not n then
			    kfk_broker_produce_fail(kfk_broker, const.KFK_RSP_ERR_SOCKET, "recv ", err);
			    break;
	        end
	    	if n ~= size then
	    		break;
	    	end
		end  
		--recv done 
		local corr_id = util.get_byte4(recvbuf.body, 5);	
			
		local kfk_buf = _kfk_broker_waitrsp_find(kfk_broker, corr_id);
		if not kfk_buf then
			ngxlog(ERR, "[kafka] [", kfk_broker.name, 
	                    "] out of time response because of unknown corrid: ", corr_id);
            --destroy recvbuf 
            if recvbuf then
                recvbuf.body = "";
                recvbuf.of = 0;
                recvbuf.size = 0;
            end
        else
            kfk_buf.callback(kfk_broker, 0, recvbuf, kfk_buf);
		end
	        
	end
end

local function _kfk_broker_waitrsp_timeout_scan(kfk_broker)
    local cf = kfk_broker.kfk.cf;
		
	local waitbuf = kfk_broker.waitbuf;
	if waitbuf.size <= 0 then
		return;
	end
	
	local now = ngxnow();
	local head = waitbuf.head;
	local kfk_buf = head[waitbuf.key].next;
	while kfk_buf ~= head do
		if kfk_buf.waitrsp_timeout > now then
			return;
		end
		
		waitbuf:remove(kfk_buf);

        if cf.kfk_status then
            func.kfk_status_add("waitbuf", -kfk_buf.msgq.size);
        end
        kfk_buf.callback(kfk_broker, const.KFK_RSP_ERR_MESSAGE_TIMED_OUT, 
						 nil, kfk_buf);
		kfk_buf = head[waitbuf.key].next;
	end
end

local function _kfk_broker_recv_loop(kfk_broker)
	local kfk = kfk_broker.kfk;
	local cf = kfk.cf;
    
	while not kfk.quit() and 
		  kfk_broker.state == const.KFK_BROKER_UP and
		  kfk_broker.update_time == kfk.update_time do

		_kfk_broker_recv(kfk_broker);
		_kfk_broker_waitrsp_timeout_scan(kfk_broker);
	end
end

local function _kfk_broker_send(kfk_broker)
	local waitbuf = kfk_broker.waitbuf;
	local kfk = kfk_broker.kfk;
	local cf = kfk.cf;
	
	local outbuf = kfk_broker.outbuf;
	local tcp = kfk_broker.tcp;
    
	local head = outbuf.head;
	local kfk_buf = head[outbuf.key].next;
	while kfk_buf ~= head do
		if kfk_buf.corr_id == 0 and kfk_broker.waitbuf.size > 0 then
			-- let recv coroutine recv data
			ngxsleep(0.1);
			return;
		end

        local kfk_req = kfk_buf.kfk_req;
		local bytes, err = tcp:send(kfk_req);
		if not bytes then
			kfk_broker_produce_fail(kfk_broker, const.KFK_RSP_ERR_SOCKET, "send ", err);
			return;
		end

		outbuf:remove(kfk_buf);

		if cf.request_required_acks > 0 then
			kfk_buf.waitrsp_timeout = ngxnow() + cf.message_timeout_ms / 1000;
			waitbuf:insert_tail(kfk_buf);

            if cf.kfk_status then
                func.kfk_status_add("waitbuf", kfk_buf.msgq.size);
            end
        else
			kfk_buf.callback(kfk_broker, 0, nil, kfk_buf);
		end
		
		kfk_buf = head[outbuf.key].next;
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

local function _kfk_broker_produce_toppar(kfk_broker, kfk_toppar)
	local cf = kfk_broker.kfk.cf;

	local kfk_topic = kfk_toppar.kfk_topic;
	
	local buf = {
		util.set_byte2(cf.request_required_acks), -- 1 required_acks
		util.set_byte4(cf.request_timeout_ms),	-- 2 timeout
		util.set_byte4(1),						-- 3 topic count

		util.pack_kfk_string(kfk_topic.name), 	-- 4 topic_name
		util.set_byte4(1),					    -- 5 part_cnt
		util.set_byte4(kfk_toppar.partition),	-- 6 partition
		util.zero4							    -- 7 msgset size
	}
	
	local part = {
			util.zero8,					-- 1 offset 
			util.zero4,					-- 2 msg_size 
			util.zero4,					-- 3 crc 
			util.zero1,					-- 4 magic_byte 
			util.zero1,					-- 5 attr 
		 	"",							-- 6 key 
		 	""							-- 7 value 
	};
	
	local kfk_buf = _kfk_buf_new();
	
	local msgset_size = 0;	
	local msgq = kfk_toppar.msgq;
	local n = msgq.size;
	local msgcnt = n < cf.batch_num_messages and n or cf.batch_num_messages;

    if cf.kfk_status then
        func.kfk_status_add(kfk_topic.name .. kfk_toppar.partition, -msgcnt);
    end
    
	local head = msgq.head;
	local kfk_msg = head[msgq.key].next;
	while msgcnt > 0 do
	    
		local key = util.pack_kfk_bytes(nil);
		local value = util.pack_kfk_bytes(kfk_msg.str);
		local crc = ngxcrc32(part[4] .. 
						  	part[5] ..
						  	key ..
						  	value);
		local msg_size = strlen(value) +
						 strlen(key) +
						 6;
						 
		part[2] = util.set_byte4(msg_size);
		part[3] = util.set_byte4(crc);
		part[6] = key;
		part[7] = value;
		
		buf[#buf + 1] = concat(part);
		msgset_size = msgset_size +
					  msg_size +
					  12;
					  
		msgcnt = msgcnt - 1;
	    msgq:remove(kfk_msg);

        kfk_buf.msgq:insert_tail(kfk_msg);

		kfk_msg = head[msgq.key].next;
	end
	
	buf[7] = util.set_byte4(msgset_size);
	
	_kfk_broker_buf_enq(kfk_broker, kfk_buf, concat(buf), const.KFK_PRODUCE_REQ,
						_kfk_broker_produce_reply, kfk_toppar);
end

local function _kfk_broker_enq_msgs(cf, kfk_broker, time_s)
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

local function _kfk_broker_send_loop(kfk_broker)
	local kfk = kfk_broker.kfk;
	local cf = kfk.cf;
    
    local self = corunning();

	while not kfk.quit() and 
		  kfk_broker.state == const.KFK_BROKER_UP and
		  kfk_broker.update_time == kfk.update_time do

        _kfk_broker_enq_msgs(cf, kfk_broker);
		_kfk_broker_retry_buf_move(kfk_broker);
        if kfk_broker.outbuf.size > 0 then
		    _kfk_broker_send(kfk_broker);
        else
            if util.debug then
                ngxlog(DEBUG, "[kafka] [", kfk_broker.name, "] no message pending");
            end
            ngxsleep(cf.queue_buffering_max_ms / 1000);
        end
	end
	
end


local function _kfk_broker_main_loop(kfk_broker)
	local kfk = kfk_broker.kfk;
	local recv_co;
	local err = 0;
    local flag = false;
	while not kfk.quit() do
		if kfk_broker.update_time ~= kfk.update_time then
			err = const.KFK_BROKER_NOT_VALID;
			break;
		end
		
		if kfk_broker.state ~= const.KFK_BROKER_UP then
			local ok, err = _kfk_broker_connect(kfk_broker);
			if not ok then
                for i = 1, kfk.cf.conn_retry_timeout, 2 do
                    ngxsleep(2);
                end
			end
            if util.debug then
                ngxlog(DEBUG, "[kafka] [", kfk_broker.name, "] connected ");
            end

            -- if is not the first time connected
            if kfk_broker.update_time > 0 then
                --start metadata query
                func.add_meta_query(kfk);
            end
		end

		if kfk_broker.state == const.KFK_BROKER_UP then
			kfk_broker.tcp:settimeout(kfk.cf.queue_buffering_max_ms);
					
			recv_co = ngxspawn(_kfk_broker_recv_loop, kfk_broker);
			_kfk_broker_send_loop(kfk_broker);
    		ngxwait(recv_co);
		end
	end

    if kfk.quit() then
        _kfk_broker_enq_msgs(kfk.cf, kfk_broker, 0);

        kfk_broker.tcp:settimeout(200);
        for i = 1, 3 do
            if kfk_broker.outbuf.size > 0 then
		        _kfk_broker_send(kfk_broker);
		    end
		    
		    if kfk_broker.waitbuf.size > 0 then
		    	_kfk_broker_recv(kfk_broker);
		    else 
		    	break;
		    end
        end

        if coroutine.status(kfk.main_co) == "suspended" then
            coroutine.resume(kfk.main_co);
        end
    end

    kfk_broker_produce_fail(kfk_broker, err);
end

local function _kfk_broker_add(kfk, kfk_broker)
	if util.debug then
    	ngxlog(DEBUG, "[kafka] [", kfk_broker.name, "] added");	
    end
    	
	kfk.kfk_brokers:insert_tail(kfk_broker);
	kfk.down_cnt = kfk.down_cnt + 1;
	    
	local co, err = ngxspawn(_kfk_broker_main_loop, kfk_broker);
    if not co then
        ngxlog(ERR, "[kafka] ", kfk_broker.name, " thread start failure: ", err);
        return;
    end 
    kfk.cos[#kfk.cos + 1] = co;
    
    if util.debug then
        ngxlog(DEBUG, "[kafka] [", kfk_broker.name, "] started");
    end
end

local function kfk_broker_update(kfk, broker)
    local name = broker.host .. ":" .. broker.port;
	local tmp = _kfk_broker_find_byname(kfk, name);
	
	if not tmp then
		local kfk_broker = 
					_kfk_broker_new(kfk, broker.host, broker.port, broker.nodeid);
        kfk_broker.update_time = kfk.update_time;
		_kfk_broker_add(kfk, kfk_broker);
	else
		tmp.update_time = kfk.update_time;
		if tmp.nodeid ~= broker.nodeid then
			tmp.nodeid = broker.nodeid;
		end
	end
end

local broker = {
    kfk_broker_find_byid = kfk_broker_find_byid,
	kfk_broker_update = kfk_broker_update,
    kfk_broker_produce_fail = kfk_broker_produce_fail
}
return broker
