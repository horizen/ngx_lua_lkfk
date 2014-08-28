local util = require "lkfk.util";
local topic = require "lkfk.topic";
local broker = require "lkfk.broker";
local const = require "lkfk.const";
local func = require "lkfk.func";

local tonumber = tonumber;
local concat = table.concat;
local ipairs = ipairs;
local ngxlog = ngx.log;
local strlen = string.len
local ERR = ngx.ERR;
local DEBUG = ngx.DEBUG;

local function _kfk_metadata_handle(kfk, data)
	-- we skip size and corr id
	local of = 9;

	local broker_cnt;
	broker_cnt, of = util.get_byte4(data, of);
	local brokers = util.new_tab(broker_cnt, 0);
	
	for i = 1, broker_cnt do
		local tmp = {};
		tmp.nodeid, of = util.get_byte4(data, of);
		tmp.host, of = util.get_kfk_string(data, of);
		tmp.port, of = util.get_byte4(data, of);
		brokers[#brokers + 1] = tmp;
	end
	
	local top_cnt;
	top_cnt, of = util.get_byte4(data, of);
    local topic_metas = util.new_tab(top_cnt, 0);
	for i = 1, top_cnt do
		local tmp = {};
		tmp.err, of = util.get_byte2(data, of);
		tmp.name, of = util.get_kfk_string(data, of);

		tmp.part_cnt, of = util.get_byte4(data, of);
		local part_meta = util.new_tab(tmp.part_cnt, 0);
		for j = 1, tmp.part_cnt do
			local tmp2 = {};
			tmp2.err, of = util.get_byte2(data, of);
			tmp2.id, of = util.get_byte4(data, of);
			tmp2.leader, of = util.get_byte4(data, of);
			
			tmp2.rep_size, of = util.get_byte4(data, of);
			local tmp3 = util.new_tab(tmp2.rep_size, 0);
			for r = 1, tmp2.rep_size do
				tmp3[r], of = util.get_byte4(data, of);
			end
			tmp2.rep = tmp3;
			
			tmp2.isr_size, of = util.get_byte4(data, of);
			local tmp3 = util.new_tab(tmp2.isr_size, 0);
			for r = 1, tmp2.isr_size do
				tmp3[r], of = util.get_byte4(data, of);
			end
			tmp2.isr = tmp3;
			
			part_meta[#part_meta + 1] = tmp2;
		end
		tmp.part_meta = part_meta;
		
		topic_metas[#topic_metas + 1] = tmp;
	end
    
    kfk.update_time = kfk.update_time + 1;
	for _, node in ipairs(brokers) do
		broker.kfk_broker_update(kfk, node);
	end
	
	for _, tm in ipairs(topic_metas) do
		topic.kfk_topic_metadata_update(kfk, tm);
	end

end


local function _send_req(kfk, req, meta_broker)
	local hp = util.split(meta_broker, ":");
	
	local tcp = kfk.tcp;
	tcp:settimeout(3000);
	
	local ok, err = tcp:connect(hp[1], tonumber(hp[2]));
	if not ok then
	    return nil, err;
	end
	
	local bytes, err = tcp:send(req);
	if not bytes then
	    return nil, err;
	end
	
	local body, err = tcp:receive(20);
	if not body then
	    return nil, err;
	end
	
	local size = util.get_byte4(body, 1);
	local size = size - strlen(body) + 4;
	if size > 0 then
	    local data, err = tcp:receive(size);
	    if not data then
	        return nil, err;
	    end
	    body = body .. data;
	end
	return body;
end

local _kfk_req = {
	util.zero4,								-- 1 req size
	util.set_byte2(const.KFK_METADATA_REQ),	-- 2 api key
	util.zero2,								-- 3 api version
	util.zero4,	                        	-- 4 corrlation id
	util.pack_kfk_string("lkfk"),			-- 5 client id
	""	        							-- 6 msg body
}
local function _gen_req(topics)
	local buf = {
		0,		-- 1 topic count
		""		-- 2...n topic
	}
   	buf[1] = util.set_byte4(#topics);
    for _, topic in ipairs(topics) do
    	buf[#buf + 1] = util.pack_kfk_string(topic);
    end
	
	_kfk_req[6] = concat(buf);
	_kfk_req[1] = util.set_byte4(8 + strlen(_kfk_req[5]) + strlen(_kfk_req[6]));
end


local function kfk_metadata_req(kfk)
	local metadata_broker_list = kfk.cf.metadata_broker_list;

    local n = #kfk.meta_pending_topic;

	while n > 0 do
        if util.debug then
            ngxlog(DEBUG, "[kafka] start a metadata query");
        end

		_gen_req(kfk.meta_pending_topic);

		for _, meta_broker in ipairs(metadata_broker_list) do
			local body, err = _send_req(kfk, concat(_kfk_req), meta_broker);
			if body then
				_kfk_metadata_handle(kfk, body);
				break;
			end
			ngxlog(ERR, "[kafka] [", meta_broker, "]: metadata query error: ", err);
		end

		local m = #kfk.meta_pending_topic;
		if m > n then
			local tmp = util.new_tab(m - n, 0);
			for i = n + 1, m do
				tmp[#tmp + 1] = kfk.meta_pending_topic[i];
			end
			kfk.meta_pending_topic = tmp;
		end
		n = m - n;
	end

	kfk.meta_pending_topic = {};
end

local function kfk_metadata_pretest(kfk)
	_gen_req(kfk.cf.topics);
	
	for _, meta_broker in ipairs(kfk.cf.metadata_broker_list) do
		local body, err = _send_req(kfk, concat(_kfk_req), meta_broker);
		if body then
			return body;
		end
		ngxlog(ERR, "[kafka] [", meta_broker, "]: metadata query error: ", err);
	end
end

local meta = {
	kfk_metadata_req = kfk_metadata_req,
	kfk_metadata_pretest = kfk_metadata_pretest
}

return meta
