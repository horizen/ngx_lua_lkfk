local util = require "util";

local base = require "lkfk.base";
local topic = require "lkfk.topic";
local broker = require "lkfk.broker";
local const = require "lkfk.const";
local func = require "lkfk.func";

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
    
	for _, node in ipairs(brokers) do
		broker.kfk_broker_update(kfk, node);
        
	end

	for _, tm in ipairs(topic_metas) do
		topic.kfk_topic_metadata_update(kfk, tm);
	end

end


--[[
-- all_topic := if true: retreive all topics&partitions from the broker
--              if false: just retrieve the topics we know about.
-- kfk_topic := all_topic=false && topics is set: only ask for specified topic.
--]]
local function kfk_metadata_req(kfk)
    if kfk.quit() then
        return;
    end

    if util.debug then 
        ngxlog(DEBUG, "start a metadata query");
    end

	local buf = {
		-- 1 topic count
		-- 2...n topic
	}
    	
    local tmp = {};
    local kfk_topics = kfk.meta_query_topic;

    buf[1] = 0;
    for _, kfk_topic in ipairs(kfk_topics) do
        if not tmp[kfk_topic.name] then
            tmp[kfk_topic.name] = true;
            buf[#buf + 1] = base.pack_kfk_string(kfk_topic.name);
        end
    end
    buf[1] = base.set_byte4(#buf - 1);

    kfk.meta_query_topic = {};

	local msgbody = concat(buf, "");

	local kfk_req = {
		base.zero4,							-- 1 req size
		base.set_byte2(const.KFK_METADATA_REQ),			-- 2 api key
		base.zero2,							-- 3 api version
	    base.zero4,	                        -- 4 corrlation id
		base.pack_kfk_string(kfk.cf.client_id),	-- 5 client id
		msgbody	        					-- 6 msg body
	}
	local size = 8 + strlen(kfk_req[5]) + strlen(kfk_req[6]);
	kfk_req[1] = base.set_byte4(size);
	
    local meta_brokers = kfk.cf.metadata_broker_list;
    local i = 1;
    if #meta_brokers > 1 then
        i = math.random(1, #meta_brokers);
    end
    local hp = util.split(meta_brokers[i], ":");

    local tcp = kfk.tcp;
    tcp:settimeout(3000);

    local ok, err = tcp:connect(hp[1], tonumber(hp[2]));
    if not ok then
        ngxlog(ERR, "metadata query error: ", err);
        return;
    end

    local bytes, err = tcp:send(concat(kfk_req, ""));
    if not bytes then
        ngxlog(ERR, "metadata query error: ", err);
        return;
    end

    local body, err = tcp:receive(20);
    if not body then
        ngxlog(ERR, "metadata query error: ", err);
        return;
    end

    local size = base.get_byte4(body, 1);
    local size = size - strlen(body) + 4;
    if size > 0 then
        local data, err = tcp:receive(size);
        if not data then
            ngxlog(ERR, "metadata query error: ", err);
            return;
        end

        body = body .. data;
    end

    _kfk_metadata_handle(kfk, body);    
    
end

local meta = {
	kfk_metadata_req = kfk_metadata_req
}

return meta
