local crc32 = ngx.crc32_short
local localtime = ngx.localtime

local function default_partitioner(key, part_cnt)
    if part_cnt == 1 then
        return 0;
    end
	return crc32(key) % part_cnt;
end

local function kfk_failed_handle(kfk, msg)
    return kfk.fp[msg.topic]:write(localtime(), "\t", msg.topic, 
                "\t", msg.key, "\t", msg.str, "\n");
end

local default_conf = {
	-- msg send failed callback，this is use for backup
	failed_cb = kfk_failed_handle,
	-- backup path for failed msg
	backpath = ngx.config.prefix() .. "/backup/",
	
	-- client id
	client_id = "lkfk",
	
	-- metadata broker list, recommend at least two node
	metadata_broker_list = {"host1:port1,host2:port2"},

	-- topic for kafka
	topics = {},

	request_required_acks = 1,
	-- timeout for kafka
	request_timeout_ms	= 5000,
	-- msg timeout, the different from request_timeout_ms is msg timeout add network transfer time
	message_timeout_ms  = 5000,
	-- partitioner function
	partitioner = default_partitioner,
	-- max number try to send failure msg
	message_send_max_retries = 2,
	
	retry_backoff_ms = 100,

	metadata_refresh_interval_ms = 60000,
	queue_buffering_max_ms	= 2000,
	queue_buffering_max_messages = 50000,
	batch_num_messages = 500,
	
    conn_retry_limit = 3,
    -- connect retry 
    conn_retry_timeout = 60,

    --for statistics
	kfk_status = true

	--[[not support yet
	compression_codec = "none",
	--]]
}


local conf = {
	default_conf = default_conf
}

return conf;
