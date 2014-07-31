local function default_partitioner(key, part_cnt)
	return ngx.crc32_short(key) % part_cnt;
end


local log = {
	STDERR = 1,
	EMERG = 2,
	ALERT = 3,
	CRIT = 4,
	ERR = 5,
	WARN = 6,
	NOTICE = 7,
	INFO = 8,
	DEBUG = 9
}

local function kfk_failed_handle(kfk, msg)
    return kfk.fp[msg.topic]:write(ngx.localtime(), "\t", msg.topic, 
                "\t", msg.key, "\t", msg.str, "\n");
end

local default_conf = {
	-- msg send failed callback，this is use for backup
	failed_cb = kfk_failed_handle,
	-- backup log file for failed msg
	backpath = "/usr/home/yaowei2/work/nginx/backup/",
	
	-- log level
	log = log.INFO,
	-- client id
	client_id = "lkfk",
	
	-- metadata broker list
	metadata_broker_list = {"algo089.sf.sad.sh.sinanode.com:9996","algo090.sf.sad.sh.sinanode.com:9997"},

	-- topic for kafka
	topics = {"VIEW", "CLICK", "DSP_IMPRESS", "SINA_IMPRESS", "NETWORK_IMPRESS", "TEST"},
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
	batch_num_messages = 1000,
	
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
