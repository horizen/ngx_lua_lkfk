local function default_partitioner(key, part_cnt)
	return ngx.crc32_short(key) % part_cnt;
end


local level = {
	"error",
	"notice",
	"info",
	"debug",
	
	ERR = 1,
	NOTICE = 2,
	INFO = 3,
	DEBUG = 4
}
local function kfk_failed_handle(kfk, msg)
    kfk.fp:write(ngx.localtime(), "\t", msg.level, "\t", msg.topic, 
                "\t", msg.key, "\t", msg.str, "\n");
end

local default_conf = {
	failed_cb = kfk_failed_handle,
	level = level.INFO,
	client_id = "lkfk",
	backlog = "kfk.log",
	metadata_broker_list = {"algo089.sf.sad.sh.sinanode.com:9996","algo090.sf.sad.sh.sinanode.com:9997"},
	topics = {"VIEW", "CLICK", "DSP_IMPRESS", "SINA_IMPRESS", "NETWORK_IMPRESS", "TEST"},
	request_required_acks = 1,
	request_timeout_ms	= 5000,
	message_timeout_ms  = 5000,
	partitioner = default_partitioner,
	message_send_max_retries = 2,
	retry_backoff_ms = 100,
	metadata_refresh_interval_ms = 60000,
	queue_buffering_max_ms	= 2000,
	queue_buffering_max_messages = 50000,
	batch_num_messages = 1000,

    conn_retry_limit = 3,
    conn_retry_timeout = 60,

    --for statistics
	kfk_status = true

	--[[not support yet
	compression_codec = "none",
	message_max_bytes = 100 * 1024,
	--]]
}


local conf = {
	level = level,
	default_conf = default_conf
}

return conf;
