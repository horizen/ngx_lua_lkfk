local const = {
	KFK_PRODUCE = 0,
	
	KFK_TOPIC_UNKNOWN = 0,
	KFK_TOPIC_INIT = 1,
	KFK_TOPIC_EXIST = 2,

	KFK_PARTITION_UA = -1,
	KFK_PARTITION_LEADER_UA = -1,
	
	KFK_BROKER_ID_UA = -1,
	KFK_BROKER_RECONNECT = 2,
	KFK_BROKER_DOWN = 0,
	KFK_BROKER_UP = 1,
	
	KFK_METADATA_REQ = 3,
	KFK_PRODUCE_REQ = 0,
	
	KFK_TOPIC_F_LEADER_QUERY = 1,
	
	--error code
	KFK_RSP_NOERR = 0,
	KFK_RSP_ERR_UNKNOWN_TOPIC_OR_PART = 3,
	KFK_RSP_ERR_LEADER_NOT_AVAILABLE = 5,
	KFK_RSP_ERR_NOT_LEADER_FOR_PARTITION = 6,
	KFK_RSP_ERR_REQUEST_TIMED_OUT = 7,
	KFK_RSP_ERR_BROKER_NOT_AVAILABLE = 8,
	KFK_RSP_ERR_REPLICA_NOT_AVAILABLE = 9,
	
	KFK_RSP_ERR_MESSAGE_TIMED_OUT = 13,
	KFK_RSP_ERR_TRANSPORT = 14,
	KFK_RSP_ERR_BAD_MSG = 15,
	KFK_RSP_ERR_DESTROY = 16,
    KFK_RSP_ERR_BROKER_DOWN = 17,
    
    KFK_BROKER_NOT_VALID = 18,
    KFK_NGINX_DOWN = 19
}
local errstr = {}
errstr[3] = "unknown topic or partition"
errstr[5] = "leader not available"
errstr[6] = "not leader for patition"
errstr[7] = "request timed out"
errstr[8] = "broker not available"
errstr[9] = "replica not available"
errstr[13] = "message timed out"
errstr[14] = "socket error"
errstr[15] = "bad msg"
errstr[16] = "broker destroy"
errstr[17] = "broker down"
errstr[18] = "broker not valid"
errstr[19] = "nginx down"

const.errstr = errstr
return const;
