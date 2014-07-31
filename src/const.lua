local KFK_LAST_ERR = 16;

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
	
	--error code [kfk]
	KFK_RSP_NOERR = 0,
	KFK_RSP_ERR_INVALID_MESSAGE = 2,
	KFK_RSP_ERR_UNKNOWN_TOPIC_OR_PART = 3,
	KFK_RSP_ERR_INVALID_MESSAGE_SIZE = 4,
	KFK_RSP_ERR_LEADER_NOT_AVAILABLE = 5,
	KFK_RSP_ERR_NOT_LEADER_FOR_PARTITION = 6,
	KFK_RSP_ERR_REQUEST_TIMED_OUT = 7,
	KFK_RSP_ERR_BROKER_NOT_AVAILABLE = 8,

	--error code []
	KFK_RSP_ERR_MESSAGE_TIMED_OUT = KFK_LAST_ERR + 1,
	KFK_RSP_ERR_SOCKET = KFK_LAST_ERR + 2,
	KFK_RSP_ERR_BAD_MSG = KFK_LAST_ERR + 3,
	KFK_RSP_ERR_DESTROY = KFK_LAST_ERR + 4,
    KFK_RSP_ERR_BROKER_DOWN = KFK_LAST_ERR + 5,
    
    KFK_BROKER_NOT_VALID = KFK_LAST_ERR + 6,
    KFK_NGINX_DOWN = KFK_LAST_ERR + 7
}


local errstr = {}
errstr[-1] = "-1: Unknown"
errstr[1] = "1: OffsetOutOfRange"
errstr[2] = "2: InvalidMessage"
errstr[3] = "3: UnknownTopicOrPartition"
errstr[4] = "4: InvalidMessageSize"
errstr[5] = "5: LeaderNotAvailable"
errstr[6] = "6: NotLeaderForPartitionn"
errstr[7] = "7: RequestTimedOut"
errstr[8] = "8: BrokerNotAvailable"
errstr[9] = "9: Unused"
errstr[10] = "10: MessageSizeTooLarge"
errstr[11] = "11: StaleControllerEpochCode"
errstr[12] = "12: OffsetMetadataTooLargeCode"
errstr[14] = "14: OffsetsLoadInProgressCode"
errstr[15] = "15: ConsumerCoordinatorNotAvailableCode"
errstr[16] = "16: NotCoordinatorForConsumerCode"


errstr[const.KFK_RSP_ERR_MESSAGE_TIMED_OUT] = "MessageTimedOut"
errstr[const.KFK_RSP_ERR_SOCKET] = "SocketError"
errstr[const.KFK_RSP_ERR_BAD_MSG] = "BadMessageRsp"
errstr[const.KFK_RSP_ERR_DESTROY] = "BrokerDestroy"
errstr[const.KFK_RSP_ERR_BROKER_DOWN] = "BrokerDown"
errstr[const.KFK_BROKER_NOT_VALID] = "BrokerNotValid"
errstr[const.KFK_NGINX_DOWN] = "NginxDown"

const.errstr = errstr
return const;
