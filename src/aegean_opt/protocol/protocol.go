package protocol

const (
	MessageTypeInLogRaft                = "inlog_raft"
	MessageTypeInLogRequest             = "inlog_request"
	MessageTypeInLogBatchFormed         = "inlog_batch_formed"
	MessageTypeScheduledRequest         = "scheduled_request"
	MessageTypeVerificationWindowClosed = "verification_window_closed"
	MessageTypeVerificationWindowReplay = "verification_window_replay"
)

const (
	FieldType        = "type"
	FieldSeqNum      = "seq_num"
	FieldRequestIdx  = "request_index"
	FieldRequestID   = "request_id"
	FieldRequest     = "request"
	FieldNDSeed      = "nd_seed"
	FieldNDTimestamp = "nd_timestamp"
	FieldCount       = "count"
	FieldNodeID      = "node_id"
	FieldWorker      = "assigned_worker"
	FieldDependsOn   = "depends_on"
	FieldReadKeys    = "read_keys"
	FieldWriteKeys   = "write_keys"
	FieldRequests    = "requests"
	FieldSchedule    = "scheduled_requests"
)

func NodeID(seqNum int, requestIndex int) string {
	return itoa(seqNum) + ":" + itoa(requestIndex)
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
