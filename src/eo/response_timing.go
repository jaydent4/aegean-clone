package eo

import "time"

const nestedTimingTraceEnabledKey = "_nested_trace_enabled"

const (
	nestedParentEOResponseProposeEnterUnixNanoKey  = "_nested_parent_eo_response_propose_enter_unix_nano"
	nestedParentEOResponseBatcherRecvUnixNanoKey   = "_nested_parent_eo_response_batcher_recv_unix_nano"
	nestedParentEOResponseBatchFlushUnixNanoKey    = "_nested_parent_eo_response_batch_flush_unix_nano"
	nestedParentEOResponseRaftEnqueueUnixNanoKey   = "_nested_parent_eo_response_raft_enqueue_unix_nano"
	nestedParentEOResponseRaftLoopUnixNanoKey      = "_nested_parent_eo_response_raft_loop_unix_nano"
	nestedParentEOResponseRaftRawDoneUnixNanoKey   = "_nested_parent_eo_response_raft_raw_done_unix_nano"
	nestedParentEOResponseRaftDrainDoneUnixNanoKey = "_nested_parent_eo_response_raft_drain_done_unix_nano"
	nestedParentEOResponseRaftProposalGroupSizeKey = "_nested_parent_eo_response_raft_proposal_group_size"
	nestedParentEOResponseRaftLearnUnixNanoKey     = "_nested_parent_eo_response_raft_learn_unix_nano"
	nestedParentEOResponseLearnQueueUnixNanoKey    = "_nested_parent_eo_response_learn_queue_unix_nano"
	nestedParentEOResponseLearnWorkerUnixNanoKey   = "_nested_parent_eo_response_learn_worker_unix_nano"
	nestedParentEOResponseEOLearnUnixNanoKey       = "_nested_parent_eo_response_eo_learn_unix_nano"
	nestedParentEOResponseEOProcessUnixNanoKey     = "_nested_parent_eo_response_eo_process_unix_nano"
	nestedParentEOResponseCommitCallUnixNanoKey    = "_nested_parent_eo_response_commit_call_unix_nano"
)

func stampPayloadNestedTiming(payload map[string]any, key string, at time.Time) {
	if !eoNestedTraceEnabled(payload) {
		return
	}
	payload[key] = at.UnixNano()
}

func setPayloadNestedTimingInt(payload map[string]any, key string, value int) {
	if !eoNestedTraceEnabled(payload) {
		return
	}
	payload[key] = value
}

func stampResponseProposalNestedTiming(proposal *responseProposal, key string, at time.Time) {
	if proposal == nil {
		return
	}
	stampPayloadNestedTiming(proposal.item.Response, key, at)
}

func stampResponseProposalsNestedTiming(proposals []responseProposal, key string, at time.Time) {
	for i := range proposals {
		stampResponseProposalNestedTiming(&proposals[i], key, at)
	}
}

func stampEntryNestedTiming(entry Entry, key string, at time.Time) {
	if entry.Response != nil {
		stampPayloadNestedTiming(entry.Response, key, at)
	}
	for i := range entry.Batch {
		stampPayloadNestedTiming(entry.Batch[i].Response, key, at)
	}
}

func eoNestedTraceEnabled(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	enabled, _ := payload[nestedTimingTraceEnabledKey].(bool)
	return enabled
}
