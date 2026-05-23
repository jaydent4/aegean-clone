package exec

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"aegean/common"
)

const nestedTimingMetadataPrefix = "_nested_"
const nestedDispatchSendUnixNanoKey = "_nested_dispatch_send_unix_nano"
const nestedDispatchSourceKey = "_nested_dispatch_source"
const nestedChildShimFirstReceiveUnixNanoKey = "_nested_child_shim_first_receive_unix_nano"
const nestedChildShimQuorumUnixNanoKey = "_nested_child_shim_quorum_unix_nano"
const nestedChildBatcherReceiveUnixNanoKey = "_nested_child_batcher_receive_unix_nano"
const nestedChildBatcherFlushUnixNanoKey = "_nested_child_batcher_flush_unix_nano"
const nestedBackendCommitUnixNanoKey = "_nested_backend_commit_unix_nano"
const nestedBackendNodeKey = "_nested_backend_node"
const nestedBackendSeqNumKey = "_nested_backend_seq_num"
const nestedBackendBatchOutputCountKey = "_nested_backend_batch_output_count"
const nestedParentShimReceiveUnixNanoKey = "_nested_parent_shim_receive_unix_nano"
const nestedParentQuorumUnixNanoKey = "_nested_parent_quorum_unix_nano"
const nestedParentExecHandleUnixNanoKey = "_nested_parent_exec_handle_unix_nano"
const nestedParentEOProposeUnixNanoKey = "_nested_parent_eo_propose_unix_nano"
const nestedParentEOBufferUnixNanoKey = "_nested_parent_eo_buffer_unix_nano"
const nestedChildShimHandleStartUnixNanoKey = "_nested_child_shim_handle_start_unix_nano"
const nestedChildShimEnqueueUnixNanoKey = "_nested_child_shim_enqueue_unix_nano"
const nestedChildShimSendStartUnixNanoKey = "_nested_child_shim_send_start_unix_nano"
const nestedTraceEnabledKey = "_nested_trace_enabled"
const nestedTimingLogRunConfigKey = "exec_nested_timing_log"

type nestedResponseTimingStats struct {
	count   int
	byChild map[string]*nestedChildTimingStats
}

type nestedChildTimingStats struct {
	count                    int
	dispatchToBuffer         time.Duration
	backendCommitToBuffer    time.Duration
	parentShimToBuffer       time.Duration
	parentQuorumToBuffer     time.Duration
	parentQuorumToHandle     time.Duration
	parentHandleToEOPropose  time.Duration
	parentEOProposeToBuffer  time.Duration
	childShimHandleToBuffer  time.Duration
	childShimEnqueueToBuffer time.Duration
	childShimSendToBuffer    time.Duration
	childShimSendToParent    time.Duration
	dispatchToChildShim      time.Duration
	childShimQuorumWait      time.Duration
	childShimToBatcher       time.Duration
	childBatcherQueue        time.Duration
	childBatcherToExecute    time.Duration
	maxDispatchToBuffer      time.Duration
	maxBackendCommitToBuffer time.Duration
	maxParentShimToBuffer    time.Duration
	maxParentQuorumToBuffer  time.Duration
	maxParentQuorumToHandle  time.Duration
	maxParentHandleToEOProp  time.Duration
	maxParentEOPropToBuffer  time.Duration
	maxChildShimHandleToBuf  time.Duration
	maxChildShimEnqueueToBuf time.Duration
	maxChildShimSendToBuffer time.Duration
	maxChildShimSendToParent time.Duration
	maxDispatchToChildShim   time.Duration
	maxChildShimQuorumWait   time.Duration
	maxChildShimToBatcher    time.Duration
	maxChildBatcherQueue     time.Duration
	maxChildBatcherToExecute time.Duration
	missingDispatchTimestamp int
	missingBackendTimestamp  int
	missingParentShimStamp   int
	missingParentQuorumStamp int
	missingParentEOStamp     int
	missingChildReturnStamp  int
	missingChildRequestStamp int
	negativeDispatchDuration int
	negativeBackendDuration  int
	negativeParentDuration   int
	negativeParentEODuration int
	negativeChildDuration    int
}

type nestedResponseTimingSnapshot struct {
	count   int
	byChild map[string]nestedChildTimingStats
}

func newNestedResponseTimingStats() *nestedResponseTimingStats {
	return &nestedResponseTimingStats{
		byChild: make(map[string]*nestedChildTimingStats),
	}
}

func (e *Exec) recordNestedResponseTiming(parentRequestID string, payload map[string]any) {
	if !nestedTraceEnabled(payload) {
		return
	}
	if payload == nil {
		return
	}
	seqAny, ok := e.GetRequestContextValue(parentRequestID, requestBatchSeqContextKey)
	if !ok {
		return
	}
	seqNum, ok := nestedInt(seqAny)
	if !ok || seqNum <= 0 {
		return
	}

	now := time.Now()
	child := nestedResponseChild(payload)

	e.nestedTimingMu.Lock()
	defer e.nestedTimingMu.Unlock()
	stats := e.nestedResponseTimings[seqNum]
	if stats == nil {
		stats = newNestedResponseTimingStats()
		e.nestedResponseTimings[seqNum] = stats
	}
	stats.count++
	childStats := stats.byChild[child]
	if childStats == nil {
		childStats = &nestedChildTimingStats{}
		stats.byChild[child] = childStats
	}
	childStats.count++

	if dispatchUnixNano, ok := nestedInt64(payload[nestedDispatchSendUnixNanoKey]); ok && dispatchUnixNano > 0 {
		duration := now.Sub(time.Unix(0, dispatchUnixNano))
		if duration >= 0 {
			childStats.dispatchToBuffer += duration
			if duration > childStats.maxDispatchToBuffer {
				childStats.maxDispatchToBuffer = duration
			}
		} else {
			childStats.negativeDispatchDuration++
		}
	} else {
		childStats.missingDispatchTimestamp++
	}

	if backendUnixNano, ok := nestedInt64(payload[nestedBackendCommitUnixNanoKey]); ok && backendUnixNano > 0 {
		duration := now.Sub(time.Unix(0, backendUnixNano))
		if duration >= 0 {
			childStats.backendCommitToBuffer += duration
			if duration > childStats.maxBackendCommitToBuffer {
				childStats.maxBackendCommitToBuffer = duration
			}
		} else {
			childStats.negativeBackendDuration++
		}
	} else {
		childStats.missingBackendTimestamp++
	}

	if parentShimUnixNano, ok := nestedInt64(payload[nestedParentShimReceiveUnixNanoKey]); ok && parentShimUnixNano > 0 {
		duration := now.Sub(time.Unix(0, parentShimUnixNano))
		if duration >= 0 {
			childStats.parentShimToBuffer += duration
			if duration > childStats.maxParentShimToBuffer {
				childStats.maxParentShimToBuffer = duration
			}
		} else {
			childStats.negativeParentDuration++
		}
	} else {
		childStats.missingParentShimStamp++
	}

	if parentQuorumUnixNano, ok := nestedInt64(payload[nestedParentQuorumUnixNanoKey]); ok && parentQuorumUnixNano > 0 {
		duration := now.Sub(time.Unix(0, parentQuorumUnixNano))
		if duration >= 0 {
			childStats.parentQuorumToBuffer += duration
			if duration > childStats.maxParentQuorumToBuffer {
				childStats.maxParentQuorumToBuffer = duration
			}
		} else {
			childStats.negativeParentDuration++
		}
	} else {
		childStats.missingParentQuorumStamp++
	}

	bufferTime := now
	if bufferUnixNano, ok := nestedInt64(payload[nestedParentEOBufferUnixNanoKey]); ok && bufferUnixNano > 0 {
		bufferTime = time.Unix(0, bufferUnixNano)
	} else {
		childStats.missingParentEOStamp++
	}
	if parentQuorumUnixNano, ok := nestedInt64(payload[nestedParentQuorumUnixNanoKey]); ok && parentQuorumUnixNano > 0 {
		if handleUnixNano, ok := nestedInt64(payload[nestedParentExecHandleUnixNanoKey]); ok && handleUnixNano > 0 {
			recordNestedDuration(time.Unix(0, handleUnixNano).Sub(time.Unix(0, parentQuorumUnixNano)), &childStats.parentQuorumToHandle, &childStats.maxParentQuorumToHandle, &childStats.negativeParentEODuration)
		} else {
			childStats.missingParentEOStamp++
		}
	}
	if handleUnixNano, ok := nestedInt64(payload[nestedParentExecHandleUnixNanoKey]); ok && handleUnixNano > 0 {
		if proposeUnixNano, ok := nestedInt64(payload[nestedParentEOProposeUnixNanoKey]); ok && proposeUnixNano > 0 {
			recordNestedDuration(time.Unix(0, proposeUnixNano).Sub(time.Unix(0, handleUnixNano)), &childStats.parentHandleToEOPropose, &childStats.maxParentHandleToEOProp, &childStats.negativeParentEODuration)
		} else {
			childStats.missingParentEOStamp++
		}
	}
	if proposeUnixNano, ok := nestedInt64(payload[nestedParentEOProposeUnixNanoKey]); ok && proposeUnixNano > 0 {
		recordNestedDuration(bufferTime.Sub(time.Unix(0, proposeUnixNano)), &childStats.parentEOProposeToBuffer, &childStats.maxParentEOPropToBuffer, &childStats.negativeParentEODuration)
	} else {
		childStats.missingParentEOStamp++
	}
	if childShimHandleUnixNano, ok := nestedInt64(payload[nestedChildShimHandleStartUnixNanoKey]); ok && childShimHandleUnixNano > 0 {
		recordNestedDuration(now.Sub(time.Unix(0, childShimHandleUnixNano)), &childStats.childShimHandleToBuffer, &childStats.maxChildShimHandleToBuf, &childStats.negativeChildDuration)
	} else {
		childStats.missingChildReturnStamp++
	}

	if childShimEnqueueUnixNano, ok := nestedInt64(payload[nestedChildShimEnqueueUnixNanoKey]); ok && childShimEnqueueUnixNano > 0 {
		recordNestedDuration(now.Sub(time.Unix(0, childShimEnqueueUnixNano)), &childStats.childShimEnqueueToBuffer, &childStats.maxChildShimEnqueueToBuf, &childStats.negativeChildDuration)
	} else {
		childStats.missingChildReturnStamp++
	}

	if childShimSendUnixNano, ok := nestedInt64(payload[nestedChildShimSendStartUnixNanoKey]); ok && childShimSendUnixNano > 0 {
		recordNestedDuration(now.Sub(time.Unix(0, childShimSendUnixNano)), &childStats.childShimSendToBuffer, &childStats.maxChildShimSendToBuffer, &childStats.negativeChildDuration)
		if parentShimUnixNano, ok := nestedInt64(payload[nestedParentShimReceiveUnixNanoKey]); ok && parentShimUnixNano > 0 {
			recordNestedDuration(time.Unix(0, parentShimUnixNano).Sub(time.Unix(0, childShimSendUnixNano)), &childStats.childShimSendToParent, &childStats.maxChildShimSendToParent, &childStats.negativeChildDuration)
		}
	} else {
		childStats.missingChildReturnStamp++
	}
}

func (e *Exec) nestedTimingLogsEnabled() bool {
	if e == nil {
		return false
	}
	return common.BoolOrDefault(e.RunConfig, nestedTimingLogRunConfigKey, false) ||
		common.BoolOrDefault(e.RunConfig, execBottleneckProbeRunConfigKey, false)
}

func (e *Exec) takeNestedResponseTimingStats(seqNum int) nestedResponseTimingSnapshot {
	e.nestedTimingMu.Lock()
	defer e.nestedTimingMu.Unlock()
	stats := e.nestedResponseTimings[seqNum]
	if stats == nil {
		return nestedResponseTimingSnapshot{}
	}
	delete(e.nestedResponseTimings, seqNum)

	snapshot := nestedResponseTimingSnapshot{
		count:   stats.count,
		byChild: make(map[string]nestedChildTimingStats, len(stats.byChild)),
	}
	for child, childStats := range stats.byChild {
		if childStats == nil {
			continue
		}
		snapshot.byChild[child] = *childStats
	}
	return snapshot
}

func nestedRequestArrivalTimingStats(parallelBatches [][]map[string]any) nestedResponseTimingSnapshot {
	now := time.Now()
	stats := newNestedResponseTimingStats()
	for _, batch := range parallelBatches {
		for _, request := range batch {
			dispatchUnixNano, ok := nestedInt64(request[nestedDispatchSendUnixNanoKey])
			if !ok || dispatchUnixNano <= 0 {
				continue
			}
			duration := now.Sub(time.Unix(0, dispatchUnixNano))
			if duration < 0 {
				continue
			}
			stats.count++
			child := nestedResponseChild(request)
			childStats := stats.byChild[child]
			if childStats == nil {
				childStats = &nestedChildTimingStats{}
				stats.byChild[child] = childStats
			}
			childStats.count++
			childStats.dispatchToBuffer += duration
			if duration > childStats.maxDispatchToBuffer {
				childStats.maxDispatchToBuffer = duration
			}
			if firstReceiveUnixNano, ok := nestedInt64(request[nestedChildShimFirstReceiveUnixNanoKey]); ok && firstReceiveUnixNano > 0 {
				recordNestedDuration(time.Unix(0, firstReceiveUnixNano).Sub(time.Unix(0, dispatchUnixNano)), &childStats.dispatchToChildShim, &childStats.maxDispatchToChildShim, &childStats.negativeChildDuration)
				if quorumUnixNano, ok := nestedInt64(request[nestedChildShimQuorumUnixNanoKey]); ok && quorumUnixNano > 0 {
					recordNestedDuration(time.Unix(0, quorumUnixNano).Sub(time.Unix(0, firstReceiveUnixNano)), &childStats.childShimQuorumWait, &childStats.maxChildShimQuorumWait, &childStats.negativeChildDuration)
					if batcherReceiveUnixNano, ok := nestedInt64(request[nestedChildBatcherReceiveUnixNanoKey]); ok && batcherReceiveUnixNano > 0 {
						recordNestedDuration(time.Unix(0, batcherReceiveUnixNano).Sub(time.Unix(0, quorumUnixNano)), &childStats.childShimToBatcher, &childStats.maxChildShimToBatcher, &childStats.negativeChildDuration)
						if batcherFlushUnixNano, ok := nestedInt64(request[nestedChildBatcherFlushUnixNanoKey]); ok && batcherFlushUnixNano > 0 {
							recordNestedDuration(time.Unix(0, batcherFlushUnixNano).Sub(time.Unix(0, batcherReceiveUnixNano)), &childStats.childBatcherQueue, &childStats.maxChildBatcherQueue, &childStats.negativeChildDuration)
							recordNestedDuration(now.Sub(time.Unix(0, batcherFlushUnixNano)), &childStats.childBatcherToExecute, &childStats.maxChildBatcherToExecute, &childStats.negativeChildDuration)
						} else {
							childStats.missingChildRequestStamp++
						}
					} else {
						childStats.missingChildRequestStamp++
					}
				} else {
					childStats.missingChildRequestStamp++
				}
			} else {
				childStats.missingChildRequestStamp++
			}
		}
	}
	if stats.count == 0 {
		return nestedResponseTimingSnapshot{}
	}
	snapshot := nestedResponseTimingSnapshot{
		count:   stats.count,
		byChild: make(map[string]nestedChildTimingStats, len(stats.byChild)),
	}
	for child, childStats := range stats.byChild {
		if childStats == nil {
			continue
		}
		snapshot.byChild[child] = *childStats
	}
	return snapshot
}

func recordNestedDuration(duration time.Duration, total *time.Duration, max *time.Duration, negative *int) {
	if duration < 0 {
		*negative = *negative + 1
		return
	}
	*total += duration
	if duration > *max {
		*max = duration
	}
}

func (s nestedResponseTimingSnapshot) child(name string) nestedChildTimingStats {
	if s.byChild == nil {
		return nestedChildTimingStats{}
	}
	return s.byChild[name]
}

func copyNestedTimingMetadata(dst map[string]any, src map[string]any) bool {
	if dst == nil || src == nil {
		return false
	}
	copied := false
	for key, value := range src {
		if strings.HasPrefix(key, nestedTimingMetadataPrefix) {
			dst[key] = value
			copied = true
		}
	}
	return copied
}

func addNestedBackendCommitMetadata(dst map[string]any, nodeName string, seqNum int, outputCount int) {
	if dst == nil {
		return
	}
	dst[nestedBackendCommitUnixNanoKey] = time.Now().UnixNano()
	dst[nestedBackendNodeKey] = nodeName
	dst[nestedBackendSeqNumKey] = seqNum
	dst[nestedBackendBatchOutputCountKey] = outputCount
}

func nestedTraceEnabled(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	enabled, _ := payload[nestedTraceEnabledKey].(bool)
	return enabled
}

func nestedResponseChild(payload map[string]any) string {
	requestID := fmt.Sprint(payload["request_id"])
	switch {
	case strings.HasSuffix(requestID, "/geo"):
		return "geo"
	case strings.HasSuffix(requestID, "/rate"):
		return "rate"
	case strings.HasSuffix(requestID, "/reservation"):
		return "reservation"
	case strings.HasSuffix(requestID, "/profile"):
		return "profile"
	}
	if idx := strings.LastIndex(requestID, "/"); idx >= 0 && idx+1 < len(requestID) {
		return requestID[idx+1:]
	}
	return "unknown"
}

func nestedInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		return int(parsed), true
	default:
		return 0, false
	}
}

func nestedInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case float64:
		return int64(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		if err == nil {
			return parsed, true
		}
		asFloat, err := strconv.ParseFloat(typed.String(), 64)
		if err != nil {
			return 0, false
		}
		return int64(asFloat), true
	default:
		return 0, false
	}
}
