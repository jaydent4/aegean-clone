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
const nestedBackendCommitUnixNanoKey = "_nested_backend_commit_unix_nano"
const nestedBackendNodeKey = "_nested_backend_node"
const nestedBackendSeqNumKey = "_nested_backend_seq_num"
const nestedBackendBatchOutputCountKey = "_nested_backend_batch_output_count"
const nestedParentShimReceiveUnixNanoKey = "_nested_parent_shim_receive_unix_nano"
const nestedParentQuorumUnixNanoKey = "_nested_parent_quorum_unix_nano"
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
	maxDispatchToBuffer      time.Duration
	maxBackendCommitToBuffer time.Duration
	maxParentShimToBuffer    time.Duration
	maxParentQuorumToBuffer  time.Duration
	missingDispatchTimestamp int
	missingBackendTimestamp  int
	missingParentShimStamp   int
	missingParentQuorumStamp int
	negativeDispatchDuration int
	negativeBackendDuration  int
	negativeParentDuration   int
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
}

func (e *Exec) nestedTimingLogsEnabled() bool {
	if e == nil {
		return false
	}
	return common.BoolOrDefault(e.RunConfig, nestedTimingLogRunConfigKey, false)
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
