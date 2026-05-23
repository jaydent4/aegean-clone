package exec

import (
	"log"
	"sort"
	"time"
)

const (
	nestedEOResponseTimingLogInterval = time.Second
	nestedEOResponseTimingAllChild    = "all"
)

type nestedEOResponseTimingStats struct {
	windowStart time.Time
	count       int
	byChild     map[string]*nestedEOResponseChildTimingStats
}

type nestedEOResponseChildTimingStats struct {
	count                  int
	childSendToParentEO    time.Duration
	childSendToBuffer      time.Duration
	receiveToPropose       time.Duration
	proposeToBatcher       time.Duration
	batchWait              time.Duration
	flushToRaftEnqueue     time.Duration
	raftQueue              time.Duration
	raftToLearn            time.Duration
	learnQueue             time.Duration
	learnWorkerToEOLearn   time.Duration
	eoLearnToProcess       time.Duration
	processToCommit        time.Duration
	commitToBuffer         time.Duration
	receiveToBuffer        time.Duration
	maxChildSendToParentEO time.Duration
	maxChildSendToBuffer   time.Duration
	maxBatchWait           time.Duration
	maxRaftToLearn         time.Duration
	maxReceiveToBuffer     time.Duration
	missingTimestamps      int
	negativeDurations      int
}

type nestedEOResponseTimingSnapshot struct {
	windowStart time.Time
	windowEnd   time.Time
	count       int
	byChild     map[string]nestedEOResponseChildTimingStats
}

func newNestedEOResponseTimingStats(windowStart time.Time) *nestedEOResponseTimingStats {
	return &nestedEOResponseTimingStats{
		windowStart: windowStart,
		byChild:     make(map[string]*nestedEOResponseChildTimingStats),
	}
}

func (e *Exec) stampParentEOResponseReceive(payload map[string]any, at time.Time) {
	if !e.nestedTimingLogsEnabled() || !nestedTraceEnabled(payload) {
		return
	}
	payload[nestedParentEOResponseReceiveUnixNanoKey] = at.UnixNano()
}

func (e *Exec) stampParentEOResponseBuffer(payload map[string]any, at time.Time) {
	if !e.nestedTimingLogsEnabled() || !nestedTraceEnabled(payload) {
		return
	}
	payload[nestedParentEOResponseBufferUnixNanoKey] = at.UnixNano()
}

func (e *Exec) recordNestedEOResponseTiming(payload map[string]any, bufferTime time.Time) {
	if !e.nestedTimingLogsEnabled() || !nestedTraceEnabled(payload) {
		return
	}

	var snapshot nestedEOResponseTimingSnapshot
	shouldLog := false

	e.nestedTimingMu.Lock()
	stats := e.nestedEOResponseStats
	if stats == nil {
		stats = newNestedEOResponseTimingStats(bufferTime)
		e.nestedEOResponseStats = stats
	}
	stats.record(payload)
	if bufferTime.Sub(stats.windowStart) >= nestedEOResponseTimingLogInterval {
		snapshot = stats.snapshot(bufferTime)
		e.nestedEOResponseStats = newNestedEOResponseTimingStats(bufferTime)
		shouldLog = true
	}
	e.nestedTimingMu.Unlock()

	if shouldLog && snapshot.count > 0 {
		e.logNestedEOResponseTiming(snapshot)
	}
}

func (s *nestedEOResponseTimingStats) record(payload map[string]any) {
	s.count++
	child := nestedResponseChild(payload)
	s.recordChild(nestedEOResponseTimingAllChild, payload)
	if child != nestedEOResponseTimingAllChild {
		s.recordChild(child, payload)
	}
}

func (s *nestedEOResponseTimingStats) recordChild(child string, payload map[string]any) {
	childStats := s.byChild[child]
	if childStats == nil {
		childStats = &nestedEOResponseChildTimingStats{}
		s.byChild[child] = childStats
	}
	childStats.count++
	childStats.recordDuration(payload, nestedChildShimSendStartUnixNanoKey, nestedParentEOResponseReceiveUnixNanoKey, &childStats.childSendToParentEO, &childStats.maxChildSendToParentEO)
	childStats.recordDuration(payload, nestedChildShimSendStartUnixNanoKey, nestedParentEOResponseBufferUnixNanoKey, &childStats.childSendToBuffer, &childStats.maxChildSendToBuffer)
	childStats.recordDuration(payload, nestedParentEOResponseReceiveUnixNanoKey, nestedParentEOResponseProposeEnterUnixNanoKey, &childStats.receiveToPropose, nil)
	childStats.recordDuration(payload, nestedParentEOResponseProposeEnterUnixNanoKey, nestedParentEOResponseBatcherRecvUnixNanoKey, &childStats.proposeToBatcher, nil)
	childStats.recordDuration(payload, nestedParentEOResponseBatcherRecvUnixNanoKey, nestedParentEOResponseBatchFlushUnixNanoKey, &childStats.batchWait, &childStats.maxBatchWait)
	childStats.recordDuration(payload, nestedParentEOResponseBatchFlushUnixNanoKey, nestedParentEOResponseRaftEnqueueUnixNanoKey, &childStats.flushToRaftEnqueue, nil)
	childStats.recordDuration(payload, nestedParentEOResponseRaftEnqueueUnixNanoKey, nestedParentEOResponseRaftLoopUnixNanoKey, &childStats.raftQueue, nil)
	childStats.recordDuration(payload, nestedParentEOResponseRaftLoopUnixNanoKey, nestedParentEOResponseRaftLearnUnixNanoKey, &childStats.raftToLearn, &childStats.maxRaftToLearn)
	childStats.recordDuration(payload, nestedParentEOResponseLearnQueueUnixNanoKey, nestedParentEOResponseLearnWorkerUnixNanoKey, &childStats.learnQueue, nil)
	childStats.recordDuration(payload, nestedParentEOResponseLearnWorkerUnixNanoKey, nestedParentEOResponseEOLearnUnixNanoKey, &childStats.learnWorkerToEOLearn, nil)
	childStats.recordDuration(payload, nestedParentEOResponseEOLearnUnixNanoKey, nestedParentEOResponseEOProcessUnixNanoKey, &childStats.eoLearnToProcess, nil)
	childStats.recordDuration(payload, nestedParentEOResponseEOProcessUnixNanoKey, nestedParentEOResponseCommitCallUnixNanoKey, &childStats.processToCommit, nil)
	childStats.recordDuration(payload, nestedParentEOResponseCommitCallUnixNanoKey, nestedParentEOResponseBufferUnixNanoKey, &childStats.commitToBuffer, nil)
	childStats.recordDuration(payload, nestedParentEOResponseReceiveUnixNanoKey, nestedParentEOResponseBufferUnixNanoKey, &childStats.receiveToBuffer, &childStats.maxReceiveToBuffer)
}

func (s *nestedEOResponseChildTimingStats) recordDuration(payload map[string]any, startKey string, endKey string, total *time.Duration, max *time.Duration) {
	startUnixNano, startOK := nestedInt64(payload[startKey])
	endUnixNano, endOK := nestedInt64(payload[endKey])
	if !startOK || !endOK || startUnixNano <= 0 || endUnixNano <= 0 {
		s.missingTimestamps++
		return
	}
	duration := time.Unix(0, endUnixNano).Sub(time.Unix(0, startUnixNano))
	if duration < 0 {
		s.negativeDurations++
		return
	}
	*total += duration
	if max != nil && duration > *max {
		*max = duration
	}
}

func (s *nestedEOResponseTimingStats) snapshot(windowEnd time.Time) nestedEOResponseTimingSnapshot {
	out := nestedEOResponseTimingSnapshot{
		windowStart: s.windowStart,
		windowEnd:   windowEnd,
		count:       s.count,
		byChild:     make(map[string]nestedEOResponseChildTimingStats, len(s.byChild)),
	}
	for child, stats := range s.byChild {
		if stats == nil {
			continue
		}
		out.byChild[child] = *stats
	}
	return out
}

func (e *Exec) logNestedEOResponseTiming(snapshot nestedEOResponseTimingSnapshot) {
	children := make([]string, 0, len(snapshot.byChild))
	if _, ok := snapshot.byChild[nestedEOResponseTimingAllChild]; ok {
		children = append(children, nestedEOResponseTimingAllChild)
	}
	for child := range snapshot.byChild {
		if child == nestedEOResponseTimingAllChild {
			continue
		}
		children = append(children, child)
	}
	if len(children) > 1 {
		sort.Strings(children[1:])
	}

	window := snapshot.windowEnd.Sub(snapshot.windowStart)
	for _, child := range children {
		stats := snapshot.byChild[child]
		if stats.count == 0 {
			continue
		}
		log.Printf(
			"%s: nested_eo_response_timing window_ms=%d child=%s count=%d child_send_to_parent_eo_us=%d child_send_to_buffer_us=%d receive_to_propose_us=%d propose_to_batcher_us=%d batch_wait_us=%d flush_to_raft_enqueue_us=%d raft_queue_us=%d raft_to_learn_us=%d learn_queue_us=%d learn_worker_to_eo_learn_us=%d eo_learn_to_process_us=%d process_to_commit_us=%d commit_to_buffer_us=%d receive_to_buffer_us=%d max_child_send_to_parent_eo_us=%d max_child_send_to_buffer_us=%d max_batch_wait_us=%d max_raft_to_learn_us=%d max_receive_to_buffer_us=%d missing_timestamps=%d negative_durations=%d",
			e.Name,
			window.Milliseconds(),
			child,
			stats.count,
			nestedAvgMicros(stats.childSendToParentEO, stats.count),
			nestedAvgMicros(stats.childSendToBuffer, stats.count),
			nestedAvgMicros(stats.receiveToPropose, stats.count),
			nestedAvgMicros(stats.proposeToBatcher, stats.count),
			nestedAvgMicros(stats.batchWait, stats.count),
			nestedAvgMicros(stats.flushToRaftEnqueue, stats.count),
			nestedAvgMicros(stats.raftQueue, stats.count),
			nestedAvgMicros(stats.raftToLearn, stats.count),
			nestedAvgMicros(stats.learnQueue, stats.count),
			nestedAvgMicros(stats.learnWorkerToEOLearn, stats.count),
			nestedAvgMicros(stats.eoLearnToProcess, stats.count),
			nestedAvgMicros(stats.processToCommit, stats.count),
			nestedAvgMicros(stats.commitToBuffer, stats.count),
			nestedAvgMicros(stats.receiveToBuffer, stats.count),
			stats.maxChildSendToParentEO.Microseconds(),
			stats.maxChildSendToBuffer.Microseconds(),
			stats.maxBatchWait.Microseconds(),
			stats.maxRaftToLearn.Microseconds(),
			stats.maxReceiveToBuffer.Microseconds(),
			stats.missingTimestamps,
			stats.negativeDurations,
		)
	}
}

func nestedAvgMicros(total time.Duration, count int) int64 {
	if count <= 0 {
		return 0
	}
	return total.Microseconds() / int64(count)
}
