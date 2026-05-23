package eo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"aegean/telemetry"
	raft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var errConsensusBoxStopped = errors.New("eo consensus box stopped")

type proposalRequest struct {
	entry    Entry
	result   chan error
	span     trace.Span
	enqueued time.Time
}

type stepRequest struct {
	message  raftpb.Message
	enqueued time.Time
}

type asyncLearnRequest struct {
	slot     uint64
	entry    Entry
	enqueued time.Time
}

type unreachablePeer struct {
	id uint64
}

type sendRequest struct {
	message  raftpb.Message
	enqueued time.Time
}

type pendingAppendSignal struct {
	index     uint64
	requestID string
	span      trace.Span
}

type pendingCommitSignal struct {
	index uint64
	span  trace.Span
}

type drainReadyStats struct {
	readyBatches      int
	entries           int
	committedEntries  int
	messages          int
	selfMessages      int
	sentMessages      int
	unreachablePeers  int
	confChanges       int
	normalEntries     int
	snapshots         int
	hardStates        int
	leaderTransitions int
	storageNanos      int64
	sendEnqueueNanos  int64
	selfStepNanos     int64
	learnDecodeNanos  int64
	learnCloneNanos   int64
	learnEnqueueNanos int64
	learnNanos        int64
	advanceNanos      int64
	readyNanos        int64
	messageNanos      int64
	commitNanos       int64
}

type raftConsensusBox struct {
	name       string
	selfID     uint64
	peerIDs    map[string]uint64
	peers      map[uint64]string
	send       SendRaftFunc
	sendBatch  SendRaftBatchFunc
	learn      LearnFunc
	learnBatch LearnBatchFunc

	leaderID    atomic.Uint64
	learnedSlot uint64

	proposals       chan proposalRequest
	prioritySteps   chan stepRequest
	backgroundSteps chan stepRequest
	sendQueues      map[uint64]chan sendRequest
	sendBatchSize   int
	learnBatchSize  int
	learnQueue      chan asyncLearnRequest
	unreachable     chan unreachablePeer
	spans           map[string]trace.Span
	commitWaitSpans map[string]trace.Span
	appendAckSpans  map[uint64][]pendingAppendSignal
	ackCommitSpans  map[uint64][]pendingCommitSignal
	stopOnce        sync.Once
	stopCh          chan struct{}
	doneCh          chan struct{}
}

const (
	raftProposalQueueSize     = 4096
	raftAsyncSendQueueSize    = 1024
	raftAsyncLearnQueueSize   = 4096
	defaultRaftSendBatchSize  = 64
	defaultRaftLearnBatchSize = 256
	raftLoopTraceWindow       = time.Second
)

type raftLoopTraceWindowStats struct {
	component      string
	nodeName       string
	selfID         uint64
	start          time.Time
	counts         map[string]int64
	durations      map[string]time.Duration
	durationCounts map[string]int64
	maxDurations   map[string]time.Duration
	lastGauges     map[string]int64
	maxGauges      map[string]int64
}

func newRaftLoopTraceWindowStats(component string, nodeName string, selfID uint64) *raftLoopTraceWindowStats {
	w := &raftLoopTraceWindowStats{
		component: component,
		nodeName:  nodeName,
		selfID:    selfID,
	}
	w.reset(time.Now())
	return w
}

func (w *raftLoopTraceWindowStats) reset(start time.Time) {
	w.start = start
	w.counts = make(map[string]int64)
	w.durations = make(map[string]time.Duration)
	w.durationCounts = make(map[string]int64)
	w.maxDurations = make(map[string]time.Duration)
	w.lastGauges = make(map[string]int64)
	w.maxGauges = make(map[string]int64)
}

func (w *raftLoopTraceWindowStats) addCount(name string, value int64) {
	if value == 0 {
		return
	}
	w.counts[name] += value
}

func (w *raftLoopTraceWindowStats) addDuration(name string, duration time.Duration) {
	if duration <= 0 {
		return
	}
	w.durations[name] += duration
	w.durationCounts[name]++
	if duration > w.maxDurations[name] {
		w.maxDurations[name] = duration
	}
}

func (w *raftLoopTraceWindowStats) observeGauge(name string, value int) {
	v := int64(value)
	w.lastGauges[name] = v
	if v > w.maxGauges[name] {
		w.maxGauges[name] = v
	}
}

func (w *raftLoopTraceWindowStats) recordQueues(proposals, prioritySteps, backgroundSteps, unreachable, learn int) {
	w.observeGauge("proposals_queued", proposals)
	w.observeGauge("priority_steps_queued", prioritySteps)
	w.observeGauge("background_steps_queued", backgroundSteps)
	w.observeGauge("unreachable_queued", unreachable)
	w.observeGauge("learn_queued", learn)
}

func (w *raftLoopTraceWindowStats) recordDrain(stats drainReadyStats, duration time.Duration) {
	w.addDuration("drain_ready", duration)
	w.addCount("ready_batches", int64(stats.readyBatches))
	w.addCount("ready_entries", int64(stats.entries))
	w.addCount("ready_committed_entries", int64(stats.committedEntries))
	w.addCount("ready_messages", int64(stats.messages))
	w.addCount("ready_self_messages", int64(stats.selfMessages))
	w.addCount("ready_sent_messages", int64(stats.sentMessages))
	w.addCount("ready_conf_changes", int64(stats.confChanges))
	w.addCount("ready_normal_entries", int64(stats.normalEntries))
	w.addCount("ready_snapshots", int64(stats.snapshots))
	w.addCount("ready_hard_states", int64(stats.hardStates))
	w.addCount("ready_leader_transitions", int64(stats.leaderTransitions))
	w.addDuration("storage", time.Duration(stats.storageNanos))
	w.addDuration("send_enqueue", time.Duration(stats.sendEnqueueNanos))
	w.addDuration("self_step", time.Duration(stats.selfStepNanos))
	w.addDuration("learn_decode", time.Duration(stats.learnDecodeNanos))
	w.addDuration("learn_clone", time.Duration(stats.learnCloneNanos))
	w.addDuration("learn_enqueue", time.Duration(stats.learnEnqueueNanos))
	w.addDuration("learn_committed", time.Duration(stats.learnNanos))
	w.addDuration("advance", time.Duration(stats.advanceNanos))
	w.addDuration("ready_read", time.Duration(stats.readyNanos))
	w.addDuration("ready_messages", time.Duration(stats.messageNanos))
	w.addDuration("ready_commits", time.Duration(stats.commitNanos))
}

func (w *raftLoopTraceWindowStats) recordStepRequest(queueName string, request stepRequest) {
	if !request.enqueued.IsZero() {
		w.addDuration(queueName+"_step_queue_wait", time.Since(request.enqueued))
	}
	w.addCount(queueName+"_step_type_"+raftMessageTypeName(request.message.Type), 1)
}

func (w *raftLoopTraceWindowStats) maybeFlush(leaderID uint64, reason string) {
	if time.Since(w.start) >= raftLoopTraceWindow {
		w.flush(leaderID, reason)
	}
}

func (w *raftLoopTraceWindowStats) flush(leaderID uint64, reason string) {
	if w.counts["iterations"] == 0 {
		w.reset(time.Now())
		return
	}
	now := time.Now()
	elapsed := now.Sub(w.start)
	attrs := []attribute.KeyValue{
		attribute.String("raft.loop.component", w.component),
		attribute.String("node.name", w.nodeName),
		attribute.Int64("raft.node_id", int64(w.selfID)),
		attribute.Int64("raft.leader_id", int64(leaderID)),
		attribute.Bool("raft.loop.is_leader", leaderID == w.selfID && w.selfID != 0),
		attribute.String("raft.loop.flush_reason", reason),
		attribute.Int64("raft.loop.window_us", elapsed.Microseconds()),
		attribute.Float64("raft.loop.iterations_per_sec", float64(w.counts["iterations"])/elapsed.Seconds()),
	}
	for _, key := range sortedInt64MapKeys(w.counts) {
		attrs = append(attrs, attribute.Int64("raft.loop.count."+key, w.counts[key]))
	}
	for _, key := range sortedDurationMapKeys(w.durations) {
		total := w.durations[key]
		count := w.durationCounts[key]
		avg := time.Duration(0)
		if count > 0 {
			avg = total / time.Duration(count)
		}
		attrs = append(attrs,
			attribute.Int64("raft.loop.duration."+key+".count", count),
			attribute.Int64("raft.loop.duration."+key+".total_us", total.Microseconds()),
			attribute.Int64("raft.loop.duration."+key+".avg_us", avg.Microseconds()),
			attribute.Int64("raft.loop.duration."+key+".max_us", w.maxDurations[key].Microseconds()),
		)
	}
	for _, key := range sortedInt64MapKeys(w.lastGauges) {
		attrs = append(attrs,
			attribute.Int64("raft.loop.gauge."+key+".last", w.lastGauges[key]),
			attribute.Int64("raft.loop.gauge."+key+".max", w.maxGauges[key]),
		)
	}

	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		w.component+".raft.loop.window",
		trace.WithTimestamp(w.start),
		trace.WithAttributes(attrs...),
	)
	span.End(trace.WithTimestamp(now))
	w.logWindow(leaderID, elapsed)
	w.reset(now)
}

func (w *raftLoopTraceWindowStats) logWindow(leaderID uint64, elapsed time.Duration) {
	if !eoRaftWindowLogsEnabled() {
		return
	}
	iterations := w.counts["iterations"]
	iterationsPerSec := float64(0)
	if elapsed > 0 {
		iterationsPerSec = float64(iterations) / elapsed.Seconds()
	}
	log.Printf(
		"%s: %s_raft_loop_window window_ms=%d leader=%t iter_per_s=%.1f proposals=%d priority_steps=%d background_steps=%d ready_batches=%d ready_committed=%d ready_messages=%d proposal_queue_wait_us=%d priority_step_queue_wait_us=%d background_step_queue_wait_us=%d handler_us=%d raw_propose_us=%d raw_step_us=%d drain_ready_us=%d ready_read_us=%d ready_messages_us=%d ready_commits_us=%d send_enqueue_us=%d learn_committed_us=%d advance_us=%d proposals_q_max=%d priority_q_max=%d background_q_max=%d send_unreachable_q_max=%d learn_q_max=%d",
		w.nodeName,
		w.component,
		elapsed.Milliseconds(),
		leaderID == w.selfID && w.selfID != 0,
		iterationsPerSec,
		w.counts["proposals"],
		w.counts["priority_steps"],
		w.counts["background_steps"],
		w.counts["ready_batches"],
		w.counts["ready_committed_entries"],
		w.counts["ready_messages"],
		w.durations["proposal_queue_wait"].Microseconds(),
		w.durations["priority_step_queue_wait"].Microseconds(),
		w.durations["background_step_queue_wait"].Microseconds(),
		w.durations["handler"].Microseconds(),
		w.durations["raw_propose"].Microseconds(),
		w.durations["raw_step"].Microseconds(),
		w.durations["drain_ready"].Microseconds(),
		w.durations["ready_read"].Microseconds(),
		w.durations["ready_messages"].Microseconds(),
		w.durations["ready_commits"].Microseconds(),
		w.durations["send_enqueue"].Microseconds(),
		w.durations["learn_committed"].Microseconds(),
		w.durations["advance"].Microseconds(),
		w.maxGauges["proposals_queued"],
		w.maxGauges["priority_steps_queued"],
		w.maxGauges["background_steps_queued"],
		w.maxGauges["unreachable_queued"],
		w.maxGauges["learn_queued"],
	)
}

func sortedInt64MapKeys(values map[string]int64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedDurationMapKeys(values map[string]time.Duration) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type raftLearnWorkerTraceWindowStats struct {
	nodeName       string
	selfID         uint64
	start          time.Time
	counts         map[string]int64
	durations      map[string]time.Duration
	durationCounts map[string]int64
	maxDurations   map[string]time.Duration
	lastGauges     map[string]int64
	maxGauges      map[string]int64
}

func newRaftLearnWorkerTraceWindowStats(nodeName string, selfID uint64) *raftLearnWorkerTraceWindowStats {
	w := &raftLearnWorkerTraceWindowStats{
		nodeName: nodeName,
		selfID:   selfID,
	}
	w.reset(time.Now())
	return w
}

func (w *raftLearnWorkerTraceWindowStats) reset(start time.Time) {
	w.start = start
	w.counts = make(map[string]int64)
	w.durations = make(map[string]time.Duration)
	w.durationCounts = make(map[string]int64)
	w.maxDurations = make(map[string]time.Duration)
	w.lastGauges = make(map[string]int64)
	w.maxGauges = make(map[string]int64)
}

func (w *raftLearnWorkerTraceWindowStats) addCount(name string, value int64) {
	if value == 0 {
		return
	}
	w.counts[name] += value
}

func (w *raftLearnWorkerTraceWindowStats) addDuration(name string, duration time.Duration) {
	if duration <= 0 {
		return
	}
	w.durations[name] += duration
	w.durationCounts[name]++
	if duration > w.maxDurations[name] {
		w.maxDurations[name] = duration
	}
}

func (w *raftLearnWorkerTraceWindowStats) observeGauge(name string, value int) {
	v := int64(value)
	w.lastGauges[name] = v
	if v > w.maxGauges[name] {
		w.maxGauges[name] = v
	}
}

func (w *raftLearnWorkerTraceWindowStats) recordBatch(batch []asyncLearnRequest, queueDepthBeforeDrain int, queueDepthAfterDrain int, callbackDuration time.Duration) {
	w.addCount("batches", 1)
	w.addCount("entries", int64(len(batch)))
	w.addDuration("callback", callbackDuration)
	now := time.Now()
	for _, request := range batch {
		w.addDuration("queue_wait", now.Sub(request.enqueued))
	}
	w.observeGauge("batch_size", len(batch))
	w.observeGauge("queue_depth_before_drain", queueDepthBeforeDrain)
	w.observeGauge("queue_depth_after_drain", queueDepthAfterDrain)
}

func (w *raftLearnWorkerTraceWindowStats) maybeFlush(leaderID uint64, reason string) {
	if time.Since(w.start) >= raftLoopTraceWindow {
		w.flush(leaderID, reason)
	}
}

func (w *raftLearnWorkerTraceWindowStats) flush(leaderID uint64, reason string) {
	if w.counts["batches"] == 0 {
		w.reset(time.Now())
		return
	}
	now := time.Now()
	elapsed := now.Sub(w.start)
	attrs := []attribute.KeyValue{
		attribute.String("raft.loop.component", "eo"),
		attribute.String("node.name", w.nodeName),
		attribute.Int64("raft.node_id", int64(w.selfID)),
		attribute.Int64("raft.leader_id", int64(leaderID)),
		attribute.Bool("raft.loop.is_leader", leaderID == w.selfID && w.selfID != 0),
		attribute.String("eo.raft.learn_worker.flush_reason", reason),
		attribute.Int64("eo.raft.learn_worker.window_us", elapsed.Microseconds()),
	}
	for _, key := range sortedInt64MapKeys(w.counts) {
		attrs = append(attrs, attribute.Int64("eo.raft.learn_worker.count."+key, w.counts[key]))
	}
	for _, key := range sortedDurationMapKeys(w.durations) {
		total := w.durations[key]
		count := w.durationCounts[key]
		avg := time.Duration(0)
		if count > 0 {
			avg = total / time.Duration(count)
		}
		attrs = append(attrs,
			attribute.Int64("eo.raft.learn_worker.duration."+key+".count", count),
			attribute.Int64("eo.raft.learn_worker.duration."+key+".total_us", total.Microseconds()),
			attribute.Int64("eo.raft.learn_worker.duration."+key+".avg_us", avg.Microseconds()),
			attribute.Int64("eo.raft.learn_worker.duration."+key+".max_us", w.maxDurations[key].Microseconds()),
		)
	}
	for _, key := range sortedInt64MapKeys(w.lastGauges) {
		attrs = append(attrs,
			attribute.Int64("eo.raft.learn_worker.gauge."+key+".last", w.lastGauges[key]),
			attribute.Int64("eo.raft.learn_worker.gauge."+key+".max", w.maxGauges[key]),
		)
	}

	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"eo.raft.learn_worker.window",
		trace.WithTimestamp(w.start),
		trace.WithAttributes(attrs...),
	)
	span.End(trace.WithTimestamp(now))
	w.logWindow(leaderID, elapsed)
	w.reset(now)
}

func (w *raftLearnWorkerTraceWindowStats) logWindow(leaderID uint64, elapsed time.Duration) {
	if !eoRaftWindowLogsEnabled() {
		return
	}
	entriesPerSec := float64(0)
	if elapsed > 0 {
		entriesPerSec = float64(w.counts["entries"]) / elapsed.Seconds()
	}
	log.Printf(
		"%s: eo_raft_learn_worker_window window_ms=%d leader=%t entries_per_s=%.1f batches=%d entries=%d queue_wait_us=%d callback_us=%d batch_size_max=%d queue_depth_max=%d",
		w.nodeName,
		elapsed.Milliseconds(),
		leaderID == w.selfID && w.selfID != 0,
		entriesPerSec,
		w.counts["batches"],
		w.counts["entries"],
		w.durations["queue_wait"].Microseconds(),
		w.durations["callback"].Microseconds(),
		w.maxGauges["batch_size"],
		w.maxGauges["queue_depth_before_drain"],
	)
}

type raftSendWorkerTraceWindowStats struct {
	nodeName       string
	selfID         uint64
	peerID         uint64
	peer           string
	start          time.Time
	counts         map[string]int64
	durations      map[string]time.Duration
	durationCounts map[string]int64
	maxDurations   map[string]time.Duration
	lastGauges     map[string]int64
	maxGauges      map[string]int64
}

func newRaftSendWorkerTraceWindowStats(nodeName string, selfID uint64, peerID uint64, peer string) *raftSendWorkerTraceWindowStats {
	w := &raftSendWorkerTraceWindowStats{
		nodeName: nodeName,
		selfID:   selfID,
		peerID:   peerID,
		peer:     peer,
	}
	w.reset(time.Now())
	return w
}

func (w *raftSendWorkerTraceWindowStats) reset(start time.Time) {
	w.start = start
	w.counts = make(map[string]int64)
	w.durations = make(map[string]time.Duration)
	w.durationCounts = make(map[string]int64)
	w.maxDurations = make(map[string]time.Duration)
	w.lastGauges = make(map[string]int64)
	w.maxGauges = make(map[string]int64)
}

func (w *raftSendWorkerTraceWindowStats) addCount(name string, value int64) {
	if value == 0 {
		return
	}
	w.counts[name] += value
}

func (w *raftSendWorkerTraceWindowStats) addDuration(name string, duration time.Duration) {
	if duration <= 0 {
		return
	}
	w.durations[name] += duration
	w.durationCounts[name]++
	if duration > w.maxDurations[name] {
		w.maxDurations[name] = duration
	}
}

func (w *raftSendWorkerTraceWindowStats) observeGauge(name string, value int) {
	v := int64(value)
	w.lastGauges[name] = v
	if v > w.maxGauges[name] {
		w.maxGauges[name] = v
	}
}

func (w *raftSendWorkerTraceWindowStats) recordBatch(batch []sendRequest, queueDepthBeforeDrain int, queueDepthAfterDrain int, sendStarted time.Time, sendDuration time.Duration, sendError bool) {
	w.addCount("batches", 1)
	w.addCount("messages", int64(len(batch)))
	w.addDuration("send_rpc", sendDuration)
	for _, request := range batch {
		if !request.enqueued.IsZero() {
			w.addDuration("queue_wait", sendStarted.Sub(request.enqueued))
		}
		w.addCount("message_type_"+raftMessageTypeName(request.message.Type), 1)
	}
	if sendError {
		w.addCount("send_errors", 1)
	}
	w.observeGauge("batch_size", len(batch))
	w.observeGauge("queue_depth_before_drain", queueDepthBeforeDrain)
	w.observeGauge("queue_depth_after_drain", queueDepthAfterDrain)
}

func (w *raftSendWorkerTraceWindowStats) maybeFlush(leaderID uint64, reason string) {
	if time.Since(w.start) >= raftLoopTraceWindow {
		w.flush(leaderID, reason)
	}
}

func (w *raftSendWorkerTraceWindowStats) flush(leaderID uint64, reason string) {
	if w.counts["batches"] == 0 {
		w.reset(time.Now())
		return
	}
	now := time.Now()
	elapsed := now.Sub(w.start)
	attrs := []attribute.KeyValue{
		attribute.String("raft.loop.component", "eo"),
		attribute.String("node.name", w.nodeName),
		attribute.Int64("raft.node_id", int64(w.selfID)),
		attribute.Int64("raft.leader_id", int64(leaderID)),
		attribute.Bool("raft.loop.is_leader", leaderID == w.selfID && w.selfID != 0),
		attribute.String("raft.peer", w.peer),
		attribute.Int64("raft.peer_id", int64(w.peerID)),
		attribute.String("eo.raft.send_worker.flush_reason", reason),
		attribute.Int64("eo.raft.send_worker.window_us", elapsed.Microseconds()),
	}
	for _, key := range sortedInt64MapKeys(w.counts) {
		attrs = append(attrs, attribute.Int64("eo.raft.send_worker.count."+key, w.counts[key]))
	}
	for _, key := range sortedDurationMapKeys(w.durations) {
		total := w.durations[key]
		count := w.durationCounts[key]
		avg := time.Duration(0)
		if count > 0 {
			avg = total / time.Duration(count)
		}
		attrs = append(attrs,
			attribute.Int64("eo.raft.send_worker.duration."+key+".count", count),
			attribute.Int64("eo.raft.send_worker.duration."+key+".total_us", total.Microseconds()),
			attribute.Int64("eo.raft.send_worker.duration."+key+".avg_us", avg.Microseconds()),
			attribute.Int64("eo.raft.send_worker.duration."+key+".max_us", w.maxDurations[key].Microseconds()),
		)
	}
	for _, key := range sortedInt64MapKeys(w.lastGauges) {
		attrs = append(attrs,
			attribute.Int64("eo.raft.send_worker.gauge."+key+".last", w.lastGauges[key]),
			attribute.Int64("eo.raft.send_worker.gauge."+key+".max", w.maxGauges[key]),
		)
	}

	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"eo.raft.send_worker.window",
		trace.WithTimestamp(w.start),
		trace.WithAttributes(attrs...),
	)
	span.End(trace.WithTimestamp(now))
	w.logWindow(leaderID, elapsed)
	w.reset(now)
}

func (w *raftSendWorkerTraceWindowStats) logWindow(leaderID uint64, elapsed time.Duration) {
	if !eoRaftWindowLogsEnabled() {
		return
	}
	messagesPerSec := float64(0)
	if elapsed > 0 {
		messagesPerSec = float64(w.counts["messages"]) / elapsed.Seconds()
	}
	log.Printf(
		"%s: eo_raft_send_worker_window peer=%s window_ms=%d leader=%t messages_per_s=%.1f batches=%d messages=%d send_errors=%d queue_wait_us=%d send_rpc_us=%d batch_size_max=%d queue_depth_max=%d",
		w.nodeName,
		w.peer,
		elapsed.Milliseconds(),
		leaderID == w.selfID && w.selfID != 0,
		messagesPerSec,
		w.counts["batches"],
		w.counts["messages"],
		w.counts["send_errors"],
		w.durations["queue_wait"].Microseconds(),
		w.durations["send_rpc"].Microseconds(),
		w.maxGauges["batch_size"],
		w.maxGauges["queue_depth_before_drain"],
	)
}

func newRaftConsensusBox(cfg BoxConfig, onLearn LearnFunc) (ConsensusBox, error) {
	peerIDs, peers, selfID, err := buildPeerIDs(cfg.Name, cfg.Peers)
	if err != nil {
		return nil, err
	}
	if cfg.SendRaft == nil {
		return nil, fmt.Errorf("raft consensus box requires a SendRaft callback")
	}
	sendBatch := cfg.SendRaftBatch
	if sendBatch == nil {
		sendBatch = func(peer string, messages []raftpb.Message) error {
			for _, message := range messages {
				if err := cfg.SendRaft(peer, message); err != nil {
					return err
				}
			}
			return nil
		}
	}

	tickInterval := cfg.TickInterval
	if tickInterval <= 0 {
		tickInterval = 10 * time.Millisecond
	}
	electionTick := cfg.ElectionTick
	if electionTick <= 0 {
		electionTick = 10
	}
	heartbeatTick := cfg.HeartbeatTick
	if heartbeatTick <= 0 {
		heartbeatTick = 1
	}
	if heartbeatTick >= electionTick {
		return nil, fmt.Errorf("heartbeat tick must be smaller than election tick")
	}
	maxInflight := cfg.MaxInflightMsgs
	if maxInflight <= 0 {
		maxInflight = 256
	}
	maxSizePerMsg := cfg.MaxSizePerMsg
	if maxSizePerMsg == 0 {
		maxSizePerMsg = 1 << 20
	}
	sendBatchSize := cfg.RaftSendBatchSize
	if sendBatchSize <= 0 {
		sendBatchSize = defaultRaftSendBatchSize
	}
	learnBatchSize := cfg.LearnBatchSize
	if learnBatchSize <= 0 {
		learnBatchSize = defaultRaftLearnBatchSize
	}
	learnBatch := cfg.LearnBatch
	if learnBatch == nil && onLearn != nil {
		learnBatch = func(entries []CommittedEntry) {
			for _, entry := range entries {
				onLearn(entry.Slot, entry.Entry)
			}
		}
	}

	storage := raft.NewMemoryStorage()
	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              selfID,
		ElectionTick:    electionTick,
		HeartbeatTick:   heartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflight,
	})
	if err != nil {
		return nil, err
	}

	bootstrapPeers := make([]raft.Peer, 0, len(peerIDs))
	peerNames := make([]string, 0, len(peerIDs))
	for peer := range peerIDs {
		peerNames = append(peerNames, peer)
	}
	sort.Strings(peerNames)
	for _, peer := range peerNames {
		bootstrapPeers = append(bootstrapPeers, raft.Peer{ID: peerIDs[peer]})
	}
	if err := rawNode.Bootstrap(bootstrapPeers); err != nil {
		return nil, err
	}

	box := &raftConsensusBox{
		name:            cfg.Name,
		selfID:          selfID,
		peerIDs:         peerIDs,
		peers:           peers,
		send:            cfg.SendRaft,
		sendBatch:       sendBatch,
		learn:           onLearn,
		learnBatch:      learnBatch,
		proposals:       make(chan proposalRequest, raftProposalQueueSize),
		prioritySteps:   make(chan stepRequest, 1024),
		backgroundSteps: make(chan stepRequest, 1024),
		sendQueues:      make(map[uint64]chan sendRequest),
		sendBatchSize:   sendBatchSize,
		learnBatchSize:  learnBatchSize,
		learnQueue:      make(chan asyncLearnRequest, raftAsyncLearnQueueSize),
		unreachable:     make(chan unreachablePeer, 1024),
		spans:           make(map[string]trace.Span),
		commitWaitSpans: make(map[string]trace.Span),
		appendAckSpans:  make(map[uint64][]pendingAppendSignal),
		ackCommitSpans:  make(map[uint64][]pendingCommitSignal),
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
	}

	for id := range peers {
		if id == selfID {
			continue
		}
		queue := make(chan sendRequest, raftAsyncSendQueueSize)
		box.sendQueues[id] = queue
		go box.runSendWorker(id, queue)
	}
	go box.runLearnWorker()
	go box.run(rawNode, storage, tickInterval, cfg.DisableFollowerElections)
	return box, nil
}

func (b *raftConsensusBox) IsLeader() bool {
	return b.leaderID.Load() == b.selfID && b.selfID != 0
}

// IsPrimary is kept as a compatibility alias for older callers.
func (b *raftConsensusBox) IsPrimary() bool {
	return b.IsLeader()
}

func (b *raftConsensusBox) Leader() (string, bool) {
	leaderID := b.leaderID.Load()
	if leaderID == 0 {
		return "", false
	}
	peer, ok := b.peers[leaderID]
	return peer, ok
}

// Primary is kept as a compatibility alias for older callers.
func (b *raftConsensusBox) Primary() (string, bool) {
	return b.Leader()
}

func (b *raftConsensusBox) Propose(entry Entry) error {
	result := make(chan error, 1)
	span := b.startRequestTrace(entry)
	request := proposalRequest{entry: entry, result: result, span: span}

	channelSpan := b.startProposeChannelSendTrace(entry)
	if err := b.enqueueProposal(request); err != nil {
		endRequestTrace(channelSpan, "stopped_before_enqueue")
		endRequestTrace(span, "stopped_before_enqueue")
		return err
	}
	endRequestTrace(channelSpan, "proposal_enqueued")
	addRequestTraceEvent(span, "proposal_enqueued")

	waitSpan := b.startProposeResultWaitTrace(entry)
	select {
	case <-b.doneCh:
		endRequestTrace(waitSpan, "stopped_before_result")
		return errConsensusBoxStopped
	case err := <-result:
		if err != nil && waitSpan != nil {
			waitSpan.RecordError(err)
		}
		endRequestTrace(waitSpan, "result_received")
		return err
	}
}

func (b *raftConsensusBox) ProposeAsync(entry Entry) error {
	span := b.startRequestTrace(entry)
	request := proposalRequest{
		entry: entry,
		span:  span,
	}
	channelSpan := b.startProposeChannelSendTrace(entry)
	if err := b.enqueueProposal(request); err != nil {
		endRequestTrace(channelSpan, "stopped_before_enqueue")
		endRequestTrace(span, "stopped_before_enqueue")
		return err
	}
	endRequestTrace(channelSpan, "proposal_enqueued")
	addRequestTraceEvent(span, "proposal_enqueued")
	return nil
}

func (b *raftConsensusBox) enqueueProposal(request proposalRequest) error {
	if request.enqueued.IsZero() {
		request.enqueued = time.Now()
	}

	select {
	case <-b.stopCh:
		return errConsensusBoxStopped
	case <-b.doneCh:
		return errConsensusBoxStopped
	default:
	}

	select {
	case <-b.stopCh:
		return errConsensusBoxStopped
	case <-b.doneCh:
		return errConsensusBoxStopped
	case b.proposals <- request:
		return nil
	}
}

func (b *raftConsensusBox) HandleMessage(message raftpb.Message) error {
	request := stepRequest{message: message, enqueued: time.Now()}
	queue := b.stepQueue(message)
	select {
	case <-b.doneCh:
		return errConsensusBoxStopped
	default:
	}

	if message.Type == raftpb.MsgHeartbeatResp {
		select {
		case <-b.doneCh:
			return errConsensusBoxStopped
		case queue <- request:
			return nil
		default:
			return nil
		}
	}

	select {
	case <-b.doneCh:
		return errConsensusBoxStopped
	case queue <- request:
		return nil
	}
}

func (b *raftConsensusBox) Stop() {
	b.stopOnce.Do(func() {
		close(b.stopCh)
		<-b.doneCh
	})
}

func (b *raftConsensusBox) runLearnWorker() {
	batch := make([]asyncLearnRequest, 0, b.learnBatchSize)
	committed := make([]CommittedEntry, 0, b.learnBatchSize)
	workerTrace := newRaftLearnWorkerTraceWindowStats(b.name, b.selfID)
	defer workerTrace.flush(b.leaderID.Load(), "stop")

	for {
		select {
		case <-b.stopCh:
			return
		case request := <-b.learnQueue:
			batch = append(batch[:0], request)
			queueDepthBeforeDrain := len(b.learnQueue)
		drain:
			for len(batch) < b.learnBatchSize {
				select {
				case <-b.stopCh:
					return
				case next := <-b.learnQueue:
					batch = append(batch, next)
				default:
					break drain
				}
			}
			queueDepthAfterDrain := len(b.learnQueue)

			callbackStart := time.Now()
			if b.learnBatch != nil {
				committed = committed[:0]
				for _, request := range batch {
					committed = append(committed, CommittedEntry{Slot: request.slot, Entry: request.entry})
				}
				b.learnBatch(committed)
			} else if b.learn != nil {
				for _, request := range batch {
					b.learn(request.slot, request.entry)
				}
			}
			workerTrace.recordBatch(batch, queueDepthBeforeDrain, queueDepthAfterDrain, time.Since(callbackStart))
			workerTrace.maybeFlush(b.leaderID.Load(), "window")
		}
	}
}

func (b *raftConsensusBox) run(rawNode *raft.RawNode, storage *raft.MemoryStorage, tickInterval time.Duration, disableFollowerElections bool) {
	defer b.endOpenRequestTraces("box_stopped")
	defer b.endQueuedProposalTraces("box_stopped")
	defer close(b.doneCh)

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	loopTrace := newRaftLoopTraceWindowStats("eo", b.name, b.selfID)
	defer loopTrace.flush(b.leaderID.Load(), "stop")

	b.drainReady(rawNode, storage)

	for {
		selectStart := time.Now()
		select {
		case request := <-b.prioritySteps:
			selectWait := time.Since(selectStart)
			handlerStart := time.Now()
			loopTrace.recordStepRequest("priority", request)
			stepDuration, drainStats, drainDuration := b.processStep(rawNode, storage, request)
			handlerDuration := time.Since(handlerStart)
			loopTrace.addCount("iterations", 1)
			loopTrace.addCount("priority_steps", 1)
			loopTrace.addDuration("select_wait", selectWait)
			loopTrace.addDuration("handler", handlerDuration)
			loopTrace.addDuration("raw_step", stepDuration)
			loopTrace.recordDrain(drainStats, drainDuration)
			loopTrace.recordQueues(len(b.proposals), len(b.prioritySteps), len(b.backgroundSteps), len(b.unreachable), len(b.learnQueue))
			loopTrace.maybeFlush(b.leaderID.Load(), "window")
			continue
		default:
		}

		selectStart = time.Now()
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			selectWait := time.Since(selectStart)
			handlerStart := time.Now()
			if shouldTickRaft(rawNode.Status(), disableFollowerElections) {
				tickStart := time.Now()
				rawNode.Tick()
				tickDuration := time.Since(tickStart)
				drainStats, drainDuration := b.drainReady(rawNode, storage)
				loopTrace.addDuration("raw_tick", tickDuration)
				loopTrace.recordDrain(drainStats, drainDuration)
				loopTrace.addCount("ticks", 1)
			} else {
				loopTrace.addCount("ticks_skipped", 1)
			}
			loopTrace.addCount("iterations", 1)
			loopTrace.addDuration("select_wait", selectWait)
			loopTrace.addDuration("handler", time.Since(handlerStart))
			loopTrace.recordQueues(len(b.proposals), len(b.prioritySteps), len(b.backgroundSteps), len(b.unreachable), len(b.learnQueue))
			loopTrace.maybeFlush(b.leaderID.Load(), "window")
		case request := <-b.proposals:
			selectWait := time.Since(selectStart)
			handlerStart := time.Now()
			if !request.enqueued.IsZero() {
				loopTrace.addDuration("proposal_queue_wait", time.Since(request.enqueued))
			}
			proposalSpan := b.startProposalLoopTrace(request.entry)
			b.rememberRequestTrace(request.entry, request.span)
			marshalStart := time.Now()
			data, err := json.Marshal(request.entry)
			marshalDuration := time.Since(marshalStart)
			var proposeDuration time.Duration
			var drainStats drainReadyStats
			var drainDuration time.Duration
			if err == nil {
				addRequestTraceEvent(request.span, "propose")
				proposeStart := time.Now()
				err = rawNode.Propose(data)
				proposeDuration = time.Since(proposeStart)
				if err != nil {
					endRequestTrace(request.span, "propose_error")
					b.forgetRequestTrace(request.entry)
				}
				drainStats, drainDuration = b.drainReady(rawNode, storage)
				if err == nil {
					b.startCommitWaitTrace(request.entry)
				}
			} else {
				endRequestTrace(request.span, "marshal_error")
				b.forgetRequestTrace(request.entry)
			}
			if request.result != nil {
				request.result <- err
			}
			if err != nil && proposalSpan != nil {
				proposalSpan.RecordError(err)
			}
			endRequestTrace(proposalSpan, "result_sent")
			loopTrace.addCount("iterations", 1)
			loopTrace.addCount("proposals", 1)
			if err != nil {
				loopTrace.addCount("proposal_errors", 1)
			}
			loopTrace.addDuration("select_wait", selectWait)
			loopTrace.addDuration("handler", time.Since(handlerStart))
			loopTrace.addDuration("marshal", marshalDuration)
			loopTrace.addDuration("raw_propose", proposeDuration)
			loopTrace.recordDrain(drainStats, drainDuration)
			loopTrace.recordQueues(len(b.proposals), len(b.prioritySteps), len(b.backgroundSteps), len(b.unreachable), len(b.learnQueue))
			loopTrace.maybeFlush(b.leaderID.Load(), "window")
		case unreachable := <-b.unreachable:
			selectWait := time.Since(selectStart)
			handlerStart := time.Now()
			rawNode.ReportUnreachable(unreachable.id)
			drainStats, drainDuration := b.drainReady(rawNode, storage)
			loopTrace.addCount("iterations", 1)
			loopTrace.addCount("unreachable_reports", 1)
			loopTrace.addDuration("select_wait", selectWait)
			loopTrace.addDuration("handler", time.Since(handlerStart))
			loopTrace.recordDrain(drainStats, drainDuration)
			loopTrace.recordQueues(len(b.proposals), len(b.prioritySteps), len(b.backgroundSteps), len(b.unreachable), len(b.learnQueue))
			loopTrace.maybeFlush(b.leaderID.Load(), "window")
		case request := <-b.prioritySteps:
			selectWait := time.Since(selectStart)
			handlerStart := time.Now()
			loopTrace.recordStepRequest("priority", request)
			stepDuration, drainStats, drainDuration := b.processStep(rawNode, storage, request)
			loopTrace.addCount("iterations", 1)
			loopTrace.addCount("priority_steps", 1)
			loopTrace.addDuration("select_wait", selectWait)
			loopTrace.addDuration("handler", time.Since(handlerStart))
			loopTrace.addDuration("raw_step", stepDuration)
			loopTrace.recordDrain(drainStats, drainDuration)
			loopTrace.recordQueues(len(b.proposals), len(b.prioritySteps), len(b.backgroundSteps), len(b.unreachable), len(b.learnQueue))
			loopTrace.maybeFlush(b.leaderID.Load(), "window")
		case request := <-b.backgroundSteps:
			selectWait := time.Since(selectStart)
			handlerStart := time.Now()
			loopTrace.recordStepRequest("background", request)
			stepDuration, drainStats, drainDuration := b.processStep(rawNode, storage, request)
			loopTrace.addCount("iterations", 1)
			loopTrace.addCount("background_steps", 1)
			loopTrace.addDuration("select_wait", selectWait)
			loopTrace.addDuration("handler", time.Since(handlerStart))
			loopTrace.addDuration("raw_step", stepDuration)
			loopTrace.recordDrain(drainStats, drainDuration)
			loopTrace.recordQueues(len(b.proposals), len(b.prioritySteps), len(b.backgroundSteps), len(b.unreachable), len(b.learnQueue))
			loopTrace.maybeFlush(b.leaderID.Load(), "window")
		}
	}
}

func shouldTickRaft(status raft.Status, disableFollowerElections bool) bool {
	if !disableFollowerElections {
		return true
	}
	if status.RaftState == raft.StateLeader {
		return true
	}
	return status.Lead == 0
}

func (b *raftConsensusBox) endQueuedProposalTraces(event string) {
	for {
		select {
		case request := <-b.proposals:
			if request.result != nil {
				select {
				case request.result <- errConsensusBoxStopped:
				default:
				}
			}
			endRequestTrace(request.span, event)
		default:
			return
		}
	}
}

func (b *raftConsensusBox) processStep(rawNode *raft.RawNode, storage *raft.MemoryStorage, request stepRequest) (time.Duration, drainReadyStats, time.Duration) {
	message := request.message
	commitBefore := rawNode.Status().Commit
	stepStart := time.Now()
	if err := rawNode.Step(message); err == nil {
		stepDuration := time.Since(stepStart)
		b.finishAppendAckSignals(message)
		b.trackAckToCommitSignal(message, commitBefore, rawNode.Status().Commit)
		drainStats, drainDuration := b.drainReady(rawNode, storage)
		return stepDuration, drainStats, drainDuration
	}
	return time.Since(stepStart), drainReadyStats{}, 0
}

func (b *raftConsensusBox) drainReady(rawNode *raft.RawNode, storage *raft.MemoryStorage) (drainReadyStats, time.Duration) {
	start := time.Now()
	stats := drainReadyStats{}

	for rawNode.HasReady() {
		readyStart := time.Now()
		ready := rawNode.Ready()
		stats.readyNanos += time.Since(readyStart).Nanoseconds()
		stats.readyBatches++
		stats.entries += len(ready.Entries)
		stats.committedEntries += len(ready.CommittedEntries)
		stats.messages += len(ready.Messages)

		if ready.SoftState != nil {
			b.leaderID.Store(ready.SoftState.Lead)
			stats.leaderTransitions++
		}
		if !raft.IsEmptySnap(ready.Snapshot) {
			stats.snapshots++
			storageStart := time.Now()
			_ = storage.ApplySnapshot(ready.Snapshot)
			stats.storageNanos += time.Since(storageStart).Nanoseconds()
		}
		if !raft.IsEmptyHardState(ready.HardState) {
			stats.hardStates++
			storageStart := time.Now()
			_ = storage.SetHardState(ready.HardState)
			stats.storageNanos += time.Since(storageStart).Nanoseconds()
		}
		if len(ready.Entries) > 0 {
			storageStart := time.Now()
			_ = storage.Append(ready.Entries)
			stats.storageNanos += time.Since(storageStart).Nanoseconds()
		}

		messageStart := time.Now()
		for _, message := range ready.Messages {
			if message.To == b.selfID {
				stats.selfMessages++
				selfStepStart := time.Now()
				_ = rawNode.Step(message)
				stats.selfStepNanos += time.Since(selfStepStart).Nanoseconds()
				continue
			}
			peer, ok := b.peers[message.To]
			if !ok {
				continue
			}
			sendStart := time.Now()
			if b.enqueueSend(peer, message) {
				stats.sentMessages++
			}
			stats.sendEnqueueNanos += time.Since(sendStart).Nanoseconds()
		}
		stats.messageNanos += time.Since(messageStart).Nanoseconds()

		commitStart := time.Now()
		for _, entry := range ready.CommittedEntries {
			learnStart := time.Now()
			b.learnCommittedEntry(rawNode, entry, &stats)
			stats.learnNanos += time.Since(learnStart).Nanoseconds()
		}
		stats.commitNanos += time.Since(commitStart).Nanoseconds()

		advanceStart := time.Now()
		rawNode.Advance(ready)
		stats.advanceNanos += time.Since(advanceStart).Nanoseconds()
	}
	return stats, time.Since(start)
}

func (b *raftConsensusBox) enqueueSend(peer string, message raftpb.Message) bool {
	queue := b.sendQueues[message.To]
	if queue == nil {
		return false
	}
	request := sendRequest{message: message, enqueued: time.Now()}
	select {
	case <-b.stopCh:
		return false
	case queue <- request:
		b.trackAppendAckSignals(message)
		if isCommitIndexMessage(message) {
			b.finishCommitSignals(message.To, message.Commit)
		}
		return true
	}
}

func (b *raftConsensusBox) stepQueue(message raftpb.Message) chan stepRequest {
	if isPriorityStep(message.Type) {
		return b.prioritySteps
	}
	return b.backgroundSteps
}

func isPriorityStep(messageType raftpb.MessageType) bool {
	switch messageType {
	case raftpb.MsgApp, raftpb.MsgAppResp, raftpb.MsgVote, raftpb.MsgVoteResp, raftpb.MsgPreVote, raftpb.MsgPreVoteResp:
		return true
	default:
		return false
	}
}

func (b *raftConsensusBox) runSendWorker(peerID uint64, queue <-chan sendRequest) {
	peer := b.peers[peerID]
	workerTrace := newRaftSendWorkerTraceWindowStats(b.name, b.selfID, peerID, peer)
	defer workerTrace.flush(b.leaderID.Load(), "stop")
	requests := make([]sendRequest, 0, b.sendBatchSize)
	messages := make([]raftpb.Message, 0, b.sendBatchSize)
	for {
		select {
		case <-b.stopCh:
			return
		case request := <-queue:
			if b.sendBatchSize <= 1 {
				sendStart := time.Now()
				err := b.send(peer, request.message)
				workerTrace.recordBatch([]sendRequest{request}, len(queue), len(queue), sendStart, time.Since(sendStart), err != nil)
				workerTrace.maybeFlush(b.leaderID.Load(), "window")
				if err != nil {
					select {
					case b.unreachable <- unreachablePeer{id: peerID}:
					case <-b.stopCh:
					default:
					}
				}
				continue
			}
			requests = append(requests[:0], request)
			queueDepthBeforeDrain := len(queue)
		drain:
			for len(requests) < b.sendBatchSize {
				select {
				case next := <-queue:
					requests = append(requests, next)
				default:
					break drain
				}
			}
			queueDepthAfterDrain := len(queue)
			sentRequests, coalesced := coalescePositiveAppRespRequests(requests)
			if coalesced > 0 {
				workerTrace.addCount("coalesced_app_responses", int64(coalesced))
			}
			messages = messages[:0]
			for _, request := range sentRequests {
				messages = append(messages, request.message)
			}
			sendStart := time.Now()
			err := b.sendBatch(peer, messages)
			workerTrace.recordBatch(sentRequests, queueDepthBeforeDrain, queueDepthAfterDrain, sendStart, time.Since(sendStart), err != nil)
			workerTrace.maybeFlush(b.leaderID.Load(), "window")
			if err != nil {
				select {
				case b.unreachable <- unreachablePeer{id: peerID}:
				case <-b.stopCh:
				default:
				}
				continue
			}
		}
	}
}

func coalescePositiveAppRespRequests(requests []sendRequest) ([]sendRequest, int) {
	if len(requests) <= 1 {
		return requests, 0
	}

	out := requests[:0]
	var latest sendRequest
	haveLatest := false
	coalesced := 0
	flushLatest := func() {
		if haveLatest {
			out = append(out, latest)
			haveLatest = false
		}
	}

	for _, request := range requests {
		if isCoalescablePositiveAppResp(request.message) {
			if haveLatest && !sameAppRespStream(latest.message, request.message) {
				flushLatest()
			}
			if haveLatest {
				coalesced++
				if request.message.Index >= latest.message.Index {
					latest = request
				}
				continue
			}
			latest = request
			haveLatest = true
			continue
		}
		flushLatest()
		out = append(out, request)
	}
	flushLatest()
	return out, coalesced
}

func isCoalescablePositiveAppResp(message raftpb.Message) bool {
	return message.Type == raftpb.MsgAppResp && !message.Reject && message.Index > 0
}

func sameAppRespStream(left raftpb.Message, right raftpb.Message) bool {
	return left.From == right.From && left.To == right.To && left.Term == right.Term
}

func (b *raftConsensusBox) learnCommittedEntry(rawNode *raft.RawNode, entry raftpb.Entry, stats *drainReadyStats) {
	switch entry.Type {
	case raftpb.EntryConfChange:
		if stats != nil {
			stats.confChanges++
		}
		if len(entry.Data) == 0 {
			return
		}
		var change raftpb.ConfChange
		if err := change.Unmarshal(entry.Data); err == nil {
			rawNode.ApplyConfChange(change)
		}
	case raftpb.EntryConfChangeV2:
		if stats != nil {
			stats.confChanges++
		}
		if len(entry.Data) == 0 {
			return
		}
		var change raftpb.ConfChangeV2
		if err := change.Unmarshal(entry.Data); err == nil {
			rawNode.ApplyConfChange(change)
		}
	case raftpb.EntryNormal:
		if stats != nil {
			stats.normalEntries++
		}
		if len(entry.Data) == 0 {
			return
		}
		var value Entry
		decodeStart := time.Now()
		err := json.Unmarshal(entry.Data, &value)
		if stats != nil {
			stats.learnDecodeNanos += time.Since(decodeStart).Nanoseconds()
		}
		if err != nil {
			return
		}
		b.learnedSlot++
		b.finishCommittedRequestTrace(b.learnedSlot, value)
		cloneStart := time.Now()
		cloned := cloneEntry(value)
		if stats != nil {
			stats.learnCloneNanos += time.Since(cloneStart).Nanoseconds()
		}
		request := asyncLearnRequest{slot: b.learnedSlot, entry: cloned, enqueued: time.Now()}
		enqueueStart := time.Now()
		select {
		case <-b.stopCh:
			return
		case b.learnQueue <- request:
		}
		if stats != nil {
			stats.learnEnqueueNanos += time.Since(enqueueStart).Nanoseconds()
		}
	}
}

func (b *raftConsensusBox) startRequestTrace(entry Entry) trace.Span {
	if entry.RequestID == "" || !telemetry.DetailedSpansEnabled() {
		return nil
	}
	_, span := telemetry.StartSpanFromPayload(
		entry.Response,
		"eo.raft.request_to_commit",
		append(
			b.entryTraceAttrs(entry),
			attribute.String("eo.raft.trace_scope", "local_propose"),
		)...,
	)
	addRequestTraceEvent(span, "request")
	return span
}

func (b *raftConsensusBox) startProposeChannelSendTrace(entry Entry) trace.Span {
	if entry.RequestID == "" || !telemetry.DetailedSpansEnabled() {
		return nil
	}
	_, span := telemetry.StartSpanFromPayload(
		entry.Response,
		"eo.raft.propose_channel_send",
		append(
			b.entryTraceAttrs(entry),
			attribute.Int("eo.raft.proposals_queued_before_send", len(b.proposals)),
		)...,
	)
	return span
}

func (b *raftConsensusBox) startProposeResultWaitTrace(entry Entry) trace.Span {
	if entry.RequestID == "" || !telemetry.DetailedSpansEnabled() {
		return nil
	}
	_, span := telemetry.StartSpanFromPayload(
		entry.Response,
		"eo.raft.propose_result_wait",
		append(
			b.entryTraceAttrs(entry),
			attribute.Int("eo.raft.proposals_queued_after_send", len(b.proposals)),
		)...,
	)
	return span
}

func (b *raftConsensusBox) rememberRequestTrace(entry Entry, span trace.Span) {
	if span == nil || entry.RequestID == "" {
		return
	}
	b.spans[entry.RequestID] = span
	addRequestTraceEvent(span, "proposal_received")
}

func (b *raftConsensusBox) forgetRequestTrace(entry Entry) {
	if entry.RequestID == "" {
		return
	}
	delete(b.spans, entry.RequestID)
}

func (b *raftConsensusBox) finishCommittedRequestTrace(slot uint64, entry Entry) {
	if entry.RequestID == "" || !telemetry.DetailedSpansEnabled() {
		return
	}
	b.finishCommitWaitTrace(slot, entry)
	span, ok := b.spans[entry.RequestID]
	if !ok {
		_, span = telemetry.StartSpanFromPayload(
			entry.Response,
			"eo.raft.request_to_commit",
			append(
				b.entryTraceAttrs(entry),
				attribute.Int64("eo.raft.commit_slot", int64(slot)),
				attribute.String("eo.raft.trace_scope", "commit_observed"),
			)...,
		)
		endRequestTrace(span, "commit_observed")
		return
	}
	delete(b.spans, entry.RequestID)
	span.SetAttributes(
		attribute.Int64("eo.raft.commit_slot", int64(slot)),
		attribute.String("eo.raft.trace_scope", "local_propose"),
	)
	endRequestTrace(span, "commit")
}

func (b *raftConsensusBox) endOpenRequestTraces(event string) {
	for requestID, span := range b.spans {
		endRequestTrace(span, event)
		delete(b.spans, requestID)
	}
	for requestID, span := range b.commitWaitSpans {
		endRequestTrace(span, event)
		delete(b.commitWaitSpans, requestID)
	}
	for peerID, signals := range b.appendAckSpans {
		for _, signal := range signals {
			endRequestTrace(signal.span, event)
		}
		delete(b.appendAckSpans, peerID)
	}
	for peerID, signals := range b.ackCommitSpans {
		for _, signal := range signals {
			endRequestTrace(signal.span, event)
		}
		delete(b.ackCommitSpans, peerID)
	}
}

func (b *raftConsensusBox) startProposalLoopTrace(entry Entry) trace.Span {
	if entry.RequestID == "" || !telemetry.DetailedSpansEnabled() {
		return nil
	}
	_, span := telemetry.StartSpanFromPayload(
		entry.Response,
		"eo.raft.proposal_loop",
		append(
			b.entryTraceAttrs(entry),
			attribute.Int("eo.raft.proposals_queued_after_dequeue", len(b.proposals)),
			attribute.Int("eo.raft.priority_steps_queued", len(b.prioritySteps)),
			attribute.Int("eo.raft.background_steps_queued", len(b.backgroundSteps)),
		)...,
	)
	return span
}

func (b *raftConsensusBox) startCommitWaitTrace(entry Entry) {
	if entry.RequestID == "" || !telemetry.DetailedSpansEnabled() {
		return
	}
	_, span := telemetry.StartSpanFromPayload(
		entry.Response,
		"eo.raft.wait_commit_after_propose",
		append(
			b.entryTraceAttrs(entry),
			attribute.Int("eo.raft.priority_steps_queued_after_propose", len(b.prioritySteps)),
			attribute.Int("eo.raft.background_steps_queued_after_propose", len(b.backgroundSteps)),
		)...,
	)
	b.commitWaitSpans[entry.RequestID] = span
}

func (b *raftConsensusBox) finishCommitWaitTrace(slot uint64, entry Entry) {
	span := b.commitWaitSpans[entry.RequestID]
	if span == nil {
		return
	}
	delete(b.commitWaitSpans, entry.RequestID)
	span.SetAttributes(attribute.Int64("eo.raft.commit_slot", int64(slot)))
	endRequestTrace(span, "commit")
}

func (b *raftConsensusBox) startRunIterationTrace(event string) trace.Span {
	if !telemetry.DetailedSpansEnabled() {
		return nil
	}
	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"eo.raft.run_iteration",
		trace.WithAttributes(
			attribute.String("node.name", b.name),
			attribute.Int64("raft.node_id", int64(b.selfID)),
			attribute.Int64("raft.leader_id", int64(b.leaderID.Load())),
			attribute.String("eo.raft.run_event", event),
			attribute.Int("eo.raft.proposals_queued", len(b.proposals)),
			attribute.Int("eo.raft.priority_steps_queued", len(b.prioritySteps)),
			attribute.Int("eo.raft.background_steps_queued", len(b.backgroundSteps)),
			attribute.Int("eo.raft.unreachable_queued", len(b.unreachable)),
		),
	)
	return span
}

func (b *raftConsensusBox) endRunIterationTrace(span trace.Span, event string) {
	if span == nil {
		return
	}
	span.SetAttributes(
		attribute.String("eo.raft.run_event", event),
		attribute.Int("eo.raft.proposals_queued_after", len(b.proposals)),
		attribute.Int("eo.raft.priority_steps_queued_after", len(b.prioritySteps)),
		attribute.Int("eo.raft.background_steps_queued_after", len(b.backgroundSteps)),
		attribute.Int("eo.raft.unreachable_queued_after", len(b.unreachable)),
	)
	span.End()
}

func (b *raftConsensusBox) trackAppendAckSignals(message raftpb.Message) {
	if !telemetry.DetailedSpansEnabled() || message.Type != raftpb.MsgApp || len(message.Entries) == 0 || !b.IsLeader() {
		return
	}
	peer := b.peers[message.To]
	for _, entry := range message.Entries {
		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}
		var value Entry
		if err := json.Unmarshal(entry.Data, &value); err != nil || value.RequestID == "" {
			continue
		}
		_, span := telemetry.StartSpanFromPayload(
			value.Response,
			"eo.raft.append_send_to_ack",
			append(
				b.entryTraceAttrs(value),
				attribute.String("raft.peer", peer),
				attribute.Int64("raft.message.to", int64(message.To)),
				attribute.Int64("raft.message.index", int64(message.Index)),
				attribute.Int64("raft.entry.index", int64(entry.Index)),
			)...,
		)
		b.appendAckSpans[message.To] = append(b.appendAckSpans[message.To], pendingAppendSignal{
			index:     entry.Index,
			requestID: value.RequestID,
			span:      span,
		})
	}
}

func (b *raftConsensusBox) finishAppendAckSignals(message raftpb.Message) {
	if message.Type != raftpb.MsgAppResp || message.Reject || message.Index == 0 || !b.IsLeader() {
		return
	}
	pending := b.appendAckSpans[message.From]
	if len(pending) == 0 {
		return
	}
	remaining := pending[:0]
	var finished []pendingAppendSignal
	for _, signal := range pending {
		if signal.index <= message.Index {
			finished = append(finished, signal)
			continue
		}
		remaining = append(remaining, signal)
	}
	if len(remaining) == 0 {
		delete(b.appendAckSpans, message.From)
	} else {
		b.appendAckSpans[message.From] = remaining
	}
	for _, signal := range finished {
		signal.span.SetAttributes(attribute.Int64("raft.message.ack_index", int64(message.Index)))
		endRequestTrace(signal.span, "ack_processed")
	}
}

func (b *raftConsensusBox) trackAckToCommitSignal(message raftpb.Message, commitBefore uint64, commitAfter uint64) {
	if !telemetry.DetailedSpansEnabled() || message.Type != raftpb.MsgAppResp || message.Reject || message.Index == 0 || !b.IsLeader() {
		return
	}
	peer := b.peers[message.From]
	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"eo.raft.ack_to_commit_send",
		trace.WithAttributes(
			attribute.String("node.name", b.name),
			attribute.Int64("raft.node_id", int64(b.selfID)),
			attribute.Int64("raft.leader_id", int64(b.leaderID.Load())),
			attribute.String("raft.peer", peer),
			attribute.Int64("raft.message.from", int64(message.From)),
			attribute.Int64("raft.message.to", int64(message.To)),
			attribute.Int64("raft.message.term", int64(message.Term)),
			attribute.Int64("raft.message.index", int64(message.Index)),
			attribute.Int64("raft.status.commit_before_step", int64(commitBefore)),
			attribute.Int64("raft.status.commit_after_step", int64(commitAfter)),
		),
	)
	b.ackCommitSpans[message.From] = append(b.ackCommitSpans[message.From], pendingCommitSignal{
		index: message.Index,
		span:  span,
	})
}

func (b *raftConsensusBox) finishCommitSignals(peerID uint64, commit uint64) {
	if commit == 0 {
		return
	}
	pending := b.ackCommitSpans[peerID]
	if len(pending) == 0 {
		return
	}
	remaining := pending[:0]
	var finished []pendingCommitSignal
	for _, signal := range pending {
		if signal.index <= commit {
			finished = append(finished, signal)
			continue
		}
		remaining = append(remaining, signal)
	}
	if len(remaining) == 0 {
		delete(b.ackCommitSpans, peerID)
	} else {
		b.ackCommitSpans[peerID] = remaining
	}
	for _, signal := range finished {
		signal.span.SetAttributes(attribute.Int64("raft.message.commit", int64(commit)))
		endRequestTrace(signal.span, "commit_index_enqueued")
	}
}

func (b *raftConsensusBox) entryTraceAttrs(entry Entry) []attribute.KeyValue {
	attrs := append([]attribute.KeyValue{}, telemetry.AttrsFromPayload(entry.Response)...)
	attrs = append(attrs,
		attribute.String("node.name", b.name),
		attribute.Int64("raft.node_id", int64(b.selfID)),
	)
	if entry.RequestID != "" {
		attrs = append(attrs, attribute.String("request.id", entry.RequestID))
	}
	return attrs
}

func isCommitIndexMessage(message raftpb.Message) bool {
	if message.Commit == 0 {
		return false
	}
	switch message.Type {
	case raftpb.MsgApp, raftpb.MsgHeartbeat:
		return true
	default:
		return false
	}
}

func raftMessageTypeName(messageType raftpb.MessageType) string {
	name := messageType.String()
	name = strings.TrimPrefix(name, "Msg")
	name = strings.ToLower(name)
	if name == "" {
		return "unknown"
	}
	return name
}

func eoRaftWindowLogsEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("AEGEAN_ENABLE_LOGGING"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func addRequestTraceEvent(span trace.Span, event string) {
	if span != nil {
		span.AddEvent(event)
	}
}

func endRequestTrace(span trace.Span, event string) {
	if span == nil {
		return
	}
	span.AddEvent(event)
	span.End()
}

func buildPeerIDs(self string, peers []string) (map[string]uint64, map[uint64]string, uint64, error) {
	if self == "" {
		return nil, nil, 0, fmt.Errorf("raft consensus box requires a node name")
	}
	if len(peers) == 0 {
		return nil, nil, 0, fmt.Errorf("raft consensus box requires peers")
	}

	uniquePeers := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		if peer == "" {
			continue
		}
		uniquePeers[peer] = struct{}{}
	}
	if _, ok := uniquePeers[self]; !ok {
		uniquePeers[self] = struct{}{}
	}

	peerNames := make([]string, 0, len(uniquePeers))
	for peer := range uniquePeers {
		peerNames = append(peerNames, peer)
	}
	sort.Strings(peerNames)

	peerIDs := make(map[string]uint64, len(peerNames))
	reverse := make(map[uint64]string, len(peerNames))
	for index, peer := range peerNames {
		id := uint64(index + 1)
		peerIDs[peer] = id
		reverse[id] = peer
	}
	return peerIDs, reverse, peerIDs[self], nil
}
