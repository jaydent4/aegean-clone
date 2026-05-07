package eo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	entry  Entry
	result chan error
	span   trace.Span
}

type unreachablePeer struct {
	id uint64
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
}

type raftConsensusBox struct {
	name    string
	selfID  uint64
	peerIDs map[string]uint64
	peers   map[uint64]string
	send    SendRaftFunc
	learn   LearnFunc

	leaderID    atomic.Uint64
	learnedSlot uint64

	proposals   chan proposalRequest
	steps       chan raftpb.Message
	unreachable chan unreachablePeer
	spans       map[string]trace.Span
	stopOnce    sync.Once
	stopCh      chan struct{}
	doneCh      chan struct{}
}

func newRaftConsensusBox(cfg BoxConfig, onLearn LearnFunc) (ConsensusBox, error) {
	peerIDs, peers, selfID, err := buildPeerIDs(cfg.Name, cfg.Peers)
	if err != nil {
		return nil, err
	}
	if cfg.SendRaft == nil {
		return nil, fmt.Errorf("raft consensus box requires a SendRaft callback")
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
		name:        cfg.Name,
		selfID:      selfID,
		peerIDs:     peerIDs,
		peers:       peers,
		send:        cfg.SendRaft,
		learn:       onLearn,
		proposals:   make(chan proposalRequest),
		steps:       make(chan raftpb.Message, 1024),
		unreachable: make(chan unreachablePeer, 1024),
		spans:       make(map[string]trace.Span),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}

	go box.run(rawNode, storage, tickInterval)
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

	select {
	case <-b.doneCh:
		endRequestTrace(span, "stopped_before_enqueue")
		return errConsensusBoxStopped
	case b.proposals <- request:
		addRequestTraceEvent(span, "proposal_enqueued")
	}

	select {
	case <-b.doneCh:
		return errConsensusBoxStopped
	case err := <-result:
		return err
	}
}

func (b *raftConsensusBox) HandleMessage(message raftpb.Message) error {
	select {
	case <-b.doneCh:
		return errConsensusBoxStopped
	case b.steps <- message:
		return nil
	}
}

func (b *raftConsensusBox) Stop() {
	b.stopOnce.Do(func() {
		close(b.stopCh)
		<-b.doneCh
	})
}

func (b *raftConsensusBox) run(rawNode *raft.RawNode, storage *raft.MemoryStorage, tickInterval time.Duration) {
	defer b.endOpenRequestTraces("box_stopped")
	defer close(b.doneCh)

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	b.drainReady(rawNode, storage)

	for {
		iterationSpan := b.startRunIterationTrace()
		select {
		case <-b.stopCh:
			b.annotateRunIterationTrace(iterationSpan, "stop")
			iterationSpan.End()
			return
		case <-ticker.C:
			b.annotateRunIterationTrace(iterationSpan, "tick")
			rawNode.Tick()
			b.drainReady(rawNode, storage)
			iterationSpan.End()
		case request := <-b.proposals:
			b.annotateRunIterationTrace(iterationSpan, "proposal")
			b.rememberRequestTrace(request.entry, request.span)
			data, err := json.Marshal(request.entry)
			if err == nil {
				addRequestTraceEvent(request.span, "propose")
				err = rawNode.Propose(data)
				if err != nil {
					endRequestTrace(request.span, "propose_error")
					b.forgetRequestTrace(request.entry)
				}
				b.drainReady(rawNode, storage)
			} else {
				endRequestTrace(request.span, "marshal_error")
				b.forgetRequestTrace(request.entry)
			}
			request.result <- err
			iterationSpan.End()
		case message := <-b.steps:
			b.annotateRunIterationTrace(
				iterationSpan,
				"step",
				attribute.String("raft.message.type", message.Type.String()),
				attribute.Int64("raft.message.from", int64(message.From)),
				attribute.Int64("raft.message.to", int64(message.To)),
				attribute.Int64("raft.message.term", int64(message.Term)),
			)
			if err := rawNode.Step(message); err == nil {
				b.drainReady(rawNode, storage)
			}
			iterationSpan.End()
		case unreachable := <-b.unreachable:
			b.annotateRunIterationTrace(
				iterationSpan,
				"unreachable",
				attribute.Int64("raft.unreachable_peer_id", int64(unreachable.id)),
			)
			rawNode.ReportUnreachable(unreachable.id)
			b.drainReady(rawNode, storage)
			iterationSpan.End()
		}
	}
}

func (b *raftConsensusBox) drainReady(rawNode *raft.RawNode, storage *raft.MemoryStorage) {
	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"eo.raft.drain_ready",
		trace.WithAttributes(
			attribute.String("node.name", b.name),
			attribute.Int64("raft.node_id", int64(b.selfID)),
			attribute.Int64("raft.leader_id", int64(b.leaderID.Load())),
		),
	)
	stats := drainReadyStats{}
	defer func() {
		span.SetAttributes(
			attribute.Int("raft.ready.batches", stats.readyBatches),
			attribute.Int("raft.ready.entries", stats.entries),
			attribute.Int("raft.ready.committed_entries", stats.committedEntries),
			attribute.Int("raft.ready.messages", stats.messages),
			attribute.Int("raft.ready.self_messages", stats.selfMessages),
			attribute.Int("raft.ready.sent_messages", stats.sentMessages),
			attribute.Int("raft.ready.unreachable_peers", stats.unreachablePeers),
			attribute.Int("raft.ready.conf_changes", stats.confChanges),
			attribute.Int("raft.ready.normal_entries", stats.normalEntries),
			attribute.Int("raft.ready.snapshots", stats.snapshots),
			attribute.Int("raft.ready.hard_states", stats.hardStates),
			attribute.Int("raft.ready.leader_transitions", stats.leaderTransitions),
		)
		span.End()
	}()

	for rawNode.HasReady() {
		ready := rawNode.Ready()
		stats.readyBatches++
		stats.entries += len(ready.Entries)
		stats.committedEntries += len(ready.CommittedEntries)
		stats.messages += len(ready.Messages)

		_, readySpan := telemetry.Tracer("aegean").Start(
			context.Background(),
			"eo.raft.ready_batch",
			trace.WithAttributes(
				attribute.String("node.name", b.name),
				attribute.Int64("raft.node_id", int64(b.selfID)),
				attribute.Int("raft.ready.batch", stats.readyBatches),
				attribute.Int("raft.ready.entries", len(ready.Entries)),
				attribute.Int("raft.ready.committed_entries", len(ready.CommittedEntries)),
				attribute.Int("raft.ready.messages", len(ready.Messages)),
			),
		)

		if ready.SoftState != nil {
			b.leaderID.Store(ready.SoftState.Lead)
			stats.leaderTransitions++
			readySpan.SetAttributes(attribute.Int64("raft.leader_id", int64(ready.SoftState.Lead)))
		}
		if !raft.IsEmptySnap(ready.Snapshot) {
			stats.snapshots++
			_ = storage.ApplySnapshot(ready.Snapshot)
		}
		if !raft.IsEmptyHardState(ready.HardState) {
			stats.hardStates++
			_ = storage.SetHardState(ready.HardState)
		}
		if len(ready.Entries) > 0 {
			_ = storage.Append(ready.Entries)
		}

		for _, message := range ready.Messages {
			if message.To == b.selfID {
				stats.selfMessages++
				_ = rawNode.Step(message)
				continue
			}
			peer, ok := b.peers[message.To]
			if !ok {
				continue
			}
			b.sendAsync(peer, message)
			stats.sentMessages++
		}

		for _, entry := range ready.CommittedEntries {
			b.learnCommittedEntry(rawNode, entry, &stats)
		}

		rawNode.Advance(ready)
		readySpan.End()
	}
}

func (b *raftConsensusBox) sendAsync(peer string, message raftpb.Message) {
	to := message.To
	go func() {
		if err := b.send(peer, message); err != nil {
			select {
			case b.unreachable <- unreachablePeer{id: to}:
			case <-b.doneCh:
			default:
			}
		}
	}()
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
		if err := json.Unmarshal(entry.Data, &value); err != nil {
			return
		}
		b.learnedSlot++
		b.finishCommittedRequestTrace(b.learnedSlot, value)
		if b.learn != nil {
			b.learn(b.learnedSlot, value)
		}
	}
}

func (b *raftConsensusBox) startRequestTrace(entry Entry) trace.Span {
	if entry.RequestID == "" {
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
	if entry.RequestID == "" {
		return
	}
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
}

func (b *raftConsensusBox) startRunIterationTrace() trace.Span {
	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"eo.raft.run_iteration",
		trace.WithAttributes(
			attribute.String("node.name", b.name),
			attribute.Int64("raft.node_id", int64(b.selfID)),
			attribute.Int64("raft.leader_id", int64(b.leaderID.Load())),
			attribute.Int("eo.raft.proposals_queued", len(b.proposals)),
			attribute.Int("eo.raft.steps_queued", len(b.steps)),
		),
	)
	return span
}

func (b *raftConsensusBox) annotateRunIterationTrace(span trace.Span, event string, attrs ...attribute.KeyValue) {
	if span == nil {
		return
	}
	attrs = append(attrs, attribute.String("eo.raft.run_event", event))
	span.SetAttributes(attrs...)
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
