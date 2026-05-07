package pbeo

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

var errConsensusBoxStopped = errors.New("pbeo consensus box stopped")

type proposalRequest struct {
	entry      Entry
	result     chan error
	span       trace.Span
	callSpan   trace.Span
	enqueuedAt time.Time
	payloadLen int
}

type stepRequest struct {
	message    raftpb.Message
	span       trace.Span
	enqueuedAt time.Time
}

type drainReadyStats struct {
	readyBatches      int
	entries           int
	entryBytes        int
	committedEntries  int
	committedBytes    int
	messages          int
	messageBytes      int
	selfMessages      int
	sentMessages      int
	sentMessageBytes  int
	unreachablePeers  int
	confChanges       int
	normalEntries     int
	snapshots         int
	hardStates        int
	leaderTransitions int
}

type drainReadySection struct {
	name       string
	batch      int
	span       trace.Span
	started    time.Time
	total      time.Duration
	iterations int
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

	proposals       chan proposalRequest
	prioritySteps   chan stepRequest
	backgroundSteps chan stepRequest
	spans           map[string]trace.Span
	stopOnce        sync.Once
	stopCh          chan struct{}
	doneCh          chan struct{}
}

func newRaftConsensusBox(cfg BoxConfig, onLearn LearnFunc) (ConsensusBox, error) {
	peerIDs, peers, selfID, err := buildPeerIDs(cfg.Name, cfg.Peers)
	if err != nil {
		return nil, err
	}
	if cfg.SendRaft == nil {
		return nil, fmt.Errorf("pbeo raft consensus box requires a SendRaft callback")
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
		name:            cfg.Name,
		selfID:          selfID,
		peerIDs:         peerIDs,
		peers:           peers,
		send:            cfg.SendRaft,
		learn:           onLearn,
		proposals:       make(chan proposalRequest),
		prioritySteps:   make(chan stepRequest, 1024),
		backgroundSteps: make(chan stepRequest, 1024),
		spans:           make(map[string]trace.Span),
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
	}

	go box.run(rawNode, storage, tickInterval)
	return box, nil
}

func (b *raftConsensusBox) IsLeader() bool {
	return b.leaderID.Load() == b.selfID && b.selfID != 0
}

func (b *raftConsensusBox) Leader() (string, bool) {
	leaderID := b.leaderID.Load()
	if leaderID == 0 {
		return "", false
	}
	peer, ok := b.peers[leaderID]
	return peer, ok
}

func (b *raftConsensusBox) Propose(entry Entry) error {
	result := make(chan error, 1)
	span := b.startRequestTrace(entry)
	_, callSpan := telemetry.StartSpanFromPayload(
		entry.Response,
		"pbeo.raft.propose_call",
		append(
			b.entryTraceAttrs(entry),
			attribute.Int("pbeo.raft.proposals_queued_on_call", len(b.proposals)),
		)...,
	)
	payloadLen := entryWireSize(entry)
	request := proposalRequest{
		entry:      entry,
		result:     result,
		span:       span,
		callSpan:   callSpan,
		enqueuedAt: time.Now(),
		payloadLen: payloadLen,
	}
	if span != nil {
		span.SetAttributes(
			attribute.Int("pbeo.raft.proposal_queue_depth_on_enqueue", len(b.proposals)),
			attribute.Int("pbeo.raft.proposal_payload_bytes", payloadLen),
		)
	}
	callSpan.SetAttributes(
		attribute.Int("pbeo.raft.proposal_queue_depth_on_enqueue", len(b.proposals)),
		attribute.Int("pbeo.raft.proposal_payload_bytes", payloadLen),
	)

	_, sendSpan := telemetry.StartSpanFromPayload(
		entry.Response,
		"pbeo.raft.propose_channel_send",
		append(
			b.entryTraceAttrs(entry),
			attribute.Int("pbeo.raft.proposal_queue_depth_before_send", len(b.proposals)),
			attribute.Int("pbeo.raft.proposal_payload_bytes", payloadLen),
		)...,
	)
	select {
	case <-b.doneCh:
		sendSpan.SetAttributes(attribute.String("pbeo.raft.propose_channel_send_result", "stopped_before_enqueue"))
		sendSpan.End()
		callSpan.SetAttributes(attribute.String("pbeo.raft.propose_result", "stopped_before_enqueue"))
		callSpan.End()
		endRequestTrace(span, "stopped_before_enqueue")
		return errConsensusBoxStopped
	case b.proposals <- request:
		addRequestTraceEvent(span, "proposal_enqueued")
		if span != nil {
			span.SetAttributes(attribute.Int("pbeo.raft.proposal_queue_depth_after_enqueue", len(b.proposals)))
		}
		sendSpan.SetAttributes(
			attribute.String("pbeo.raft.propose_channel_send_result", "enqueued"),
			attribute.Int("pbeo.raft.proposal_queue_depth_after_send", len(b.proposals)),
		)
		sendSpan.End()
		callSpan.SetAttributes(attribute.Int("pbeo.raft.proposal_queue_depth_after_enqueue", len(b.proposals)))
	}

	_, waitSpan := telemetry.StartSpanFromPayload(
		entry.Response,
		"pbeo.raft.propose_result_wait",
		append(
			b.entryTraceAttrs(entry),
			attribute.Int("pbeo.raft.proposal_queue_depth_after_enqueue", len(b.proposals)),
			attribute.Int("pbeo.raft.proposal_payload_bytes", payloadLen),
		)...,
	)
	select {
	case <-b.doneCh:
		waitSpan.SetAttributes(attribute.String("pbeo.raft.propose_result", "stopped"))
		waitSpan.End()
		callSpan.SetAttributes(attribute.String("pbeo.raft.propose_result", "stopped"))
		callSpan.End()
		return errConsensusBoxStopped
	case err := <-result:
		if err != nil {
			waitSpan.RecordError(err)
			callSpan.RecordError(err)
			waitSpan.SetAttributes(attribute.String("pbeo.raft.propose_result", "error"))
			callSpan.SetAttributes(attribute.String("pbeo.raft.propose_result", "error"))
		} else {
			waitSpan.SetAttributes(attribute.String("pbeo.raft.propose_result", "ok"))
			callSpan.SetAttributes(attribute.String("pbeo.raft.propose_result", "ok"))
		}
		waitSpan.End()
		callSpan.End()
		return err
	}
}

func (b *raftConsensusBox) HandleMessage(message raftpb.Message) error {
	span := b.startStepQueueTrace(message)
	request := stepRequest{
		message:    message,
		span:       span,
		enqueuedAt: time.Now(),
	}
	queue := b.stepQueue(message)
	select {
	case <-b.doneCh:
		if span != nil {
			span.SetAttributes(attribute.String("pbeo.raft.step_queue_result", "stopped_before_enqueue"))
			span.End()
		}
		return errConsensusBoxStopped
	default:
	}

	if message.Type == raftpb.MsgHeartbeatResp {
		select {
		case <-b.doneCh:
			if span != nil {
				span.SetAttributes(attribute.String("pbeo.raft.step_queue_result", "stopped_before_enqueue"))
				span.End()
			}
			return errConsensusBoxStopped
		case queue <- request:
			if span != nil {
				span.SetAttributes(
					attribute.String("pbeo.raft.step_queue_result", "enqueued"),
					attribute.Int("pbeo.raft.steps_queued_after_enqueue", len(queue)),
				)
			}
			return nil
		default:
			if span != nil {
				span.SetAttributes(attribute.String("pbeo.raft.step_queue_result", "dropped_background_queue_full"))
				span.End()
			}
			return nil
		}
	}

	select {
	case <-b.doneCh:
		if span != nil {
			span.SetAttributes(attribute.String("pbeo.raft.step_queue_result", "stopped_before_enqueue"))
			span.End()
		}
		return errConsensusBoxStopped
	case queue <- request:
		if span != nil {
			span.SetAttributes(
				attribute.String("pbeo.raft.step_queue_result", "enqueued"),
				attribute.Int("pbeo.raft.steps_queued_after_enqueue", len(queue)),
			)
		}
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
		select {
		case request := <-b.prioritySteps:
			iterationSpan := b.startRunIterationTrace()
			b.processStep(rawNode, storage, request, iterationSpan, "step_priority")
			iterationSpan.End()
			continue
		default:
		}

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
			proposalQueueWait := time.Since(request.enqueuedAt)
			_, proposalIterationSpan := telemetry.StartSpanFromPayload(
				request.entry.Response,
				"pbeo.raft.proposal_iteration",
				append(
					b.entryTraceAttrs(request.entry),
					attribute.Int64("pbeo.raft.proposal_queue_wait_us", proposalQueueWait.Microseconds()),
					attribute.Int("pbeo.raft.proposal_queue_depth_on_dequeue", len(b.proposals)),
					attribute.Int("pbeo.raft.proposal_payload_bytes", request.payloadLen),
				)...,
			)
			b.annotateRunIterationTrace(
				iterationSpan,
				"proposal",
				attribute.Int64("pbeo.raft.proposal_queue_wait_us", proposalQueueWait.Microseconds()),
				attribute.Int("pbeo.raft.proposal_payload_bytes", request.payloadLen),
			)
			if request.span != nil {
				request.span.SetAttributes(
					attribute.Int64("pbeo.raft.proposal_queue_wait_us", proposalQueueWait.Microseconds()),
					attribute.Int("pbeo.raft.proposal_queue_depth_on_dequeue", len(b.proposals)),
				)
			}
			if request.callSpan != nil {
				request.callSpan.SetAttributes(
					attribute.Int64("pbeo.raft.proposal_queue_wait_us", proposalQueueWait.Microseconds()),
					attribute.Int("pbeo.raft.proposal_queue_depth_on_dequeue", len(b.proposals)),
				)
			}
			b.rememberRequestTrace(request.entry, request.span)
			_, marshalSpan := telemetry.StartSpanFromPayload(
				request.entry.Response,
				"pbeo.raft.proposal_marshal",
				append(
					b.entryTraceAttrs(request.entry),
					attribute.Int("pbeo.raft.proposal_payload_bytes_estimate", request.payloadLen),
				)...,
			)
			data, err := json.Marshal(request.entry)
			if err != nil {
				marshalSpan.RecordError(err)
			}
			marshalSpan.SetAttributes(attribute.Int("pbeo.raft.proposal_marshal_bytes", len(data)))
			marshalSpan.End()
			if err == nil {
				if request.span != nil {
					request.span.SetAttributes(attribute.Int("pbeo.raft.proposal_marshal_bytes", len(data)))
				}
				addRequestTraceEvent(request.span, "propose")
				_, rawProposeSpan := telemetry.StartSpanFromPayload(
					request.entry.Response,
					"pbeo.raft.raw_node_propose",
					append(
						b.entryTraceAttrs(request.entry),
						attribute.Int("pbeo.raft.proposal_marshal_bytes", len(data)),
					)...,
				)
				err = rawNode.Propose(data)
				if err != nil {
					rawProposeSpan.RecordError(err)
					endRequestTrace(request.span, "propose_error")
					b.forgetRequestTrace(request.entry)
				}
				rawProposeSpan.End()
				_, drainSpan := telemetry.StartSpanFromPayload(
					request.entry.Response,
					"pbeo.raft.proposal_drain_ready",
					append(
						b.entryTraceAttrs(request.entry),
						attribute.Int("pbeo.raft.proposal_marshal_bytes", len(data)),
					)...,
				)
				b.drainReady(rawNode, storage)
				drainSpan.End()
			} else {
				endRequestTrace(request.span, "marshal_error")
				b.forgetRequestTrace(request.entry)
			}
			if err != nil {
				proposalIterationSpan.RecordError(err)
			}
			_, resultSendSpan := telemetry.StartSpanFromPayload(
				request.entry.Response,
				"pbeo.raft.proposal_result_send",
				append(
					b.entryTraceAttrs(request.entry),
					attribute.Int("pbeo.raft.proposal_queue_depth_before_result_send", len(b.proposals)),
				)...,
			)
			request.result <- err
			if err != nil {
				resultSendSpan.RecordError(err)
			}
			resultSendSpan.SetAttributes(attribute.Int("pbeo.raft.proposal_queue_depth_after_result_send", len(b.proposals)))
			resultSendSpan.End()
			proposalIterationSpan.SetAttributes(attribute.Int("pbeo.raft.proposal_queue_depth_after_iteration", len(b.proposals)))
			proposalIterationSpan.End()
			iterationSpan.End()
		case request := <-b.prioritySteps:
			b.processStep(rawNode, storage, request, iterationSpan, "step_priority")
			iterationSpan.End()
		case request := <-b.backgroundSteps:
			b.processStep(rawNode, storage, request, iterationSpan, "step_background")
			iterationSpan.End()
		}
	}
}

func (b *raftConsensusBox) processStep(rawNode *raft.RawNode, storage *raft.MemoryStorage, request stepRequest, iterationSpan trace.Span, event string) {
	message := request.message
	stepQueueWait := time.Since(request.enqueuedAt)
	if request.span != nil {
		request.span.SetAttributes(
			attribute.Int64("pbeo.raft.step_queue_wait_us", stepQueueWait.Microseconds()),
			attribute.Int("pbeo.raft.priority_steps_queued_on_dequeue", len(b.prioritySteps)),
			attribute.Int("pbeo.raft.background_steps_queued_on_dequeue", len(b.backgroundSteps)),
		)
	}
	b.annotateRunIterationTrace(
		iterationSpan,
		event,
		attribute.String("raft.message.type", message.Type.String()),
		attribute.Int64("raft.message.from", int64(message.From)),
		attribute.Int64("raft.message.to", int64(message.To)),
		attribute.Int64("raft.message.term", int64(message.Term)),
		attribute.Int("raft.message.bytes", message.Size()),
		attribute.Int64("pbeo.raft.step_queue_wait_us", stepQueueWait.Microseconds()),
	)
	if err := rawNode.Step(message); err == nil {
		b.drainReady(rawNode, storage)
		if request.span != nil {
			request.span.SetAttributes(attribute.String("pbeo.raft.step_result", "ok"))
		}
	} else if request.span != nil {
		request.span.RecordError(err)
		request.span.SetAttributes(attribute.String("pbeo.raft.step_result", "error"))
	}
	if request.span != nil {
		request.span.End()
	}
}

func (b *raftConsensusBox) drainReady(rawNode *raft.RawNode, storage *raft.MemoryStorage) {
	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"pbeo.raft.drain_ready",
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
			attribute.Int("raft.ready.entry_bytes", stats.entryBytes),
			attribute.Int("raft.ready.committed_entries", stats.committedEntries),
			attribute.Int("raft.ready.committed_bytes", stats.committedBytes),
			attribute.Int("raft.ready.messages", stats.messages),
			attribute.Int("raft.ready.message_bytes", stats.messageBytes),
			attribute.Int("raft.ready.self_messages", stats.selfMessages),
			attribute.Int("raft.ready.sent_messages", stats.sentMessages),
			attribute.Int("raft.ready.sent_message_bytes", stats.sentMessageBytes),
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
		entryBytes := entriesWireSize(ready.Entries)
		committedBytes := entriesWireSize(ready.CommittedEntries)
		messageBytes := messagesWireSize(ready.Messages)
		stats.entryBytes += entryBytes
		stats.committedBytes += committedBytes
		stats.messageBytes += messageBytes

		_, readySpan := telemetry.Tracer("aegean").Start(
			context.Background(),
			"pbeo.raft.ready_batch",
			trace.WithAttributes(
				attribute.String("node.name", b.name),
				attribute.Int64("raft.node_id", int64(b.selfID)),
				attribute.Int("raft.ready.batch", stats.readyBatches),
				attribute.Int("raft.ready.entries", len(ready.Entries)),
				attribute.Int("raft.ready.entry_bytes", entryBytes),
				attribute.Int("raft.ready.committed_entries", len(ready.CommittedEntries)),
				attribute.Int("raft.ready.committed_bytes", committedBytes),
				attribute.Int("raft.ready.messages", len(ready.Messages)),
				attribute.Int("raft.ready.message_bytes", messageBytes),
			),
		)
		b.annotateReadyWithStatus(rawNode, readySpan)

		if ready.SoftState != nil {
			b.leaderID.Store(ready.SoftState.Lead)
			stats.leaderTransitions++
			readySpan.SetAttributes(attribute.Int64("raft.leader_id", int64(ready.SoftState.Lead)))
		}
		storageSection := b.startDrainReadySection("storage", stats.readyBatches)
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
		storageSection.End()

		sendSection := b.startDrainReadySection("send_messages", stats.readyBatches)
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
			messageBytes := message.Size()
			_, messageSpan := telemetry.Tracer("aegean").Start(
				context.Background(),
				"pbeo.raft.send_message",
				trace.WithAttributes(
					attribute.String("node.name", b.name),
					attribute.Int64("raft.node_id", int64(b.selfID)),
					attribute.String("raft.peer", peer),
					attribute.Int64("raft.message.to", int64(message.To)),
					attribute.Int64("raft.message.from", int64(message.From)),
					attribute.Int64("raft.message.term", int64(message.Term)),
					attribute.String("raft.message.type", message.Type.String()),
					attribute.Int("raft.message.entries", len(message.Entries)),
					attribute.Int("raft.message.bytes", messageBytes),
				),
			)
			if err := b.send(peer, message); err != nil {
				stats.unreachablePeers++
				messageSpan.RecordError(err)
				messageSpan.SetAttributes(attribute.Bool("pbeo.raft.send_error", true))
				messageSpan.End()
				rawNode.ReportUnreachable(message.To)
				continue
			}
			stats.sentMessages++
			stats.sentMessageBytes += messageBytes
			messageSpan.End()
		}
		sendSection.SetAttributes(
			attribute.Int("raft.ready.messages", len(ready.Messages)),
			attribute.Int("raft.ready.sent_messages", stats.sentMessages),
			attribute.Int("raft.ready.sent_message_bytes", stats.sentMessageBytes),
			attribute.Int("raft.ready.self_messages", stats.selfMessages),
			attribute.Int("raft.ready.unreachable_peers", stats.unreachablePeers),
		)
		sendSection.End()

		learnSection := b.startDrainReadySection("learn_committed", stats.readyBatches)
		for _, entry := range ready.CommittedEntries {
			b.learnCommittedEntry(rawNode, entry, &stats)
		}
		learnSection.SetAttributes(
			attribute.Int("raft.ready.committed_entries", len(ready.CommittedEntries)),
			attribute.Int("raft.ready.normal_entries", stats.normalEntries),
			attribute.Int("raft.ready.conf_changes", stats.confChanges),
		)
		learnSection.End()

		advanceSection := b.startDrainReadySection("advance", stats.readyBatches)
		rawNode.Advance(ready)
		advanceSection.End()
		readySpan.End()
	}
}

func (b *raftConsensusBox) startDrainReadySection(section string, batch int) trace.Span {
	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"pbeo.raft.drain_ready.section",
		trace.WithAttributes(
			attribute.String("node.name", b.name),
			attribute.Int64("raft.node_id", int64(b.selfID)),
			attribute.Int("raft.ready.batch", batch),
			attribute.String("pbeo.raft.drain_ready.section", section),
		),
	)
	return span
}

func (b *raftConsensusBox) annotateReadyWithStatus(rawNode *raft.RawNode, span trace.Span) {
	if span == nil {
		return
	}
	status := rawNode.Status()
	attrs := []attribute.KeyValue{
		attribute.Int64("raft.status.term", int64(status.Term)),
		attribute.Int64("raft.status.commit", int64(status.Commit)),
		attribute.Int64("raft.status.applied", int64(status.Applied)),
		attribute.String("raft.status.state", status.RaftState.String()),
	}
	maxMatchLag := uint64(0)
	maxNextLag := uint64(0)
	inflightFullPeers := 0
	for id, progress := range status.Progress {
		if id == b.selfID {
			continue
		}
		peer := b.peers[id]
		if peer == "" {
			peer = fmt.Sprintf("%d", id)
		}
		matchLag := status.Commit - minUint64(status.Commit, progress.Match)
		nextLag := status.Commit - minUint64(status.Commit, progress.Next)
		if matchLag > maxMatchLag {
			maxMatchLag = matchLag
		}
		if nextLag > maxNextLag {
			maxNextLag = nextLag
		}
		inflightCount := 0
		inflightFull := false
		if progress.Inflights != nil {
			inflightCount = progress.Inflights.Count()
			inflightFull = progress.Inflights.Full()
		}
		if inflightFull {
			inflightFullPeers++
		}
		prefix := "raft.progress." + peer
		attrs = append(attrs,
			attribute.Int64(prefix+".match", int64(progress.Match)),
			attribute.Int64(prefix+".next", int64(progress.Next)),
			attribute.String(prefix+".state", progress.State.String()),
			attribute.Bool(prefix+".recent_active", progress.RecentActive),
			attribute.Bool(prefix+".flow_paused", progress.MsgAppFlowPaused),
			attribute.Int(prefix+".inflights", inflightCount),
			attribute.Bool(prefix+".inflights_full", inflightFull),
			attribute.Int64(prefix+".match_lag", int64(matchLag)),
			attribute.Int64(prefix+".next_lag", int64(nextLag)),
		)
	}
	attrs = append(attrs,
		attribute.Int64("raft.progress.max_match_lag", int64(maxMatchLag)),
		attribute.Int64("raft.progress.max_next_lag", int64(maxNextLag)),
		attribute.Int("raft.progress.inflight_full_peers", inflightFullPeers),
	)
	span.SetAttributes(attrs...)
}

func (b *raftConsensusBox) startStepQueueTrace(message raftpb.Message) trace.Span {
	queueName := "priority"
	queueDepth := len(b.prioritySteps)
	if !isPriorityStep(message.Type) {
		queueName = "background"
		queueDepth = len(b.backgroundSteps)
	}
	_, span := telemetry.Tracer("aegean").Start(
		context.Background(),
		"pbeo.raft.step_queue_wait",
		trace.WithAttributes(
			attribute.String("node.name", b.name),
			attribute.Int64("raft.node_id", int64(b.selfID)),
			attribute.Int64("raft.leader_id", int64(b.leaderID.Load())),
			attribute.String("raft.message.type", message.Type.String()),
			attribute.Int64("raft.message.from", int64(message.From)),
			attribute.Int64("raft.message.to", int64(message.To)),
			attribute.Int64("raft.message.term", int64(message.Term)),
			attribute.Int("raft.message.entries", len(message.Entries)),
			attribute.Int("raft.message.bytes", message.Size()),
			attribute.String("pbeo.raft.step_queue", queueName),
			attribute.Int("pbeo.raft.steps_queued_on_enqueue", queueDepth),
			attribute.Int("pbeo.raft.priority_steps_queued_on_enqueue", len(b.prioritySteps)),
			attribute.Int("pbeo.raft.background_steps_queued_on_enqueue", len(b.backgroundSteps)),
		),
	)
	return span
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
		"pbeo.raft.request_to_commit",
		append(
			b.entryTraceAttrs(entry),
			attribute.String("pbeo.raft.trace_scope", "local_propose"),
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
			"pbeo.raft.request_to_commit",
			append(
				b.entryTraceAttrs(entry),
				attribute.Int64("pbeo.raft.commit_slot", int64(slot)),
				attribute.String("pbeo.raft.trace_scope", "commit_observed"),
			)...,
		)
		endRequestTrace(span, "commit_observed")
		return
	}
	delete(b.spans, entry.RequestID)
	span.SetAttributes(
		attribute.Int64("pbeo.raft.commit_slot", int64(slot)),
		attribute.String("pbeo.raft.trace_scope", "local_propose"),
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
		"pbeo.raft.run_iteration",
		trace.WithAttributes(
			attribute.String("node.name", b.name),
			attribute.Int64("raft.node_id", int64(b.selfID)),
			attribute.Int64("raft.leader_id", int64(b.leaderID.Load())),
			attribute.Int("pbeo.raft.proposals_queued", len(b.proposals)),
			attribute.Int("pbeo.raft.priority_steps_queued", len(b.prioritySteps)),
			attribute.Int("pbeo.raft.background_steps_queued", len(b.backgroundSteps)),
			attribute.Int("pbeo.raft.steps_queued", len(b.prioritySteps)+len(b.backgroundSteps)),
		),
	)
	return span
}

func (b *raftConsensusBox) annotateRunIterationTrace(span trace.Span, event string, attrs ...attribute.KeyValue) {
	if span == nil {
		return
	}
	attrs = append(attrs, attribute.String("pbeo.raft.run_event", event))
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

func entryWireSize(entry Entry) int {
	data, err := json.Marshal(entry)
	if err != nil {
		return 0
	}
	return len(data)
}

func entriesWireSize(entries []raftpb.Entry) int {
	total := 0
	for _, entry := range entries {
		total += entry.Size()
	}
	return total
}

func messagesWireSize(messages []raftpb.Message) int {
	total := 0
	for _, message := range messages {
		total += message.Size()
	}
	return total
}

func minUint64(a uint64, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
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
		return nil, nil, 0, fmt.Errorf("pbeo raft consensus box requires a node name")
	}
	if len(peers) == 0 {
		return nil, nil, 0, fmt.Errorf("pbeo raft consensus box requires peers")
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
