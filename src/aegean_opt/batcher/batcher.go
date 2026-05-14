package batcher

import (
	"aegean/aegean_opt/inlog"
	"aegean/aegean_opt/protocol"
	"fmt"
	"sync"
	"time"

	"aegean/common"
	netx "aegean/net"
	"aegean/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Batcher groups client requests into ordered batches as described in Eve's execution stage
// It assigns a sequence number to each batch and attaches nondeterminism data
type Batcher struct {
	Name      string
	NextCh    chan<- map[string]any
	Execs     []string
	isPrimary bool
	// Accumulates incoming client requests until flushed
	batch        []map[string]any
	batchSize    int
	batchTimeout time.Duration
	// Monotonic batch sequence number
	seqNum            int
	mu                sync.Mutex
	batchStartTime    time.Time
	requestSpans      map[string]trace.Span
	useInLog          bool
	inLog             *inlog.InLog
	inLogSeqNum       int
	inLogReqIdx       int
	inLogCount        int
	inLogRequestIDs   []string
	inLogNDSeeds      []int64
	inLogNDTimestamps []float64
	seenRequests      map[string]struct{}
}

func NewBatcher(name string, nextCh chan<- map[string]any, execs []string, isPrimary bool, runConfig map[string]any) *Batcher {
	if nextCh == nil {
		panic("batcher component requires non-nil nextCh")
	}
	b := &Batcher{
		Name:         name,
		NextCh:       nextCh,
		Execs:        execs,
		isPrimary:    isPrimary,
		batch:        []map[string]any{},
		batchSize:    common.MustInt(runConfig, "batch_size"),
		batchTimeout: time.Duration(common.MustInt(runConfig, "batch_timeout_ms")) * time.Millisecond,
		requestSpans: make(map[string]trace.Span),
		seenRequests: make(map[string]struct{}),
		inLogSeqNum:  1,
	}
	if common.BoolOrDefault(runConfig, "inlog_enabled", false) {
		b.useInLog = true
		component, err := inlog.New(inlog.Config{
			Name:  name,
			Peers: execs,
			SendRaft: func(peer string, payload map[string]any) error {
				_, err := netx.SendMessage(peer, 8000, payload)
				return err
			},
			Commit: func(slot uint64, entry inlog.Entry) {
				b.applyInLogEntry(slot, entry)
			},
			TickInterval:      time.Duration(common.IntOrDefault(runConfig, "inlog_tick_interval_ms", 10)) * time.Millisecond,
			ElectionTick:      common.IntOrDefault(runConfig, "inlog_election_tick", 10),
			HeartbeatTick:     common.IntOrDefault(runConfig, "inlog_heartbeat_tick", 1),
			CampaignOnStart:   isPrimary,
			CampaignRetryTick: common.IntOrDefault(runConfig, "inlog_campaign_retry_tick", 100),
		})
		if err != nil {
			panic(err)
		}
		b.inLog = component
	}
	return b
}

func (b *Batcher) InLogReady() bool {
	if !b.useInLog {
		return true
	}
	if !b.isPrimary {
		return true
	}
	return b.inLog != nil && b.inLog.IsLeader()
}

func (b *Batcher) StartBatchFlusher() {
	go b.batchFlusher()
}

func (b *Batcher) batchFlusher() {
	for {
		time.Sleep(b.batchTimeout)
		b.mu.Lock()
		// Flush on timeout if there are pending requests
		if b.useInLog {
			if b.inLogCount > 0 && !b.batchStartTime.IsZero() && time.Since(b.batchStartTime) >= b.batchTimeout {
				_ = b.closeInLogWindowLocked()
			}
		} else if len(b.batch) > 0 && !b.batchStartTime.IsZero() && time.Since(b.batchStartTime) >= b.batchTimeout {
			b.flushBatchLocked()
		}
		b.mu.Unlock()
	}
}

func (b *Batcher) flushBatchLocked() {
	if len(b.batch) == 0 {
		return
	}
	if !b.isPrimary {
		b.batch = []map[string]any{}
		b.batchStartTime = time.Time{}
		return
	}

	batch := b.batch
	b.batch = []map[string]any{}
	b.seqNum++
	b.batchStartTime = time.Time{}
	for _, request := range batch {
		b.endRequestBatchWaitLocked(request)
	}

	// Attach nondeterminism data for consistent execution across replicas
	message := map[string]any{
		"type":         "batch",
		"seq_num":      b.seqNum,
		"requests":     batch,
		"nd_seed":      time.Now().UnixMilli(),
		"nd_timestamp": float64(time.Now().UnixNano()) / 1e9,
	}
	if len(batch) > 0 {
		telemetry.CopyContext(message, batch[0])
	}

	for _, execNode := range b.Execs {
		if execNode == b.Name && b.NextCh != nil {
			b.NextCh <- message
			continue
		}
		_, _ = netx.SendMessage(execNode, 8000, message)
	}
}

func (b *Batcher) HandleRequestMessage(payload map[string]any) map[string]any {
	if b.useInLog {
		return b.handleInLogRequest(payload)
	}

	// TODO: should we forward to primary + also check if primary is live?
	if !b.isPrimary {
		return map[string]any{"status": "ignored_non_primary"}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.batch) == 0 {
		b.batchStartTime = time.Now()
	}
	b.startRequestBatchWaitLocked(payload)
	b.batch = append(b.batch, payload)
	if len(b.batch) >= b.batchSize {
		b.flushBatchLocked()
	}

	return map[string]any{"status": "batched"}
}

func (b *Batcher) handleInLogRequest(payload map[string]any) map[string]any {
	if !b.isPrimary {
		return map[string]any{"status": "ignored_non_primary"}
	}
	if b.inLog == nil {
		return map[string]any{"status": "error", "error": "inlog not configured"}
	}
	if !b.inLog.IsLeader() {
		leader, _ := b.inLog.Leader()
		return map[string]any{"status": "not_log_leader", "leader": leader}
	}

	requestID, _ := canonicalRequestID(payload[protocol.FieldRequestID])
	b.mu.Lock()
	defer b.mu.Unlock()
	if requestID != "" {
		if _, duplicate := b.seenRequests[requestID]; duplicate {
			return map[string]any{"status": "duplicate_skipped", "request_id": requestID}
		}
	}
	if b.inLogCount >= b.batchSize {
		if err := b.closeInLogWindowLocked(); err != nil {
			return map[string]any{"status": "batch_close_failed", "error": err.Error()}
		}
	}
	if b.inLogCount == 0 {
		b.batchStartTime = time.Now()
	}

	ndSeed := time.Now().UnixNano()
	ndTimestamp := float64(time.Now().UnixNano()) / 1e9
	b.batch = append(b.batch, cloneMap(payload))
	b.inLogRequestIDs = append(b.inLogRequestIDs, requestID)
	b.inLogNDSeeds = append(b.inLogNDSeeds, ndSeed)
	b.inLogNDTimestamps = append(b.inLogNDTimestamps, ndTimestamp)
	if requestID != "" {
		b.seenRequests[requestID] = struct{}{}
	}
	b.inLogCount++
	b.inLogReqIdx++
	if b.inLogCount >= b.batchSize {
		if err := b.closeInLogWindowLocked(); err != nil {
			return map[string]any{"status": "proposed_request_close_failed", "error": err.Error(), "request_id": requestID}
		}
	}
	return map[string]any{"status": "proposed", "request_id": requestID}
}

func (b *Batcher) closeInLogWindowLocked() error {
	if b.inLogCount == 0 {
		return nil
	}
	requests := cloneMapSlice(b.batch)
	requestIDs := append([]string{}, b.inLogRequestIDs...)
	ndSeeds := append([]int64{}, b.inLogNDSeeds...)
	ndTimestamps := append([]float64{}, b.inLogNDTimestamps...)
	entry := inlog.Entry{
		Type:         inlog.EntryTypeRequestBatch,
		SeqNum:       b.inLogSeqNum,
		Count:        b.inLogCount,
		Requests:     requests,
		RequestIDs:   requestIDs,
		NDSeeds:      ndSeeds,
		NDTimestamps: ndTimestamps,
	}
	if err := b.inLog.Propose(entry); err != nil {
		return err
	}
	b.batch = []map[string]any{}
	b.inLogRequestIDs = nil
	b.inLogNDSeeds = nil
	b.inLogNDTimestamps = nil
	b.inLogSeqNum++
	b.inLogReqIdx = 0
	b.inLogCount = 0
	b.batchStartTime = time.Time{}
	return nil
}

func (b *Batcher) HandleInLogRaftMessage(payload map[string]any) map[string]any {
	if b.inLog == nil {
		return map[string]any{"status": "error", "error": "inlog not configured"}
	}
	return b.inLog.HandleRaftMessage(payload)
}

func (b *Batcher) applyInLogEntry(slot uint64, entry inlog.Entry) {
	if b.NextCh == nil {
		return
	}
	switch entry.Type {
	case inlog.EntryTypeRequest:
		message := map[string]any{
			protocol.FieldType:        protocol.MessageTypeInLogRequest,
			"log_slot":                slot,
			protocol.FieldSeqNum:      entry.SeqNum,
			protocol.FieldRequestIdx:  entry.RequestIdx,
			protocol.FieldRequestID:   entry.RequestID,
			protocol.FieldRequest:     cloneMap(entry.Request),
			protocol.FieldNDSeed:      entry.NDSeed,
			protocol.FieldNDTimestamp: entry.NDTimestamp,
		}
		telemetry.CopyContext(message, entry.Request)
		b.NextCh <- message
	case inlog.EntryTypeRequestBatch:
		count := entry.Count
		if count <= 0 || count > len(entry.Requests) {
			count = len(entry.Requests)
		}
		for i := 0; i < count; i++ {
			request := cloneMap(entry.Requests[i])
			requestID := ""
			if i < len(entry.RequestIDs) {
				requestID = entry.RequestIDs[i]
			}
			if requestID == "" {
				requestID, _ = canonicalRequestID(request[protocol.FieldRequestID])
			}
			ndSeed := int64(0)
			if i < len(entry.NDSeeds) {
				ndSeed = entry.NDSeeds[i]
			}
			ndTimestamp := float64(0)
			if i < len(entry.NDTimestamps) {
				ndTimestamp = entry.NDTimestamps[i]
			}
			message := map[string]any{
				protocol.FieldType:        protocol.MessageTypeInLogRequest,
				"log_slot":                slot,
				protocol.FieldSeqNum:      entry.SeqNum,
				protocol.FieldRequestIdx:  i,
				protocol.FieldRequestID:   requestID,
				protocol.FieldRequest:     request,
				protocol.FieldNDSeed:      ndSeed,
				protocol.FieldNDTimestamp: ndTimestamp,
			}
			telemetry.CopyContext(message, request)
			b.NextCh <- message
		}
		b.NextCh <- map[string]any{
			protocol.FieldType:   protocol.MessageTypeInLogBatchFormed,
			"log_slot":           slot,
			protocol.FieldSeqNum: entry.SeqNum,
			protocol.FieldCount:  count,
		}
	case inlog.EntryTypeBatchFormed:
		b.NextCh <- map[string]any{
			protocol.FieldType:   protocol.MessageTypeInLogBatchFormed,
			"log_slot":           slot,
			protocol.FieldSeqNum: entry.SeqNum,
			protocol.FieldCount:  entry.Count,
		}
	}
}

// TODO: allow primaries to rotate, on batcher failures

func (b *Batcher) startRequestBatchWaitLocked(payload map[string]any) {
	requestID, ok := canonicalRequestID(payload["request_id"])
	if !ok {
		return
	}
	if _, exists := b.requestSpans[requestID]; exists {
		return
	}
	_, span := telemetry.StartSpanFromPayload(
		payload,
		"batcher.request_queue_wait",
		append(
			telemetry.AttrsFromPayload(payload),
			attribute.String("node.name", b.Name),
		)...,
	)
	b.requestSpans[requestID] = span
}

func (b *Batcher) endRequestBatchWaitLocked(payload map[string]any) {
	requestID, ok := canonicalRequestID(payload["request_id"])
	if !ok {
		return
	}
	span, exists := b.requestSpans[requestID]
	if !exists {
		return
	}
	delete(b.requestSpans, requestID)
	if span != nil {
		span.End()
	}
}

func canonicalRequestID(id any) (string, bool) {
	if id == nil {
		return "", false
	}
	return fmt.Sprintf("%v", id), true
}

func cloneMap(src map[string]any) map[string]any {
	if src == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

func cloneMapSlice(src []map[string]any) []map[string]any {
	if len(src) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(src))
	for _, item := range src {
		out = append(out, cloneMap(item))
	}
	return out
}
