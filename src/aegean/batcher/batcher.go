package batcher

import (
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
	seqNum         int
	mu             sync.Mutex
	batchStartTime time.Time
	requestSpans   map[string]trace.Span
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
	}
	return b
}

func (b *Batcher) StartBatchFlusher() {
	go b.batchFlusher()
}

func (b *Batcher) batchFlusher() {
	for {
		time.Sleep(b.batchTimeout)
		b.mu.Lock()
		// Flush on timeout if there are pending requests
		if len(b.batch) > 0 && !b.batchStartTime.IsZero() && time.Since(b.batchStartTime) >= b.batchTimeout {
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
