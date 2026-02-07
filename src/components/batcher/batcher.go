package batcher

import (
	"log"
	"sync"
	"time"

	"aegean/common"
)

// Batcher groups client requests into ordered batches as described in Eve's execution stage
// It assigns a sequence number to each batch and attaches nondeterminism data
type Batcher struct {
	Name      string
	NextCh    chan<- map[string]any
	Execs     []string
	LocalName string
	isPrimary bool
	// Accumulates incoming client requests until flushed
	batch        []map[string]any
	batchSize    int
	batchTimeout time.Duration
	// Monotonic batch sequence number
	seqNum        int
	mu            sync.Mutex
	lastBatchTime time.Time
}

func NewBatcher(name string, nextCh chan<- map[string]any, execs []string, localName string, isPrimary bool) *Batcher {
	if nextCh == nil {
		log.Fatalf("batcher component requires non-nil nextCh")
	}
	if localName == "" {
		log.Fatalf("batcher component requires localName")
	}
	b := &Batcher{
		Name:          name,
		NextCh:        nextCh,
		Execs:         execs,
		LocalName:     localName,
		isPrimary:     isPrimary,
		batch:         []map[string]any{},
		batchSize:     10,
		batchTimeout:  100 * time.Millisecond,
		lastBatchTime: time.Now(),
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
		if len(b.batch) > 0 && time.Since(b.lastBatchTime) >= b.batchTimeout {
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
		b.lastBatchTime = time.Now()
		return
	}

	batch := b.batch
	b.batch = []map[string]any{}
	b.seqNum++
	b.lastBatchTime = time.Now()

	// Attach nondeterminism data for consistent execution across replicas
	message := map[string]any{
		"type":         "batch",
		"seq_num":      b.seqNum,
		"requests":     batch,
		"nd_seed":      time.Now().UnixMilli(),
		"nd_timestamp": float64(time.Now().UnixNano()) / 1e9,
	}

	log.Printf("%s: Created batch %d with %d requests", b.Name, b.seqNum, len(batch))

	for _, execNode := range b.Execs {
		if execNode == b.LocalName && b.NextCh != nil {
			b.NextCh <- message
			continue
		}
		if _, err := common.SendMessage(execNode, 8000, message); err != nil {
			log.Printf("%s: Failed to send batch %d to exec %s: %v", b.Name, b.seqNum, execNode, err)
		}
	}
}

func (b *Batcher) HandleRequestMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", b.Name, payload)

	// TODO: should we forward to primary + also check if primary is live?
	if !b.isPrimary {
		return map[string]any{"status": "ignored_non_primary"}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.batch = append(b.batch, payload)
	if len(b.batch) >= b.batchSize {
		b.flushBatchLocked()
	}

	return map[string]any{"status": "batched"}
}

// TODO: allow primaries to rotate, on batcher failures
