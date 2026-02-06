package nodes

import (
	"log"
	"sync"
	"time"
)

type Mixer struct {
	Name string
	NextCh chan<- map[string]any
	// Origin shim for response routing
	Shim          string
	batch         []map[string]any
	batchSize     int
	batchTimeout  time.Duration
	seqNum        int
	mu            sync.Mutex
	lastBatchTime time.Time
}

func NewMixer(name string, nextCh chan<- map[string]any) *Mixer {
	if nextCh == nil {
		log.Fatalf("mixer component requires non-nil nextCh")
	}
	m := &Mixer{
		Name:          name,
		NextCh:        nextCh,
		batch:         []map[string]any{},
		batchSize:     10,
		batchTimeout:  100 * time.Millisecond,
		lastBatchTime: time.Now(),
	}
	return m
}

func (m *Mixer) StartBatchFlusher() {
	go m.batchFlusher()
}

func (m *Mixer) batchFlusher() {
	for {
		time.Sleep(m.batchTimeout)
		m.mu.Lock()
		// TODO: different mixers may have different timings of flushing, this may send different parallelBatches to exec, causing divergence
		if len(m.batch) > 0 && time.Since(m.lastBatchTime) >= m.batchTimeout {
			// TODO: undeterministic mixer is leading to divergence frequently
			// Reenable timeout flushing when mixer is properly implemented
			// m.flushBatchLocked()
		}
		m.mu.Unlock()
	}
}

func (m *Mixer) getKeys(request map[string]any) (map[string]struct{}, map[string]struct{}) {
	op, _ := request["op"].(string)
	payload, _ := request["op_payload"].(map[string]any)

	readKeys := make(map[string]struct{})
	writeKeys := make(map[string]struct{})

	switch op {
	case "read":
		if key, ok := payload["key"].(string); ok {
			readKeys[key] = struct{}{}
		}
	case "write":
		if key, ok := payload["key"].(string); ok {
			writeKeys[key] = struct{}{}
		}
	case "read_write":
		if key, ok := payload["read_key"].(string); ok {
			readKeys[key] = struct{}{}
		}
		if key, ok := payload["write_key"].(string); ok {
			writeKeys[key] = struct{}{}
		}
	}

	return readKeys, writeKeys
}

func hasIntersection(a, b map[string]struct{}) bool {
	for key := range a {
		if _, ok := b[key]; ok {
			return true
		}
	}
	return false
}

func (m *Mixer) hasConflict(reqRead, reqWrite, batchReads, batchWrites map[string]struct{}) bool {
	if hasIntersection(reqWrite, batchWrites) {
		return true
	}
	if hasIntersection(reqWrite, batchReads) {
		return true
	}
	if hasIntersection(reqRead, batchWrites) {
		return true
	}
	return false
}

type parallelBatch struct {
	requests []map[string]any
	reads    map[string]struct{}
	writes   map[string]struct{}
}

func (m *Mixer) partitionIntoParallelBatches(batch []map[string]any) [][]map[string]any {
	parallelBatches := []parallelBatch{}

	for _, request := range batch {
		reqRead, reqWrite := m.getKeys(request)
		placed := false

		for i := range parallelBatches {
			pb := &parallelBatches[i]
			if !m.hasConflict(reqRead, reqWrite, pb.reads, pb.writes) {
				pb.requests = append(pb.requests, request)
				for key := range reqRead {
					pb.reads[key] = struct{}{}
				}
				for key := range reqWrite {
					pb.writes[key] = struct{}{}
				}
				placed = true
				break
			}
		}

		if !placed {
			pb := parallelBatch{
				requests: []map[string]any{request},
				reads:    reqRead,
				writes:   reqWrite,
			}
			parallelBatches = append(parallelBatches, pb)
		}
	}

	result := make([][]map[string]any, 0, len(parallelBatches))
	for _, pb := range parallelBatches {
		result = append(result, pb.requests)
	}
	return result
}

func (m *Mixer) flushBatchLocked() {
	if len(m.batch) == 0 {
		return
	}

	batch := m.batch
	m.batch = []map[string]any{}
	m.seqNum++
	m.lastBatchTime = time.Now()

	parallelBatches := m.partitionIntoParallelBatches(batch)

	log.Printf("%s: Batch %d partitioned into %d parallelBatches from %d requests", m.Name, m.seqNum, len(parallelBatches), len(batch))

	message := map[string]any{
		"type":             "batch",
		"seq_num":          m.seqNum,
		"parallel_batches": parallelBatches,
		"nd_seed":          time.Now().UnixMilli(),
		"nd_timestamp":     float64(time.Now().UnixNano()) / 1e9,
	}

	if m.NextCh != nil {
		m.NextCh <- message
	} else {
		log.Printf("%s: Next channel not set; dropping batch %d", m.Name, m.seqNum)
	}
}

func (m *Mixer) HandleMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", m.Name, payload)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.batch = append(m.batch, payload)
	if len(m.batch) >= m.batchSize {
		m.flushBatchLocked()
	}

	return map[string]any{"status": "batched"}
}
