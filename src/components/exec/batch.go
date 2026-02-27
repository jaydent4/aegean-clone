package exec

import (
	"log"
	"time"

	"aegean/common"
)

func batchRequestIDs(parallelBatches [][]map[string]any) []string {
	ids := make([]string, 0)
	fallback := 0
	for _, batch := range parallelBatches {
		for _, request := range batch {
			ids = append(ids, requestIDForSchedule(request, fallback))
			fallback++
		}
	}
	return ids
}

func (e *Exec) flushNextBatch() bool {
	e.mu.Lock()
	seq := e.nextBatchSeq
	e.mu.Unlock()

	msgs := e.batchBuffer.Pop(seq)
	if len(msgs) == 0 {
		return false
	}
	for _, msg := range msgs {
		_ = e.handleBatch(msg)
	}
	e.mu.Lock()
	if e.nextBatchSeq == seq {
		e.nextBatchSeq++
	}
	e.mu.Unlock()
	return true
}

func (e *Exec) handleBatch(payload map[string]any) map[string]any {
	seqNum := common.GetInt(payload, "seq_num")
	parallelBatchesAny, _ := payload["parallel_batches"]
	ndSeed := common.GetInt64(payload, "nd_seed")
	ndTimestamp := common.GetFloat(payload, "nd_timestamp")
	if ndTimestamp == 0 {
		ndTimestamp = float64(time.Now().UnixNano()) / 1e9
	}

	parallelBatches, ok := parallelBatchesAny.([][]map[string]any)
	if !ok {
		return map[string]any{"status": "error", "error": "invalid parallel_batches"}
	}
	log.Printf("%s: received batch seq_num=%d request_ids=%v", e.Name, seqNum, batchRequestIDs(parallelBatches))

	// Defer insertion of new keys to end-of-batch deterministic phase.
	e.beginBatchMerkleContext()
	// Execute all parallelBatches and collect outputs.
	e.mu.Lock()
	forceSequential := e.forceSequential
	e.mu.Unlock()
	var outputs []map[string]any
	if forceSequential {
		outputs = e.executeSequentialBatches(parallelBatches, ndSeed, ndTimestamp)
	} else {
		outputs = e.executeParallelBatches(parallelBatches, ndSeed, ndTimestamp)
	}
	e.finalizeBatchMerkleContext()

	e.stateMu.Lock()
	e.workingState.EnsureMerkle()
	stateSnapshot := common.CopyStringMap(e.workingState.KVStore)
	stateRoot := e.workingState.MerkleRoot
	e.stateMu.Unlock()

	e.mu.Lock()
	e.replayableBatchInputs[seqNum] = payload
	e.pendingExecResults[seqNum] = pendingExecResult{
		outputs:    outputs,
		state:      stateSnapshot,
		merkleRoot: stateRoot,
		token:      "",
	}
	e.mu.Unlock()
	return map[string]any{"status": "executed", "seq_num": seqNum}
}
