package exec

import (
	"log"
	"time"

	"aegean/common"
)

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
		log.Printf("%s: Invalid parallel_batches type %T", e.Name, parallelBatchesAny)
		return map[string]any{"status": "error", "error": "invalid parallel_batches"}
	}
	log.Printf("%s: Executing batch %d with %d parallelBatches", e.Name, seqNum, len(parallelBatches))

	// Defer insertion of new keys to end-of-batch deterministic phase.
	e.beginBatchMerkleContext()
	// Execute all parallelBatches and collect outputs
	outputs := e.executeParallelBatches(parallelBatches, ndSeed, ndTimestamp)
	e.finalizeBatchMerkleContext()

	e.stateMu.Lock()
	e.workingState.EnsureMerkle()
	merkleSnapshot := e.workingState.Merkle.Clone()
	stateSnapshot := merkleSnapshot.SnapshotMap()
	stateRoot := merkleSnapshot.Root()
	e.stateMu.Unlock()

	e.mu.Lock()
	e.pendingResponses[seqNum] = pendingResponse{
		outputs:    outputs,
		state:      stateSnapshot,
		merkle:     merkleSnapshot,
		merkleRoot: stateRoot,
		token:      "",
	}
	e.mu.Unlock()
	return map[string]any{"status": "executed", "seq_num": seqNum}
}
