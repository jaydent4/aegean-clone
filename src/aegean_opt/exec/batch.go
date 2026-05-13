package exec

import (
	"log"
	"time"

	"aegean/common"
	"aegean/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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

func batchRequestCount(parallelBatches [][]map[string]any) int {
	total := 0
	for _, batch := range parallelBatches {
		total += len(batch)
	}
	return total
}

func (e *Exec) flushNextBatch() bool {
	e.mu.Lock()
	seq := e.nextBatchSeq
	if e.batchExecuting {
		e.mu.Unlock()
		return false
	}
	e.mu.Unlock()

	msgs := e.batchBuffer.Pop(seq)
	if len(msgs) == 0 {
		return false
	}
	for _, msg := range msgs {
		parallelBatches, _ := msg["parallel_batches"].([][]map[string]any)
		requestCount := batchRequestCount(parallelBatches)
		nextVerifySeq, stableSeq := e.RequestVerifyGateSnapshot()
		e.mu.Lock()
		if e.nextBatchSeq == seq {
			e.nextBatchSeq++
		}
		e.batchExecuting = true
		e.mu.Unlock()
		if queueSpanAny, ok := msg["_batch_queue_wait_span"]; ok {
			if queueSpan, ok := queueSpanAny.(trace.Span); ok && queueSpan != nil {
				queueSpan.End()
			}
			delete(msg, "_batch_queue_wait_span")
		}
		_, batchServiceSpan := telemetry.StartSpanFromPayload(
			msg,
			"exec.batch_service_time",
			append(
				telemetry.AttrsFromPayload(msg),
				attribute.String("node.name", e.Name),
				attribute.Int("batch.seq_num", seq),
				attribute.Int("batch.request_count", requestCount),
				attribute.Int("parallel_batch.count", len(parallelBatches)),
				attribute.Int("gate.next_verify_seq", nextVerifySeq),
				attribute.Int("gate.stable_seq_num", stableSeq),
			)...,
		)
		for parallelBatchIdx, batch := range parallelBatches {
			for requestIdx, request := range batch {
				e.endRequestSpan(request["request_id"], batchBufferWaitSpanContextKey)
				_ = parallelBatchIdx
				_ = requestIdx
			}
		}
		msg["_batch_service_span"] = batchServiceSpan
		e.batchExecCh <- batchExecutionTask{payload: msg}
	}
	return true
}

func (e *Exec) executeBatch(payload map[string]any) *batchExecutionResult {
	seqNum := common.GetInt(payload, "seq_num")
	parallelBatchesAny, _ := payload["parallel_batches"]
	ndSeed := common.GetInt64(payload, "nd_seed")
	ndTimestamp := common.GetFloat(payload, "nd_timestamp")
	if ndTimestamp == 0 {
		ndTimestamp = float64(time.Now().UnixNano()) / 1e9
	}

	parallelBatches, ok := parallelBatchesAny.([][]map[string]any)
	if !ok {
		return &batchExecutionResult{seqNum: seqNum, payload: payload}
	}
	requestCount := batchRequestCount(parallelBatches)
	for parallelBatchIdx, batch := range parallelBatches {
		for requestIdx, request := range batch {
			_ = e.SetRequestContextValue(request["request_id"], requestBatchSeqContextKey, seqNum)
			_ = e.SetRequestContextValue(request["request_id"], "parallel_batch_index", parallelBatchIdx)
			_ = e.SetRequestContextValue(request["request_id"], "parallel_batch_request_index", requestIdx)
			_ = e.SetRequestContextValue(request["request_id"], "parallel_batch_size", len(batch))
			_ = e.SetRequestContextValue(request["request_id"], "parallel_batch_count", len(parallelBatches))
			_ = e.SetRequestContextValue(request["request_id"], "batch_request_count", requestCount)
		}
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

	var batchServiceSpan trace.Span
	if spanAny, ok := payload["_batch_service_span"]; ok {
		if span, ok := spanAny.(trace.Span); ok {
			batchServiceSpan = span
		}
		delete(payload, "_batch_service_span")
	}

	return &batchExecutionResult{
		seqNum:           seqNum,
		payload:          payload,
		outputs:          outputs,
		state:            stateSnapshot,
		merkleRoot:       stateRoot,
		batchServiceSpan: batchServiceSpan,
	}
}

func (e *Exec) handleBatch(payload map[string]any) map[string]any {
	result := e.executeBatch(payload)
	e.applyBatchExecutionResult(result)
	return map[string]any{"status": "executed", "seq_num": result.seqNum}
}

func (e *Exec) applyBatchExecutionResult(result *batchExecutionResult) {
	if result == nil {
		return
	}
	if result.batchServiceSpan != nil {
		result.batchServiceSpan.End()
	}
	e.mu.Lock()
	e.batchExecuting = false
	e.replayableBatchInputs[result.seqNum] = result.payload
	e.pendingExecResults[result.seqNum] = pendingExecResult{
		outputs:    result.outputs,
		state:      result.state,
		merkleRoot: result.merkleRoot,
		token:      "",
	}
	e.mu.Unlock()
}
