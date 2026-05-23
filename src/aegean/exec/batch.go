package exec

import (
	"log"
	"time"

	"aegean/common"
	"aegean/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func batchRequestCount(parallelBatches [][]map[string]any) int {
	total := 0
	for _, batch := range parallelBatches {
		total += len(batch)
	}
	return total
}

func schedulerWorkflowStat(stats executionSchedulerStats, key string) int64 {
	if stats.workflowStats == nil {
		return 0
	}
	return stats.workflowStats[key]
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
		var batchServiceSpan trace.Span
		if telemetry.DetailedSpansEnabled() {
			_, batchServiceSpan = telemetry.StartSpanFromPayload(
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
		}
		for parallelBatchIdx, batch := range parallelBatches {
			for requestIdx, request := range batch {
				e.endRequestSpan(request["request_id"], batchBufferWaitSpanContextKey)
				_ = parallelBatchIdx
				_ = requestIdx
			}
		}
		if batchServiceSpan != nil && batchServiceSpan.IsRecording() {
			msg["_batch_service_span"] = batchServiceSpan
		}
		e.batchExecCh <- batchExecutionTask{payload: msg}
	}
	return true
}

func (e *Exec) executeBatch(payload map[string]any) *batchExecutionResult {
	totalStart := time.Now()

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
	var nestedArrivalStats nestedResponseTimingSnapshot
	nestedTimingEnabled := e.nestedTimingLogsEnabled()
	if nestedTimingEnabled {
		nestedArrivalStats = nestedRequestArrivalTimingStats(parallelBatches)
	}
	contextStart := time.Now()
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
	contextDuration := time.Since(contextStart)

	// Defer insertion of new keys to end-of-batch deterministic phase.
	beginMerkleStart := time.Now()
	e.beginBatchMerkleContext()
	beginMerkleDuration := time.Since(beginMerkleStart)
	// Execute all parallelBatches and collect outputs.
	e.mu.Lock()
	forceSequential := e.forceSequential
	e.mu.Unlock()
	var outputs []map[string]any
	var schedulerStats executionSchedulerStats
	executeStart := time.Now()
	if forceSequential {
		outputs, schedulerStats = e.executeSequentialBatches(parallelBatches, ndSeed, ndTimestamp)
	} else {
		outputs, schedulerStats = e.executeParallelBatches(parallelBatches, ndSeed, ndTimestamp)
	}
	executeDuration := time.Since(executeStart)

	e.stateMu.Lock()
	pendingNewKeys := 0
	baseKeys := 0
	if e.batchCtx != nil {
		pendingNewKeys = len(e.batchCtx.pendingNew)
		baseKeys = e.batchCtx.baseKeyCount
	}
	e.stateMu.Unlock()

	finalizeMerkleStart := time.Now()
	stateDelta := e.finalizeBatchMerkleContext()
	finalizeMerkleDuration := time.Since(finalizeMerkleStart)
	stateDeltaKeys := len(stateDelta)

	snapshotStart := time.Now()
	e.stateMu.Lock()
	e.ensureWorkingMerkle()
	stateRoot := e.workingState.MerkleRoot
	stateKeys := len(e.workingState.KVStore)
	e.stateMu.Unlock()
	snapshotDuration := time.Since(snapshotStart)
	totalDuration := time.Since(totalStart)
	var nestedTimingStats nestedResponseTimingSnapshot
	if nestedTimingEnabled {
		nestedTimingStats = e.takeNestedResponseTimingStats(seqNum)
	}

	var batchServiceSpan trace.Span
	if spanAny, ok := payload["_batch_service_span"]; ok {
		if span, ok := spanAny.(trace.Span); ok {
			batchServiceSpan = span
		}
		delete(payload, "_batch_service_span")
	}
	if batchServiceSpan != nil && batchServiceSpan.IsRecording() {
		batchServiceSpan.SetAttributes(
			attribute.Int64("exec.batch.context_us", contextDuration.Microseconds()),
			attribute.Int64("exec.batch.begin_merkle_us", beginMerkleDuration.Microseconds()),
			attribute.Int64("exec.batch.execute_us", executeDuration.Microseconds()),
			attribute.Int("exec.batch.worker_calls", schedulerStats.workerCalls),
			attribute.Int("exec.batch.blocked_responses", schedulerStats.blockedResponses),
			attribute.Int("exec.batch.nested_waits", schedulerStats.nestedWaits),
			attribute.Int64("exec.batch.nested_wait_us", schedulerStats.nestedWait.Microseconds()),
			attribute.Int64("exec.batch.worker_exec_us", schedulerStats.workerExec.Microseconds()),
			attribute.Int64("exec.batch.max_worker_exec_us", schedulerStats.maxWorkerExec.Microseconds()),
			attribute.Int("exec.batch.pending_new_keys", pendingNewKeys),
			attribute.Int("exec.batch.base_keys", baseKeys),
			attribute.Int("exec.batch.state_delta_keys", stateDeltaKeys),
			attribute.Int64("exec.batch.finalize_merkle_us", finalizeMerkleDuration.Microseconds()),
			attribute.Int64("exec.batch.snapshot_us", snapshotDuration.Microseconds()),
			attribute.Int("exec.batch.state_keys", stateKeys),
			attribute.Int64("exec.batch.total_us", totalDuration.Microseconds()),
			attribute.Int("exec.batch.output_count", len(outputs)),
			attribute.Bool("exec.batch.force_sequential", forceSequential),
		)
		if schedulerWorkflowStat(schedulerStats, "hotel_search_calls") > 0 {
			batchServiceSpan.SetAttributes(
				attribute.Int64("workflow.hotel_search.calls", schedulerWorkflowStat(schedulerStats, "hotel_search_calls")),
				attribute.Int64("workflow.hotel_search.stage_initial", schedulerWorkflowStat(schedulerStats, "hotel_search_stage_initial")),
				attribute.Int64("workflow.hotel_search.stage_await_geo", schedulerWorkflowStat(schedulerStats, "hotel_search_stage_await_geo")),
				attribute.Int64("workflow.hotel_search.stage_await_rate", schedulerWorkflowStat(schedulerStats, "hotel_search_stage_await_rate")),
				attribute.Int64("workflow.hotel_search.total_us", schedulerWorkflowStat(schedulerStats, "hotel_search_total_us")),
				attribute.Int64("workflow.hotel_search.dispatch_us", schedulerWorkflowStat(schedulerStats, "hotel_search_dispatch_us")),
				attribute.Int64("workflow.hotel_search.ledger_us", schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_us")),
				attribute.Int64("workflow.hotel_search.ledger_state_read_us", schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_state_read_us")),
				attribute.Int64("workflow.hotel_search.ledger_redis_get_us", schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_redis_get_us")),
				attribute.Int64("workflow.hotel_search.ledger_state_write_us", schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_state_write_us")),
				attribute.Int64("workflow.hotel_search.ledger_redis_set_us", schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_redis_set_us")),
				attribute.Int64("workflow.hotel_search.get_nested_us", schedulerWorkflowStat(schedulerStats, "hotel_search_get_nested_us")),
				attribute.Int64("workflow.hotel_search.nested_ready_us", schedulerWorkflowStat(schedulerStats, "hotel_search_nested_ready_us")),
				attribute.Int64("workflow.hotel_search.nested_select_us", schedulerWorkflowStat(schedulerStats, "hotel_search_nested_select_us")),
				attribute.Int64("workflow.hotel_search.context_set_us", schedulerWorkflowStat(schedulerStats, "hotel_search_context_set_us")),
				attribute.Int64("workflow.hotel_search.context_payload_us", schedulerWorkflowStat(schedulerStats, "hotel_search_context_payload_us")),
			)
		}
	}
	timingLogsEnabled := e.timingLogsEnabled()
	if timingLogsEnabled {
		log.Printf(
			"%s: exec_batch_timing seq_num=%d request_count=%d parallel_batches=%d output_count=%d force_sequential=%t context_us=%d begin_merkle_us=%d execute_us=%d worker_calls=%d blocked_responses=%d nested_waits=%d nested_wait_us=%d worker_exec_us=%d max_worker_exec_us=%d pending_new_keys=%d base_keys=%d state_delta_keys=%d finalize_merkle_us=%d snapshot_us=%d state_keys=%d total_us=%d",
			e.Name,
			seqNum,
			requestCount,
			len(parallelBatches),
			len(outputs),
			forceSequential,
			contextDuration.Microseconds(),
			beginMerkleDuration.Microseconds(),
			executeDuration.Microseconds(),
			schedulerStats.workerCalls,
			schedulerStats.blockedResponses,
			schedulerStats.nestedWaits,
			schedulerStats.nestedWait.Microseconds(),
			schedulerStats.workerExec.Microseconds(),
			schedulerStats.maxWorkerExec.Microseconds(),
			pendingNewKeys,
			baseKeys,
			stateDeltaKeys,
			finalizeMerkleDuration.Microseconds(),
			snapshotDuration.Microseconds(),
			stateKeys,
			totalDuration.Microseconds(),
		)
	}
	if nestedTimingStats.count > 0 {
		geoTiming := nestedTimingStats.child("geo")
		rateTiming := nestedTimingStats.child("rate")
		log.Printf(
			"%s: nested_response_timing seq_num=%d response_count=%d geo_count=%d geo_dispatch_to_buffer_us=%d geo_backend_to_buffer_us=%d geo_child_handle_to_buffer_us=%d geo_child_enqueue_to_buffer_us=%d geo_child_send_to_buffer_us=%d geo_child_send_to_parent_us=%d geo_parent_shim_to_buffer_us=%d geo_parent_quorum_to_buffer_us=%d geo_max_dispatch_to_buffer_us=%d geo_max_backend_to_buffer_us=%d geo_max_child_send_to_parent_us=%d geo_max_parent_shim_to_buffer_us=%d geo_max_parent_quorum_to_buffer_us=%d geo_missing_dispatch_ts=%d geo_missing_backend_ts=%d geo_missing_parent_ts=%d geo_missing_child_return_ts=%d rate_count=%d rate_dispatch_to_buffer_us=%d rate_backend_to_buffer_us=%d rate_child_handle_to_buffer_us=%d rate_child_enqueue_to_buffer_us=%d rate_child_send_to_buffer_us=%d rate_child_send_to_parent_us=%d rate_parent_shim_to_buffer_us=%d rate_parent_quorum_to_buffer_us=%d rate_max_dispatch_to_buffer_us=%d rate_max_backend_to_buffer_us=%d rate_max_child_send_to_parent_us=%d rate_max_parent_shim_to_buffer_us=%d rate_max_parent_quorum_to_buffer_us=%d rate_missing_dispatch_ts=%d rate_missing_backend_ts=%d rate_missing_parent_ts=%d rate_missing_child_return_ts=%d",
			e.Name,
			seqNum,
			nestedTimingStats.count,
			geoTiming.count,
			geoTiming.dispatchToBuffer.Microseconds(),
			geoTiming.backendCommitToBuffer.Microseconds(),
			geoTiming.childShimHandleToBuffer.Microseconds(),
			geoTiming.childShimEnqueueToBuffer.Microseconds(),
			geoTiming.childShimSendToBuffer.Microseconds(),
			geoTiming.childShimSendToParent.Microseconds(),
			geoTiming.parentShimToBuffer.Microseconds(),
			geoTiming.parentQuorumToBuffer.Microseconds(),
			geoTiming.maxDispatchToBuffer.Microseconds(),
			geoTiming.maxBackendCommitToBuffer.Microseconds(),
			geoTiming.maxChildShimSendToParent.Microseconds(),
			geoTiming.maxParentShimToBuffer.Microseconds(),
			geoTiming.maxParentQuorumToBuffer.Microseconds(),
			geoTiming.missingDispatchTimestamp+geoTiming.negativeDispatchDuration,
			geoTiming.missingBackendTimestamp+geoTiming.negativeBackendDuration,
			geoTiming.missingParentShimStamp+geoTiming.missingParentQuorumStamp+geoTiming.negativeParentDuration,
			geoTiming.missingChildReturnStamp+geoTiming.negativeChildDuration,
			rateTiming.count,
			rateTiming.dispatchToBuffer.Microseconds(),
			rateTiming.backendCommitToBuffer.Microseconds(),
			rateTiming.childShimHandleToBuffer.Microseconds(),
			rateTiming.childShimEnqueueToBuffer.Microseconds(),
			rateTiming.childShimSendToBuffer.Microseconds(),
			rateTiming.childShimSendToParent.Microseconds(),
			rateTiming.parentShimToBuffer.Microseconds(),
			rateTiming.parentQuorumToBuffer.Microseconds(),
			rateTiming.maxDispatchToBuffer.Microseconds(),
			rateTiming.maxBackendCommitToBuffer.Microseconds(),
			rateTiming.maxChildShimSendToParent.Microseconds(),
			rateTiming.maxParentShimToBuffer.Microseconds(),
			rateTiming.maxParentQuorumToBuffer.Microseconds(),
			rateTiming.missingDispatchTimestamp+rateTiming.negativeDispatchDuration,
			rateTiming.missingBackendTimestamp+rateTiming.negativeBackendDuration,
			rateTiming.missingParentShimStamp+rateTiming.missingParentQuorumStamp+rateTiming.negativeParentDuration,
			rateTiming.missingChildReturnStamp+rateTiming.negativeChildDuration,
		)
	}
	if nestedArrivalStats.count > 0 {
		geoTiming := nestedArrivalStats.child("geo")
		rateTiming := nestedArrivalStats.child("rate")
		log.Printf(
			"%s: nested_request_arrival_timing seq_num=%d request_count=%d geo_count=%d geo_dispatch_to_execute_start_us=%d geo_dispatch_to_child_shim_us=%d geo_child_shim_quorum_wait_us=%d geo_child_shim_to_batcher_us=%d geo_child_batcher_queue_us=%d geo_child_batcher_to_execute_us=%d geo_max_dispatch_to_execute_start_us=%d geo_max_child_batcher_queue_us=%d geo_missing_child_request_ts=%d rate_count=%d rate_dispatch_to_execute_start_us=%d rate_dispatch_to_child_shim_us=%d rate_child_shim_quorum_wait_us=%d rate_child_shim_to_batcher_us=%d rate_child_batcher_queue_us=%d rate_child_batcher_to_execute_us=%d rate_max_dispatch_to_execute_start_us=%d rate_max_child_batcher_queue_us=%d rate_missing_child_request_ts=%d",
			e.Name,
			seqNum,
			nestedArrivalStats.count,
			geoTiming.count,
			geoTiming.dispatchToBuffer.Microseconds(),
			geoTiming.dispatchToChildShim.Microseconds(),
			geoTiming.childShimQuorumWait.Microseconds(),
			geoTiming.childShimToBatcher.Microseconds(),
			geoTiming.childBatcherQueue.Microseconds(),
			geoTiming.childBatcherToExecute.Microseconds(),
			geoTiming.maxDispatchToBuffer.Microseconds(),
			geoTiming.maxChildBatcherQueue.Microseconds(),
			geoTiming.missingChildRequestStamp+geoTiming.negativeChildDuration,
			rateTiming.count,
			rateTiming.dispatchToBuffer.Microseconds(),
			rateTiming.dispatchToChildShim.Microseconds(),
			rateTiming.childShimQuorumWait.Microseconds(),
			rateTiming.childShimToBatcher.Microseconds(),
			rateTiming.childBatcherQueue.Microseconds(),
			rateTiming.childBatcherToExecute.Microseconds(),
			rateTiming.maxDispatchToBuffer.Microseconds(),
			rateTiming.maxChildBatcherQueue.Microseconds(),
			rateTiming.missingChildRequestStamp+rateTiming.negativeChildDuration,
		)
	}
	if timingLogsEnabled && schedulerWorkflowStat(schedulerStats, "hotel_search_calls") > 0 {
		log.Printf(
			"%s: hotel_search_workflow_timing seq_num=%d calls=%d stage_initial=%d stage_await_geo=%d stage_await_rate=%d blocked=%d complete=%d wait_again=%d empty_geo=%d dispatches=%d nested_entries=%d total_us=%d payload_us=%d stage_get_us=%d normalize_us=%d validate_us=%d context_set_us=%d context_payload_us=%d nested_build_us=%d dispatch_us=%d get_nested_us=%d nested_ready_us=%d nested_select_us=%d ledger_us=%d ledger_key_us=%d ledger_read_us=%d ledger_state_read_us=%d ledger_read_redis_client_us=%d ledger_redis_get_us=%d ledger_decode_us=%d ledger_mutate_us=%d ledger_encode_us=%d ledger_write_us=%d ledger_state_write_us=%d ledger_write_redis_client_us=%d ledger_redis_set_us=%d clear_context_us=%d response_build_us=%d",
			e.Name,
			seqNum,
			schedulerWorkflowStat(schedulerStats, "hotel_search_calls"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_stage_initial"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_stage_await_geo"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_stage_await_rate"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_blocked_outputs"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_complete_outputs"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_wait_again_outputs"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_empty_geo_outputs"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_dispatches"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_nested_response_entries"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_total_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_payload_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_stage_get_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_normalize_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_validate_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_context_set_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_context_payload_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_nested_build_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_dispatch_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_get_nested_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_nested_ready_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_nested_select_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_key_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_read_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_state_read_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_read_redis_client_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_redis_get_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_decode_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_mutate_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_encode_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_write_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_state_write_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_write_redis_client_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_ledger_redis_set_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_clear_context_us"),
			schedulerWorkflowStat(schedulerStats, "hotel_search_response_build_us"),
		)
	}

	return &batchExecutionResult{
		seqNum:           seqNum,
		payload:          payload,
		outputs:          outputs,
		stateDelta:       stateDelta,
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
		stateDelta: result.stateDelta,
		merkleRoot: result.merkleRoot,
		token:      "",
	}
	e.mu.Unlock()
}
