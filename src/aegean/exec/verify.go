package exec

import (
	"log"
	"time"

	netx "aegean/net"
	"aegean/telemetry"
	"go.opentelemetry.io/otel/attribute"
)

func (e *Exec) flushNextVerify() bool {
	e.mu.Lock()
	seq := e.nextVerifySeq
	pending, ok := e.pendingExecResults[seq]
	stableSeqNum := e.stableState.SeqNum
	prevHash := e.stableState.PrevHash
	view := e.view
	e.mu.Unlock()
	// Compute token with committed prevHash to avoid divergence for the next sequence.
	if ok && seq == stableSeqNum+1 && !pending.verifySent {
		verifyStart := time.Now()
		for _, request := range e.requestPayloadsForSeq(seq) {
			e.endRequestSpan(request["request_id"], postNestedVerifyGateWaitSpanContextKey)
			e.endRequestSpan(request["request_id"], requestVerifyGateWaitSpanContextKey)
			e.startRequestSpan(
				request,
				requestVerifyWaitSpanContextKey,
				"exec.request_verify_wait",
				attribute.Int("batch.seq_num", seq),
				attribute.Int("gate.next_verify_seq", seq),
				attribute.Int("gate.stable_seq_num", stableSeqNum),
			)
		}
		if batchPayload, ok := e.replayableBatchInputs[seq]; ok && telemetry.DetailedSpansEnabled() {
			_, verifySpan := telemetry.StartSpanFromPayload(
				batchPayload,
				"exec.verify_wait",
				append(
					telemetry.AttrsFromPayload(batchPayload),
					attribute.String("node.name", e.Name),
					attribute.Int("batch.seq_num", seq),
				)...,
			)
			if verifySpan.IsRecording() {
				pending.verifySpan = verifySpan
			}
		}
		computeStart := time.Now()
		token := e.computeStateHash(pending.merkleRoot, pending.outputs, prevHash, seq)
		computeDuration := time.Since(computeStart)
		pending.token = token
		pending.verifySent = true
		e.mu.Lock()
		// Guard against rollover while token was being computed
		if e.nextVerifySeq != seq || e.stableState.SeqNum != stableSeqNum {
			e.mu.Unlock()
			return false
		}
		e.pendingExecResults[seq] = pending
		e.mu.Unlock()

		// Broadcast verify request for this sequence to all verifiers
		verifyMsg := map[string]any{
			"type":      "verify",
			"view":      view,
			"seq_num":   seq,
			"token":     token,
			"prev_hash": prevHash,
			"exec_id":   e.Name,
		}
		if batchPayload, ok := e.replayableBatchInputs[seq]; ok {
			telemetry.CopyContext(verifyMsg, batchPayload)
		}
		timingLogsEnabled := e.timingLogsEnabled()
		if timingLogsEnabled {
			log.Printf(
				"%s: assembled_verify seq_num=%d view_num=%d stable_seq_num=%d prev_hash=%s state_root=%s final_hash=%s output_count=%d verifiers=%d",
				e.Name,
				seq,
				view,
				stableSeqNum,
				shortHash(prevHash),
				shortHash(pending.merkleRoot),
				shortHash(token),
				len(pending.outputs),
				len(e.Verifiers),
			)
		}
		if logExecStateDetails {
			log.Printf(
				"%s: assembled verify hash state_delta seq_num=%d view_num=%d state_delta=%v",
				e.Name,
				seq,
				view,
				truncateLogStringMap(pending.stateDelta),
			)
		}

		sendStart := time.Now()
		remoteVerifiers := 0
		for _, verifier := range e.Verifiers {
			if verifier == e.Name && e.VerifierCh != nil {
				e.VerifierCh <- verifyMsg
				continue
			}
			remoteVerifiers++
			_, _ = netx.SendMessage(verifier, 8000, verifyMsg)
		}
		sendDuration := time.Since(sendStart)
		totalDuration := time.Since(verifyStart)
		if pending.verifySpan != nil && pending.verifySpan.IsRecording() {
			pending.verifySpan.SetAttributes(
				attribute.Int64("exec.verify.compute_hash_us", computeDuration.Microseconds()),
				attribute.Int64("exec.verify.send_us", sendDuration.Microseconds()),
				attribute.Int64("exec.verify.total_us", totalDuration.Microseconds()),
				attribute.Int("exec.verify.output_count", len(pending.outputs)),
				attribute.Int("exec.verify.remote_verifier_count", remoteVerifiers),
			)
		}
		if timingLogsEnabled {
			log.Printf(
				"%s: exec_verify_timing seq_num=%d output_count=%d compute_hash_us=%d send_verify_us=%d total_us=%d verifiers=%d remote_verifiers=%d",
				e.Name,
				seq,
				len(pending.outputs),
				computeDuration.Microseconds(),
				sendDuration.Microseconds(),
				totalDuration.Microseconds(),
				len(e.Verifiers),
				remoteVerifiers,
			)
		}
		return true
	}
	return false
}

func (e *Exec) finalizeCommit(seqNum int, pending pendingExecResult, agreedToken string) {
	commitStart := time.Now()
	if pending.verifySpan != nil {
		pending.verifySpan.End()
	}
	requestPayloadsByID := e.requestPayloadsByIDForSeq(seqNum)
	stateStart := time.Now()
	e.mu.Lock()
	delete(e.pendingExecResults, seqNum)
	if e.stableState.KVStore == nil {
		e.stableState.KVStore = make(map[string]string)
	}
	stableKV := e.stableState.KVStore
	applyStateDelta(stableKV, pending.stateDelta)
	e.stableState = State{
		KVStore:    stableKV,
		Merkle:     nil,
		MerkleRoot: pending.merkleRoot,
		SeqNum:     seqNum,
		PrevHash:   agreedToken,
		Verified:   true,
	}
	e.storeCheckpointOwned(seqNum, agreedToken, stableKV, pending.merkleRoot)
	for batchSeq := range e.replayableBatchInputs {
		if batchSeq <= seqNum {
			delete(e.replayableBatchInputs, batchSeq)
		}
	}
	e.forceSequential = false
	e.mu.Unlock()
	stateDuration := time.Since(stateStart)

	responseStart := time.Now()
	for _, output := range pending.outputs {
		requestID := output["request_id"]
		e.endRequestSpan(requestID, requestVerifyWaitSpanContextKey)
		responseMsg := map[string]any{
			"type":       "response",
			"request_id": requestID,
			"response":   output,
		}
		if parentRequestID, ok := output["parent_request_id"]; ok && parentRequestID != nil {
			responseMsg["parent_request_id"] = parentRequestID
		}
		canonicalID, _ := canonicalRequestID(requestID)
		if requestPayload := requestPayloadsByID[canonicalID]; requestPayload != nil {
			telemetry.CopyContext(responseMsg, requestPayload)
			if copyNestedTimingMetadata(responseMsg, requestPayload) {
				addNestedBackendCommitMetadata(responseMsg, e.Name, seqNum, len(pending.outputs))
			}
		}
		if e.ShimCh != nil {
			e.ShimCh <- responseMsg
		}
	}
	responseDuration := time.Since(responseStart)
	gcStart := time.Now()
	e.gcCommittedNestedResponses(pending.outputs)
	gcDuration := time.Since(gcStart)
	totalDuration := time.Since(commitStart)
	if e.timingLogsEnabled() {
		log.Printf(
			"%s: exec_commit_timing seq_num=%d output_count=%d state_update_us=%d response_send_us=%d gc_us=%d total_us=%d",
			e.Name,
			seqNum,
			len(pending.outputs),
			stateDuration.Microseconds(),
			responseDuration.Microseconds(),
			gcDuration.Microseconds(),
			totalDuration.Microseconds(),
		)
	}
}

func (e *Exec) gcCommittedNestedResponses(outputs []map[string]any) {
	requestIDs := make([]string, 0, len(outputs))
	for _, output := range outputs {
		requestID, ok := canonicalRequestID(output["request_id"])
		if !ok {
			continue
		}
		requestIDs = append(requestIDs, requestID)
	}
	if len(requestIDs) == 0 {
		return
	}
	e.scheduler.clearNestedResponses(requestIDs)
}
