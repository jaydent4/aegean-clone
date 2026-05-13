package exec

import (
	"log"

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
		if batchPayload, ok := e.replayableBatchInputs[seq]; ok {
			_, verifySpan := telemetry.StartSpanFromPayload(
				batchPayload,
				"exec.verify_wait",
				append(
					telemetry.AttrsFromPayload(batchPayload),
					attribute.String("node.name", e.Name),
					attribute.Int("batch.seq_num", seq),
				)...,
			)
			pending.verifySpan = verifySpan
		}
		token := e.computeStateHash(pending.merkleRoot, pending.outputs, prevHash, seq)
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
		log.Printf(
			"%s: assembled verify hash seq_num=%d view_num=%d stable_seq_num=%d prev_hash=%s state_root=%s final_hash=%s output_count=%d outputs=%v verifiers=%v",
			e.Name,
			seq,
			view,
			stableSeqNum,
			shortHash(prevHash),
			shortHash(pending.merkleRoot),
			shortHash(token),
			len(pending.outputs),
			truncateLogValue(pending.outputs),
			e.Verifiers,
		)
		if logExecStateDetails {
			log.Printf(
				"%s: assembled verify hash state seq_num=%d view_num=%d state=%v",
				e.Name,
				seq,
				view,
				truncateLogStringMap(pending.state),
			)
		}

		for _, verifier := range e.Verifiers {
			if verifier == e.Name && e.VerifierCh != nil {
				e.VerifierCh <- verifyMsg
				continue
			}
			_, _ = netx.SendMessage(verifier, 8000, verifyMsg)
		}
		return true
	}
	return false
}

func (e *Exec) finalizeCommit(seqNum int, pending pendingExecResult, agreedToken string) {
	if pending.verifySpan != nil {
		pending.verifySpan.End()
	}
	e.mu.Lock()
	delete(e.pendingExecResults, seqNum)
	e.stableState = State{
		KVStore:    pending.state,
		Merkle:     nil,
		MerkleRoot: pending.merkleRoot,
		SeqNum:     seqNum,
		PrevHash:   agreedToken,
		Verified:   true,
	}
	e.storeCheckpoint(seqNum, agreedToken, pending.state, pending.merkleRoot)
	for batchSeq := range e.replayableBatchInputs {
		if batchSeq <= seqNum {
			delete(e.replayableBatchInputs, batchSeq)
		}
	}
	e.forceSequential = false
	e.mu.Unlock()

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
		if requestPayload := e.requestPayloadForSeq(seqNum, requestID); requestPayload != nil {
			telemetry.CopyContext(responseMsg, requestPayload)
		}
		if e.ShimCh != nil {
			e.ShimCh <- responseMsg
		}
	}
	e.gcCommittedNestedResponses(pending.outputs)
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
