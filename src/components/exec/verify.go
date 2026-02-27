package exec

import (
	"log"

	"aegean/common"
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
			pending.outputs,
			e.Verifiers,
		)
		if logExecStateDetails {
			log.Printf(
				"%s: assembled verify hash state seq_num=%d view_num=%d state=%v",
				e.Name,
				seq,
				view,
				pending.state,
			)
		}

		for _, verifier := range e.Verifiers {
			if verifier == e.Name && e.VerifierCh != nil {
				e.VerifierCh <- verifyMsg
				continue
			}
			_, _ = common.SendMessage(verifier, 8000, verifyMsg)
		}
		return true
	}
	return false
}

func (e *Exec) finalizeCommit(seqNum int, pending pendingExecResult, agreedToken string) {
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
		responseMsg := map[string]any{
			"type":       "response",
			"request_id": requestID,
			"response":   output,
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
