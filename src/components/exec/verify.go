package exec

import (
	"log"
	"time"

	"aegean/common"
)

func (e *Exec) flushNextVerify() bool {
	e.mu.Lock()
	seq := e.nextVerifySeq
	pending, ok := e.pendingResponses[seq]
	stableSeqNum := e.stableState.SeqNum
	prevHash := e.stableState.PrevHash
	view := e.view
	e.mu.Unlock()
	// Compute token with committed prevHash to avoid divergence for the next sequence.
	if ok && seq == stableSeqNum+1 && !pending.verifySent {
		if pending.merkle == nil {
			pending.merkle = NewMerkleTreeFromMap(pending.state)
			pending.merkleRoot = pending.merkle.Root()
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
		e.pendingResponses[seq] = pending
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

		for _, verifier := range e.Verifiers {
			if verifier == e.Name && e.VerifierCh != nil {
				e.VerifierCh <- verifyMsg
				continue
			}
			if _, err := common.SendMessage(verifier, 8000, verifyMsg); err != nil {
				log.Printf("Failed to send to verifier %s: %v", verifier, err)
			}
		}
		return true
	}

	// Consume buffered verify responses prioritized by highest view then earliest seq.
	msgView, msgSeq, ok := e.verifyBuffer.PeekNext()
	if !ok {
		return false
	}
	e.mu.Lock()
	stableSeqNum = e.stableState.SeqNum
	_, hasPending := e.pendingResponses[msgSeq]
	e.mu.Unlock()
	// Keep the highest-priority tuple buffered until this seq is actionable.
	if msgSeq > stableSeqNum && !hasPending {
		return false
	}
	msgs := e.verifyBuffer.Pop(msgView, msgSeq)
	if len(msgs) == 0 {
		return false
	}

	resolved := false
	for _, msg := range msgs {
		resp := e.handleVerifyResponse(msg)
		if done, _ := resp["resolved"].(bool); done {
			resolved = true
		}
	}
	return resolved
}

func (e *Exec) handleVerifyResponse(payload map[string]any) map[string]any {
	seqNum := common.GetInt(payload, "seq_num")
	agreedToken, _ := payload["token"].(string)
	view := common.GetInt(payload, "view")
	forceSequential, forceOK := payload["force_sequential"].(bool)
	verifierID, _ := payload["verifier_id"].(string)
	if payload["view"] == nil || !forceOK || verifierID == "" || agreedToken == "" {
		return map[string]any{"status": "invalid_verify_response", "resolved": false}
	}

	e.mu.Lock()
	stableSeqNum := e.stableState.SeqNum
	pending, hasPending := e.pendingResponses[seqNum]
	if seqNum > stableSeqNum && !hasPending {
		e.mu.Unlock()
		return map[string]any{"status": "no_pending_for_seq", "resolved": false}
	}

	tupleKey := responseTupleKey(view, seqNum, agreedToken, forceSequential)
	e.verifyResponseMsgs[tupleKey] = payload
	if _, ok := e.verifyResponseBySeq[seqNum]; !ok {
		e.verifyResponseBySeq[seqNum] = make(map[string]struct{})
	}
	e.verifyResponseBySeq[seqNum][tupleKey] = struct{}{}
	reached := e.verifyResponseQuorum.Add(tupleKey, verifierID)
	if !reached {
		e.ensureVerifyResponseTimerLocked(seqNum)
		e.mu.Unlock()
		return map[string]any{"status": "waiting_quorum", "resolved": false}
	}

	e.stopVerifyResponseTimerLocked(seqNum)
	e.clearVerifyResponseTrackingLocked(seqNum)
	shouldRollback := view > e.view || forceSequential
	if shouldRollback && view > e.view {
		e.view = view
	}
	if shouldRollback && hasPending {
		delete(e.pendingResponses, seqNum)
	}
	e.mu.Unlock()

	// Case 1: rollback (view increased or forced sequential by verifier).
	if shouldRollback {
		log.Printf("%s: rollback decision for seq=%d view=%d token=%s", e.Name, seqNum, view, common.TruncateToken(agreedToken))
		if e.rollbackTo(seqNum, agreedToken) {
			return map[string]any{"status": "processed", "decision": "rollback", "resolved": true}
		}
		// If rollback fails, repair
		e.requestStateTransferWithRetry(seqNum, 0, 10*time.Millisecond)
		e.mu.Lock()
		e.forceSequential = true
		e.mu.Unlock()
		return map[string]any{"status": "processed", "decision": "state_transfer", "resolved": true}
	}

	// Already-committed sequence with no rollback is a normal no-op.
	if seqNum <= stableSeqNum {
		return map[string]any{"status": "already_committed", "seq_num": seqNum, "resolved": true}
	}

	// Case 2: state transfer (same view quorum but token mismatch).
	if pending.token != agreedToken {
		log.Printf("%s: state diverged at seq=%d, agreed token mismatch", e.Name, seqNum)
		e.mu.Lock()
		delete(e.pendingResponses, seqNum)
		e.mu.Unlock()
		// Note: calling this will implicitly act as a global stall (because processMu), until state transfer is complete
		e.requestStateTransferWithRetry(seqNum, 0, 10*time.Millisecond)
		return map[string]any{"status": "processed", "decision": "state_transfer", "resolved": true}
	}

	// Case 3: normal commit (same view, token match).
	e.finalizeCommit(seqNum, pending, agreedToken)
	e.mu.Lock()
	if e.nextVerifySeq == seqNum {
		e.nextVerifySeq++
	}
	e.mu.Unlock()
	return map[string]any{"status": "processed", "decision": "commit", "resolved": true}
}

func (e *Exec) finalizeCommit(seqNum int, pending pendingResponse, agreedToken string) {
	log.Printf("%s: Committing seq_num %d", e.Name, seqNum)
	if pending.merkle == nil {
		pending.merkle = NewMerkleTreeFromMap(pending.state)
		pending.merkleRoot = pending.merkle.Root()
	}
	e.mu.Lock()
	delete(e.pendingResponses, seqNum)
	e.mu.Unlock()

	e.stateMu.Lock()
	e.workingState.KVStore = common.CopyStringMap(pending.state)
	e.workingState.Merkle = pending.merkle.Clone()
	e.workingState.MerkleRoot = pending.merkleRoot
	e.stateMu.Unlock()

	e.mu.Lock()
	e.stableState = State{
		KVStore:    common.CopyStringMap(pending.state),
		Merkle:     pending.merkle.Clone(),
		MerkleRoot: pending.merkleRoot,
		SeqNum:     seqNum,
		PrevHash:   agreedToken,
		Verified:   true,
	}
	e.storeCheckpoint(seqNum, agreedToken, pending.merkle, pending.merkleRoot)
	for batchSeq := range e.batchPayloads {
		if batchSeq <= seqNum {
			delete(e.batchPayloads, batchSeq)
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
}

func (e *Exec) rollbackWorkingToStable() {
	e.mu.Lock()
	e.stableState.EnsureMerkle()
	stableCopy := common.CopyStringMap(e.stableState.KVStore)
	stableMerkle := e.stableState.Merkle.Clone()
	stableRoot := e.stableState.MerkleRoot
	e.forceSequential = true
	e.mu.Unlock()

	e.stateMu.Lock()
	e.workingState.KVStore = stableCopy
	e.workingState.Merkle = stableMerkle
	e.workingState.MerkleRoot = stableRoot
	e.stateMu.Unlock()
}

func (e *Exec) ensureVerifyResponseTimerLocked(seqNum int) {
	if _, ok := e.verifyResponseTimers[seqNum]; ok {
		return
	}
	e.verifyResponseTimers[seqNum] = time.AfterFunc(e.verifyResponseTimeout, func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		if seqNum <= e.stableState.SeqNum {
			delete(e.verifyResponseTimers, seqNum)
			return
		}
		// Timeout fallback: force sequential path
		e.forceSequential = true
		delete(e.verifyResponseTimers, seqNum)
	})
}

func (e *Exec) stopVerifyResponseTimerLocked(seqNum int) {
	timer, ok := e.verifyResponseTimers[seqNum]
	if !ok || timer == nil {
		return
	}
	timer.Stop()
	delete(e.verifyResponseTimers, seqNum)
}

func (e *Exec) clearVerifyResponseTrackingLocked(seqNum int) {
	keys := e.verifyResponseBySeq[seqNum]
	for key := range keys {
		delete(e.verifyResponseMsgs, key)
	}
	delete(e.verifyResponseBySeq, seqNum)
}
