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
	_, recovering := e.recoveringTransfers[seq]
	e.mu.Unlock()

	// If this seq is mid-recovery, retry state transfer before further verify work
	if recovering {
		if e.requestStateTransferWithRetry(seq, 6, 10*time.Millisecond) {
			e.mu.Lock()
			delete(e.recoveringTransfers, seq)
			e.mu.Unlock()
			return true
		}
		return false
	}
	if !ok {
		return false
	}
	if seq != stableSeqNum+1 {
		return false
	}

	// Compute token with committed prevHash to avoid divergence
	if !pending.verifySent {
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

	// Consume buffered verify responses for this seq and apply quorum decisions
	msgs := e.verifyBuffer.Pop(seq)
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
	if resolved {
		// Advance verify pointer only after this sequence is fully resolved
		e.mu.Lock()
		if e.nextVerifySeq == seq {
			e.nextVerifySeq++
		}
		e.mu.Unlock()
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
	// Handle responses for already-committed sequence numbers (rollback-only path)
	if seqNum <= e.stableState.SeqNum {
		shouldRollback := view > e.view || forceSequential
		if !shouldRollback {
			e.mu.Unlock()
			return map[string]any{"status": "already_committed", "seq_num": seqNum}
		}

		// Track this vote and wait until quorum agrees on the same rollback tuple
		tupleKey := responseTupleKey(view, seqNum, agreedToken, forceSequential)
		e.verifyResponseMsgs[tupleKey] = payload
		if _, ok := e.verifyResponseBySeq[seqNum]; !ok {
			e.verifyResponseBySeq[seqNum] = make(map[string]struct{})
		}
		e.verifyResponseBySeq[seqNum][tupleKey] = struct{}{}
		reached := e.verifyResponseQuorum.Add(tupleKey, verifierID)
		if !reached {
			e.mu.Unlock()
			return map[string]any{"status": "waiting_quorum", "resolved": false}
		}

		// Quorum reached: clear tracking, advance view if needed, and release lock before recovery work
		e.clearVerifyResponseTrackingLocked(seqNum)
		if view > e.view {
			e.view = view
		}
		e.mu.Unlock()

		// Try rollback first; if unavailable, recover state via transfer and fall back to stable state on failure
		log.Printf("%s: applying rollback response at stable seq=%d view=%d token=%s", e.Name, seqNum, view, common.TruncateToken(agreedToken))
		if e.rollbackTo(seqNum, agreedToken) {
			e.mu.Lock()
			delete(e.recoveringTransfers, seqNum)
			e.mu.Unlock()
			return map[string]any{"status": "processed", "decision": "rollback", "resolved": true}
		}
		e.mu.Lock()
		e.recoveringTransfers[seqNum] = struct{}{}
		e.mu.Unlock()
		if e.requestStateTransferWithRetry(seqNum, 8, 10*time.Millisecond) {
			e.mu.Lock()
			e.forceSequential = true
			delete(e.recoveringTransfers, seqNum)
			e.mu.Unlock()
			return map[string]any{"status": "processed", "decision": "state_transfer", "resolved": true}
		}
		e.rollbackWorkingToStable()
		return map[string]any{"status": "processed", "decision": "rollback_fallback_retrying", "resolved": false}
	}

	// For future sequence numbers, require a pending candidate state to reconcile against
	pending, ok := e.pendingResponses[seqNum]
	if !ok {
		e.mu.Unlock()
		return map[string]any{"status": "no_pending_for_seq"}
	}

	// Track quorum votes for this (view, seq, token, force_sequential) tuple
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

	// Quorum reached: stop timers and clear temporary vote tracking
	e.stopVerifyResponseTimerLocked(seqNum)
	e.clearVerifyResponseTrackingLocked(seqNum)
	e.mu.Unlock()

	// Re-evaluate rollback requirement after quorum and update view when moving forward
	e.mu.Lock()
	shouldRollback := view > e.view || forceSequential
	if shouldRollback && view > e.view {
		e.view = view
	}
	e.mu.Unlock()
	if shouldRollback {
		// Execute rollback recovery flow for agreed rollback decision
		log.Printf("%s: quorum rollback response received for seq=%d view=%d token=%s", e.Name, seqNum, view, common.TruncateToken(agreedToken))
		e.mu.Lock()
		delete(e.pendingResponses, seqNum)
		e.mu.Unlock()
		if e.rollbackTo(seqNum, agreedToken) {
			e.mu.Lock()
			delete(e.recoveringTransfers, seqNum)
			e.mu.Unlock()
			return map[string]any{"status": "processed", "decision": "rollback", "resolved": true}
		}
		e.mu.Lock()
		e.recoveringTransfers[seqNum] = struct{}{}
		e.mu.Unlock()
		if e.requestStateTransferWithRetry(seqNum, 8, 10*time.Millisecond) {
			e.mu.Lock()
			e.forceSequential = true
			delete(e.recoveringTransfers, seqNum)
			e.mu.Unlock()
			return map[string]any{"status": "processed", "decision": "state_transfer", "resolved": true}
		}
		e.rollbackWorkingToStable()
		return map[string]any{"status": "processed", "decision": "rollback_fallback_retrying", "resolved": false}
	}

	// If token matches pending state, commit it as the next stable checkpoint
	if pending.token == agreedToken {
		e.finalizeCommit(seqNum, pending, agreedToken)
		return map[string]any{"status": "processed", "decision": "commit", "resolved": true}
	}

	// If token mismatches, recover by state transfer and retry from stable state if needed
	log.Printf("%s: state diverged at seq=%d, agreed token mismatch", e.Name, seqNum)
	e.mu.Lock()
	delete(e.pendingResponses, seqNum)
	e.recoveringTransfers[seqNum] = struct{}{}
	e.mu.Unlock()
	if e.requestStateTransferWithRetry(seqNum, 8, 10*time.Millisecond) {
		e.mu.Lock()
		delete(e.recoveringTransfers, seqNum)
		e.mu.Unlock()
		return map[string]any{"status": "processed", "decision": "state_transfer", "resolved": true}
	}
	e.rollbackWorkingToStable()
	return map[string]any{"status": "processed", "decision": "state_transfer_retrying", "resolved": false}
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
