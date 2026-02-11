package exec

import (
	"log"
	"time"

	"aegean/common"
)

func (e *Exec) flushNextVerifyResponse() bool {
	// Consume buffered verify responses prioritized by highest view then earliest seq.
	msgView, msgSeq, ok := e.verifyBuffer.PeekNext()
	if !ok {
		return false
	}
	e.mu.Lock()
	stableSeqNum := e.stableState.SeqNum
	_, hasPending := e.pendingExecResults[msgSeq]
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
	pending, hasPending := e.pendingExecResults[seqNum]
	if seqNum > stableSeqNum && !hasPending {
		e.mu.Unlock()
		return map[string]any{"status": "no_pending_for_seq", "resolved": false}
	}

	tupleKey := responseTupleKey(view, seqNum, agreedToken, forceSequential)
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
		delete(e.pendingExecResults, seqNum)
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
		delete(e.pendingExecResults, seqNum)
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
	delete(e.verifyResponseBySeq, seqNum)
}
