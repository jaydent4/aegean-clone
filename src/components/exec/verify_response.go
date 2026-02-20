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
	currentView := e.view
	shouldRollback := view > currentView || forceSequential
	if shouldRollback && view > e.view {
		e.view = view
	}
	if shouldRollback && hasPending {
		delete(e.pendingExecResults, seqNum)
	}
	e.mu.Unlock()

	// Case 1: rollback (view increased or forced sequential by verifier).
	if shouldRollback {
		log.Printf(
			"%s: verifier decision=rollback seq_num=%d view_num=%d local_view=%d stable_seq_num=%d verifier_id=%s force_sequential=%t token=%s had_pending=%t pending_token=%s",
			e.Name,
			seqNum,
			view,
			currentView,
			stableSeqNum,
			verifierID,
			forceSequential,
			shortHash(agreedToken),
			hasPending,
			shortHash(pending.token),
		)
		if logExecStateDetails {
			decisionState := e.stableStateSnapshotForLog()
			if hasPending {
				decisionState = common.CopyStringMap(pending.state)
			}
			log.Printf(
				"%s: verifier decision state seq_num=%d decision=rollback state=%v",
				e.Name,
				seqNum,
				decisionState,
			)
		}
		if e.rollbackTo(seqNum, agreedToken) {
			return map[string]any{"status": "processed", "decision": "rollback", "resolved": true}
		}
		// If rollback fails, repair
		log.Printf(
			"%s: verifier decision=state_transfer seq_num=%d view_num=%d reason=rollback_failed verifier_id=%s token=%s",
			e.Name,
			seqNum,
			view,
			verifierID,
			shortHash(agreedToken),
		)
		if logExecStateDetails {
			log.Printf(
				"%s: verifier decision state seq_num=%d decision=state_transfer reason=rollback_failed state=%v",
				e.Name,
				seqNum,
				e.stableStateSnapshotForLog(),
			)
		}
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
		log.Printf(
			"%s: verifier decision=state_transfer seq_num=%d view_num=%d stable_seq_num=%d verifier_id=%s reason=token_mismatch local_token=%s agreed_token=%s",
			e.Name,
			seqNum,
			view,
			stableSeqNum,
			verifierID,
			shortHash(pending.token),
			shortHash(agreedToken),
		)
		if logExecStateDetails {
			log.Printf(
				"%s: verifier decision state seq_num=%d decision=state_transfer reason=token_mismatch state=%v",
				e.Name,
				seqNum,
				common.CopyStringMap(pending.state),
			)
		}
		e.mu.Lock()
		delete(e.pendingExecResults, seqNum)
		e.mu.Unlock()
		// Note: calling this will implicitly act as a global stall (because processMu), until state transfer is complete
		e.requestStateTransferWithRetry(seqNum, 0, 10*time.Millisecond)
		return map[string]any{"status": "processed", "decision": "state_transfer", "resolved": true}
	}

	// Case 3: normal commit (same view, token match).
	log.Printf(
		"%s: verifier decision=commit seq_num=%d view_num=%d stable_seq_num=%d verifier_id=%s force_sequential=%t token=%s output_count=%d",
		e.Name,
		seqNum,
		view,
		stableSeqNum,
		verifierID,
		forceSequential,
		shortHash(agreedToken),
		len(pending.outputs),
	)
	if logExecStateDetails {
		log.Printf(
			"%s: verifier decision state seq_num=%d decision=commit state=%v",
			e.Name,
			seqNum,
			common.CopyStringMap(pending.state),
		)
	}
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

func (e *Exec) stableStateSnapshotForLog() map[string]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return common.CopyStringMap(e.stableState.KVStore)
}
