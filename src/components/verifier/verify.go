package verifier

import (
	"log"

	"aegean/common"
)

func (v *Verifier) flushNextVerify() bool {
	view, seqNum, ok := v.verifyBuffer.PeekNext()
	if !ok {
		return false
	}

	// Always prioritize highest-view traffic; within that, process earliest seq first.
	v.mu.Lock()
	currentView := v.view
	if view < currentView {
		v.mu.Unlock()
		_ = v.verifyBuffer.Pop(view, seqNum)
		return true
	}
	if seqNum > 1 {
		if _, committed := v.committedToken(seqNum - 1); !committed {
			v.mu.Unlock()
			return false
		}
	}
	v.mu.Unlock()

	msgs := v.verifyBuffer.Pop(view, seqNum)
	if len(msgs) == 0 {
		return false
	}
	for _, msg := range msgs {
		_ = v.applyVerifyMessage(msg)
	}
	return true
}

func (v *Verifier) applyVerifyMessage(payload map[string]any) map[string]any {
	seqNum := common.GetInt(payload, "seq_num")
	token, _ := payload["token"].(string)
	prevHash, _ := payload["prev_hash"].(string)
	execID, _ := payload["exec_id"].(string)
	msgView, ok := parseView(payload)

	if !ok || seqNum <= 0 || token == "" || execID == "" {
		return map[string]any{"status": "invalid_verify"}
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if msgView < v.view {
		return map[string]any{"status": "stale_view", "view": v.view}
	}
	if msgView > v.view {
		v.view = msgView
	}

	if committedToken, ok := v.committedToken(seqNum); ok {
		return map[string]any{"status": "already_committed", "token": committedToken}
	}

	if seqNum > 1 {
		prevCommitted, ok := v.committedToken(seqNum - 1)
		if !ok {
			return map[string]any{"status": "waiting_prev_commit", "seq_num": seqNum}
		}
		if prevHash != prevCommitted {
			return map[string]any{"status": "invalid_prev_hash"}
		}
	}

	slot := v.slotForLocked(seqNum, v.view)
	v.maybeStartOrResetTimerLocked(seqNum, v.view)

	count := addVote(slot.verifyVotes, token, execID)
	if count < v.execVerifyQuorum {
		maxVotes, uniqueExecSenders := verifyVoteStats(slot.verifyVotes)
		expectedExecVotes := 2*v.u + v.r + 1
		if uniqueExecSenders >= expectedExecVotes && maxVotes < v.execVerifyQuorum {
			// All expected exec replies arrived, but no token can reach quorum.
			// Trigger immediate no-agreement instead of waiting for timeout.
			v.stopTimerLocked(seqNum)
			go v.handleNoAgreement(seqNum, v.view)
			return map[string]any{"status": "no_agreement_fast_path", "count": count}
		}
		return map[string]any{"status": "waiting", "count": count}
	}

	if slot.prepreparedToken != "" && slot.prepreparedToken != token {
		return map[string]any{"status": "conflicting_preprepare", "token": slot.prepreparedToken}
	}
	if slot.prepreparedToken == "" {
		slot.prepreparedToken = token
	}

	prepareCount := addVote(slot.prepareVotes, token, v.Name)
	prepareMsg := map[string]any{
		"type":        "prepare",
		"view":        v.view,
		"seq_num":     seqNum,
		"token":       token,
		"verifier_id": v.Name,
	}
	go v.sendToVerifiers(prepareMsg)
	log.Printf("Verifier %s: PREPREPARED seq=%d view=%d token=%s", v.Name, seqNum, v.view, common.TruncateToken(token))

	// Fast-path local transition when quorum size is 1.
	if prepareCount >= v.phaseQuorum && slot.preparedToken == "" {
		slot.preparedToken = token
		commitCount := addVote(slot.commitVotes, token, v.Name)
		commitMsg := map[string]any{
			"type":        "commit",
			"view":        v.view,
			"seq_num":     seqNum,
			"token":       token,
			"verifier_id": v.Name,
		}
		go v.sendToVerifiers(commitMsg)
		if commitCount >= v.phaseQuorum && slot.committedToken == "" {
			slot.committedToken = token
			v.setCommitted(seqNum, token)
			v.stopTimerLocked(seqNum)
			go v.sendVerifyResponse(seqNum, v.view, token, false)
			go v.flushNextVerify()
		}
	}

	return map[string]any{"status": "preprepared", "count": count}
}
