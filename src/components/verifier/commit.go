package verifier

import "aegean/common"

func (v *Verifier) applyCommitMessage(payload map[string]any) map[string]any {
	seqNum := common.GetInt(payload, "seq_num")
	token, _ := payload["token"].(string)
	verifierID, _ := payload["verifier_id"].(string)
	msgView, ok := parseView(payload)
	if !ok || seqNum <= 0 || token == "" || verifierID == "" {
		return map[string]any{"status": "invalid_commit"}
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if msgView < v.view {
		return map[string]any{"status": "stale_view", "view": v.view}
	}
	if msgView > v.view {
		v.view = msgView
	}

	if committedToken, committed := v.committedToken(seqNum); committed {
		return map[string]any{"status": "already_committed", "token": committedToken}
	}

	slot := v.slotForLocked(seqNum, v.view)
	v.maybeStartOrResetTimerLocked(seqNum, v.view)
	count := addVote(slot.commitVotes, token, verifierID)
	if count < v.phaseQuorum {
		return map[string]any{"status": "waiting_commit", "count": count}
	}

	if slot.committedToken != "" && slot.committedToken != token {
		return map[string]any{"status": "conflicting_committed", "token": slot.committedToken}
	}
	slot.committedToken = token
	v.setCommitted(seqNum, token)
	v.stopTimerLocked(seqNum)

	go v.sendVerifyResponse(seqNum, v.view, token, false)
	go v.flushNextVerify()

	return map[string]any{"status": "committed", "token": token}
}
