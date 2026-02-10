package verifier

import (
	"log"

	"aegean/common"
)

func (v *Verifier) applyPrepareMessage(payload map[string]any) map[string]any {
	seqNum := common.GetInt(payload, "seq_num")
	token, _ := payload["token"].(string)
	verifierID, _ := payload["verifier_id"].(string)
	msgView, ok := parseView(payload)
	if !ok || seqNum <= 0 || token == "" || verifierID == "" {
		return map[string]any{"status": "invalid_prepare"}
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if msgView < v.view {
		return map[string]any{"status": "stale_view", "view": v.view}
	}
	if msgView > v.view {
		v.view = msgView
	}

	if _, committed := v.committedToken(seqNum); committed {
		return map[string]any{"status": "already_committed"}
	}

	slot := v.slotForLocked(seqNum, v.view)
	v.maybeStartOrResetTimerLocked(seqNum, v.view)
	count := addVote(slot.prepareVotes, token, verifierID)
	if count < v.phaseQuorum {
		return map[string]any{"status": "waiting_prepare", "count": count}
	}

	if slot.preparedToken != "" && slot.preparedToken != token {
		return map[string]any{"status": "conflicting_prepared", "token": slot.preparedToken}
	}
	if slot.preparedToken == "" {
		slot.preparedToken = token
	}

	commitCount := addVote(slot.commitVotes, token, v.Name)
	commitMsg := map[string]any{
		"type":        "commit",
		"view":        v.view,
		"seq_num":     seqNum,
		"token":       token,
		"verifier_id": v.Name,
	}
	go v.sendToVerifiers(commitMsg)
	log.Printf("Verifier %s: PREPARED seq=%d view=%d token=%s", v.Name, seqNum, v.view, common.TruncateToken(token))

	if commitCount >= v.phaseQuorum && slot.committedToken == "" {
		slot.committedToken = token
		v.setCommitted(seqNum, token)
		v.stopTimerLocked(seqNum)
		go v.sendVerifyResponse(seqNum, v.view, token, false)
		go v.flushBufferedFrom(seqNum)
	}
	return map[string]any{"status": "prepared", "count": count}
}
