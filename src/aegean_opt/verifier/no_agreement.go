package verifier

func (v *Verifier) highestPreparedLocked(view int) (int, string, bool) {
	bestSeq := -1
	bestToken := ""
	for seq, slot := range v.slots {
		if slot.view != view || slot.preparedToken == "" {
			continue
		}
		if votes, ok := slot.prepareVotes[slot.preparedToken]; !ok || len(votes) < v.phaseQuorum {
			continue
		}
		if seq > bestSeq {
			bestSeq = seq
			bestToken = slot.preparedToken
		}
	}
	if bestSeq < 0 {
		return 0, "", false
	}
	return bestSeq, bestToken, true
}

func (v *Verifier) handleNoAgreement(seqNum int, timerView int) {
	v.mu.Lock()
	if timerView != v.view {
		v.mu.Unlock()
		return
	}
	if _, committed := v.committedToken(seqNum); committed {
		v.mu.Unlock()
		return
	}
	targetView := v.view + 1
	candidate := v.candidateFromLocalLocked(v.view)
	v.mu.Unlock()

	// Paper mentions throughput-based trigger; currently timeout-only per user request.
	_ = v.applyViewChangeMessage(map[string]any{
		"type":         "view_change",
		"target_view":  targetView,
		"verifier_id":  v.Name,
		"prepared_seq": candidate.SeqNum,
		"token":        candidate.Token,
	})
	go v.sendViewChange(targetView, candidate)
}
