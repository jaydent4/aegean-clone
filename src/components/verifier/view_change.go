package verifier

import (
	"sort"

	"aegean/common"
)

type preparedCandidate struct {
	SeqNum int
	Token  string
}

type viewChangeReport struct {
	From        string
	TargetView  int
	PreparedSeq int
	Token       string
}

type newViewCertificate struct {
	TargetView int
	RollbackN  int
	RollbackT  string
	Reports    map[string]viewChangeReport
}

type viewChangeRound struct {
	targetView int
	reports    map[string]viewChangeReport
	cert       *newViewCertificate
}

func (v *Verifier) isViewPrimary(view int) bool {
	if len(v.Verifiers) == 0 {
		return false
	}
	idx := (view - 1) % len(v.Verifiers)
	if idx < 0 {
		idx = 0
	}
	return v.Verifiers[idx] == v.Name
}

func (v *Verifier) candidateFromLocalLocked(currentView int) preparedCandidate {
	n, token, ok := v.highestPreparedLocked(currentView)
	if ok {
		return preparedCandidate{SeqNum: n, Token: token}
	}

	bestSeq := 0
	bestToken := ""
	for seq, committedToken := range v.committed {
		if seq > bestSeq {
			bestSeq = seq
			bestToken = committedToken
		}
	}
	return preparedCandidate{SeqNum: bestSeq, Token: bestToken}
}

func (v *Verifier) sendViewChange(targetView int, cand preparedCandidate) {
	msg := map[string]any{
		"type":         "view_change",
		"target_view":  targetView,
		"verifier_id":  v.Name,
		"prepared_seq": cand.SeqNum,
		"token":        cand.Token,
	}
	v.sendToVerifiers(msg)
}

func (v *Verifier) ensureViewChangeRoundLocked(targetView int) *viewChangeRound {
	round, ok := v.viewChangeRounds[targetView]
	if !ok {
		round = &viewChangeRound{
			targetView: targetView,
			reports:    make(map[string]viewChangeReport),
		}
		v.viewChangeRounds[targetView] = round
	}
	return round
}

func (v *Verifier) maybeEmitNewViewLocked(targetView int) {
	if !v.isViewPrimary(targetView) {
		return
	}
	round := v.ensureViewChangeRoundLocked(targetView)
	if round.cert != nil {
		return
	}
	if len(round.reports) < v.phaseQuorum {
		return
	}

	bestSeq := 0
	bestToken := ""
	reportKeys := make([]string, 0, len(round.reports))
	for reporter := range round.reports {
		reportKeys = append(reportKeys, reporter)
	}
	sort.Strings(reportKeys)
	for _, reporter := range reportKeys {
		report := round.reports[reporter]
		if report.PreparedSeq > bestSeq {
			bestSeq = report.PreparedSeq
			bestToken = report.Token
		}
	}

	cert := &newViewCertificate{
		TargetView: targetView,
		RollbackN:  bestSeq,
		RollbackT:  bestToken,
		Reports:    make(map[string]viewChangeReport),
	}
	for reporter, report := range round.reports {
		cert.Reports[reporter] = report
	}
	round.cert = cert

	reports := make([]map[string]any, 0, len(cert.Reports))
	for _, report := range cert.Reports {
		reports = append(reports, map[string]any{
			"verifier_id":  report.From,
			"target_view":  report.TargetView,
			"prepared_seq": report.PreparedSeq,
			"token":        report.Token,
		})
	}
	msg := map[string]any{
		"type":        "new_view",
		"view":        cert.TargetView,
		"rollback_n":  cert.RollbackN,
		"token":       cert.RollbackT,
		"verifier_id": v.Name,
		"reports":     reports,
	}
	v.sendToVerifiers(msg)
	// Also apply locally without re-entering locks.
	if cert.TargetView > v.view {
		v.view = cert.TargetView
		v.stopTimerLocked(cert.RollbackN)
		go v.sendVerifyResponse(cert.RollbackN, cert.TargetView, cert.RollbackT, true)
	}
}

func (v *Verifier) applyViewChangeMessage(payload map[string]any) map[string]any {
	targetView := common.GetInt(payload, "target_view")
	from, _ := payload["verifier_id"].(string)
	preparedSeq := common.GetInt(payload, "prepared_seq")
	token, _ := payload["token"].(string)
	if targetView <= 0 || from == "" {
		return map[string]any{"status": "invalid_view_change"}
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	if targetView < v.view {
		return map[string]any{"status": "stale_view_change"}
	}

	round := v.ensureViewChangeRoundLocked(targetView)
	round.reports[from] = viewChangeReport{
		From:        from,
		TargetView:  targetView,
		PreparedSeq: preparedSeq,
		Token:       token,
	}
	v.maybeEmitNewViewLocked(targetView)
	return map[string]any{"status": "view_change_recorded"}
}

func (v *Verifier) applyNewViewMessage(payload map[string]any) map[string]any {
	newView, ok := parseView(payload)
	rollbackN := common.GetInt(payload, "rollback_n")
	token, _ := payload["token"].(string)
	reports, _ := payload["reports"].([]any)
	if !ok || newView <= 0 {
		return map[string]any{"status": "invalid_new_view"}
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	if newView <= v.view {
		return map[string]any{"status": "stale_new_view"}
	}

	// Simple certificate check: require quorum-sized report bundle.
	if len(reports) < v.phaseQuorum {
		return map[string]any{"status": "insufficient_new_view_certificate"}
	}

	v.view = newView
	v.stopTimerLocked(rollbackN)
	go v.sendVerifyResponse(rollbackN, newView, token, true)
	return map[string]any{"status": "new_view_applied", "view": newView}
}
