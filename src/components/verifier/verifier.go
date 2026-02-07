package verifier

import (
	"log"
	"sync"

	"aegean/common"
)

type Verifier struct {
	Name  string
	Execs []string
	// Local component channel
	ExecCh    chan<- map[string]any
	LocalName string
	// TODO: replace hard-coded values with formulas
	// Fault tolerance parameters (simplified: u=1, r=0 for CFT)
	u int
	r int
	// Quorum sizes
	execQuorum   int
	verifyQuorum int
	// State tracking per sequence number
	state *VerifyState
	mu    sync.Mutex
	// Out-of-order buffer for verify messages
	verifyBuffer *common.OOOBuffer[map[string]any]
}

func NewVerifier(name string, execs []string, localName string, execCh chan<- map[string]any) *Verifier {
	if execCh == nil {
		log.Fatalf("verifier component requires non-nil execCh")
	}
	if localName == "" {
		log.Fatalf("verifier component requires localName")
	}
	v := &Verifier{
		Name:      name,
		Execs:     execs,
		LocalName: localName,
		ExecCh:    execCh,
		// TODO: replace hard-coded values with formulas
		u:            1,
		r:            0,
		state:        NewVerifyState(),
		verifyBuffer: common.NewOOOBuffer[map[string]any](),
	}
	v.execQuorum = common.MaxInt(v.u, v.r) + 1
	v.verifyQuorum = 2*v.u + v.r + 1
	return v
}

func (v *Verifier) checkAgreement(seqNum int) (string, string) {
	tokenCounts := v.state.TokenCounts(seqNum)

	// Find token with most support
	bestToken := ""
	bestCount := 0
	for token, execIDs := range tokenCounts {
		if len(execIDs) > bestCount {
			bestCount = len(execIDs)
			bestToken = token
		}
	}

	totalResponses := 0
	for _, execIDs := range tokenCounts {
		totalResponses += len(execIDs)
	}

	// If we have quorum of matching tokens -> commit
	if bestCount >= v.execQuorum {
		return "commit", bestToken
	}

	// If we've heard from all execs and no quorum -> rollback
	if totalResponses >= len(v.Execs) {
		log.Printf("Verifier %s: Divergence detected for seq %d", v.Name, seqNum)
		return "rollback", bestToken
	}

	return "", ""
}

func (v *Verifier) sendVerifyResponse(seqNum int, decision, token string) {
	response := map[string]any{
		"type":         "verify_response",
		"seq_num":      seqNum,
		"decision":     decision,
		"token":        token,
		"view_changed": decision == "rollback",
	}

	for _, execNode := range v.Execs {
		if execNode == v.LocalName && v.ExecCh != nil {
			v.ExecCh <- response
			continue
		}
		if _, err := common.SendMessage(execNode, 8000, response); err != nil {
			log.Printf("Failed to send to exec %s: %v", execNode, err)
		}
	}
}

// TODO: Any of out-of-order issues?
// TODO: State transfer is unimplemented?
// TODO: Implement more locking & race condition analysis
// TODO: Remove unneeded debug lines
func (v *Verifier) HandleVerifyMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", v.Name, payload)

	seqNum := common.GetInt(payload, "seq_num")
	prevHash, _ := payload["prev_hash"].(string)

	// Buffer if we do not yet have the previous commit hash to validate against
	if seqNum > 1 && prevHash != "" {
		// This lock is to prevent: seq N verify arrives but not added to buffer, seq N - 1 arrives, flushes N - 1 but not N,
		// seq N added to buffer and never gets flushed
		v.mu.Lock()
		committed := v.state.HasCommitted(seqNum - 1)
		if !committed {
			v.verifyBuffer.Add(seqNum, payload)
			log.Printf("Verifier %s: Buffered seq=%d (waiting for commit of seq=%d)", v.Name, seqNum, seqNum-1)
			v.mu.Unlock()
			return map[string]any{"status": "buffered", "seq_num": seqNum}
		}
		v.mu.Unlock()
	}

	resp := v.applyVerifyMessage(payload)
	if status, _ := resp["status"].(string); status == "committed" || status == "rollback" {
		v.flushBufferedFrom(seqNum)
	}
	return resp
}

func (v *Verifier) flushBufferedFrom(seqNum int) {
	log.Printf("Verifier %s: Flush buffered from seq=%d", v.Name, seqNum)
	next := seqNum + 1
	for {
		if next > 1 {
			v.mu.Lock()
			committed := v.state.HasCommitted(next - 1)
			v.mu.Unlock()
			if !committed {
				log.Printf("Verifier %s: Flush stopped at seq=%d (missing commit for seq=%d)", v.Name, next, next-1)
				return
			}
		}
		msgs := v.verifyBuffer.Pop(next)
		if len(msgs) == 0 {
			log.Printf("Verifier %s: No buffered messages for seq=%d", v.Name, next)
			return
		}
		log.Printf("Verifier %s: Flushing %d buffered message(s) for seq=%d", v.Name, len(msgs), next)
		var lastResp map[string]any
		for _, msg := range msgs {
			lastResp = v.applyVerifyMessage(msg)
		}
		if lastResp == nil {
			return
		}
		status, _ := lastResp["status"].(string)
		if status == "waiting" || status == "invalid_prev_hash" {
			log.Printf("Verifier %s: Flush halted at seq=%d (status=%s)", v.Name, next, status)
			return
		}
		if status == "committed" || status == "rollback" || status == "already_committed" {
			next++
			continue
		}
		return
	}
}

func (v *Verifier) applyVerifyMessage(payload map[string]any) map[string]any {
	seqNum := common.GetInt(payload, "seq_num")
	token, _ := payload["token"].(string)
	prevHash, _ := payload["prev_hash"].(string)
	execID, _ := payload["exec_id"].(string)

	if seqNum > 1 {
		// Validate prev_hash matches what we expect (if we have committed seq_num-1)
		prevCommitted, ok := v.state.CommittedToken(seqNum - 1)
		if ok && prevHash != prevCommitted {
			log.Printf("Verifier %s: Invalid prev_hash from %s", v.Name, execID)
			return map[string]any{"status": "invalid_prev_hash"}
		}
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	// Already committed this seq_num?
	if committedToken, ok := v.state.CommittedToken(seqNum); ok {
		return map[string]any{"status": "already_committed", "token": committedToken}
	}

	// Record this token
	v.state.RecordToken(seqNum, token, execID, prevHash)

	tokenCounts := v.state.TokenCounts(seqNum)
	log.Printf("Verifier %s: seq=%d, token=%s..., from %s, count=%d", v.Name, seqNum, common.TruncateToken(token), execID, len(tokenCounts[token]))

	// Check if we can reach agreement
	decision, agreedToken := v.checkAgreement(seqNum)

	switch decision {
	case "commit":
		v.state.SetCommitted(seqNum, agreedToken)
		log.Printf("Verifier %s: COMMIT seq=%d", v.Name, seqNum)
		v.sendVerifyResponse(seqNum, "commit", agreedToken)
		// Cleanup
		v.state.DeleteTokens(seqNum)
		return map[string]any{"status": "committed", "token": agreedToken}
	case "rollback":
		log.Printf("Verifier %s: ROLLBACK seq=%d", v.Name, seqNum)
		v.sendVerifyResponse(seqNum, "rollback", agreedToken)
		// Cleanup
		v.state.DeleteTokens(seqNum)
		return map[string]any{"status": "rollback"}
	}

	return map[string]any{"status": "waiting", "count": len(tokenCounts[token])}
}
