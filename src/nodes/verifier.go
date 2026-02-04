// Package nodes contains node implementations.
// Translates: src_py/nodes/verifier.py
package nodes

import (
	"log"
	"sync"

	"aegean/common"
)

type Verifier struct {
	*Node
	Execs        []string
	u            int
	r            int
	execQuorum   int
	verifyQuorum int
	tokens       map[int]map[string]map[string]struct{}
	committed    map[int]string
	prevHashes   map[int]string
	mu           sync.Mutex
}

func NewVerifier(name, host string, port int, execs []string) *Verifier {
	v := &Verifier{
		Node:  NewNode(name, host, port),
		Execs: execs,
		// TODO: replace hard-coded values with formulas
		u:          1,
		r:          0,
		tokens:     make(map[int]map[string]map[string]struct{}),
		committed:  make(map[int]string),
		prevHashes: make(map[int]string),
	}
	v.execQuorum = maxInt(v.u, v.r) + 1
	v.verifyQuorum = 2*v.u + v.r + 1
	v.Node.HandleMessage = v.HandleMessage
	return v
}

func (v *Verifier) Start() {
	v.Node.Start()
}

func (v *Verifier) checkAgreement(seqNum int) (string, string) {
	tokenCounts := v.tokens[seqNum]

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

	if bestCount >= v.execQuorum {
		return "commit", bestToken
	}

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
		if _, err := common.SendMessage(execNode, 8000, response); err != nil {
			log.Printf("Failed to send to exec %s: %v", execNode, err)
		}
	}
}

func (v *Verifier) HandleMessage(payload map[string]any) map[string]any {
	log.Printf("Handler called on %s with payload: %v", v.Name, payload)

	seqNum := getInt(payload, "seq_num")
	token, _ := payload["token"].(string)
	prevHash, _ := payload["prev_hash"].(string)
	execID, _ := payload["exec_id"].(string)

	if seqNum > 1 {
		prevCommitted, ok := v.committed[seqNum-1]
		if ok && prevHash != prevCommitted {
			log.Printf("Verifier %s: Invalid prev_hash from %s", v.Name, execID)
			return map[string]any{"status": "invalid_prev_hash"}
		}
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if committedToken, ok := v.committed[seqNum]; ok {
		return map[string]any{"status": "already_committed", "token": committedToken}
	}

	if _, ok := v.tokens[seqNum]; !ok {
		v.tokens[seqNum] = make(map[string]map[string]struct{})
	}
	if _, ok := v.tokens[seqNum][token]; !ok {
		v.tokens[seqNum][token] = make(map[string]struct{})
	}
	v.tokens[seqNum][token][execID] = struct{}{}
	v.prevHashes[seqNum] = prevHash

	log.Printf("Verifier %s: seq=%d, token=%s..., from %s, count=%d", v.Name, seqNum, truncateToken(token), execID, len(v.tokens[seqNum][token]))

	decision, agreedToken := v.checkAgreement(seqNum)

	switch decision {
	case "commit":
		v.committed[seqNum] = agreedToken
		log.Printf("Verifier %s: COMMIT seq=%d", v.Name, seqNum)
		v.sendVerifyResponse(seqNum, "commit", agreedToken)
		delete(v.tokens, seqNum)
		return map[string]any{"status": "committed", "token": agreedToken}
	case "rollback":
		log.Printf("Verifier %s: ROLLBACK seq=%d", v.Name, seqNum)
		v.sendVerifyResponse(seqNum, "rollback", agreedToken)
		delete(v.tokens, seqNum)
		return map[string]any{"status": "rollback"}
	}

	return map[string]any{"status": "waiting", "count": len(v.tokens[seqNum][token])}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func truncateToken(token string) string {
	if len(token) <= 16 {
		return token
	}
	return token[:16]
}
