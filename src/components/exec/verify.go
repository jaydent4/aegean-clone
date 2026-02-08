package exec

import (
	"log"

	"aegean/common"
)

func (e *Exec) flushNextVerify() bool {
	e.mu.Lock()
	pending, ok := e.pendingResponses[e.nextVerifySeq]
	stableSeqNum := e.stableState.SeqNum
	e.mu.Unlock()
	if !ok {
		return false
	}
	if e.nextVerifySeq != stableSeqNum+1 {
		return false
	}
	// Compute token with committed prevHash to avoid divergence
	if !pending.verifySent {
		token := e.computeStateHash(pending.state, pending.outputs, e.stableState.PrevHash, e.nextVerifySeq)
		pending.token = token
		pending.verifySent = true
		e.mu.Lock()
		e.pendingResponses[e.nextVerifySeq] = pending
		e.mu.Unlock()

		verifyMsg := map[string]any{
			"type":      "verify",
			"seq_num":   e.nextVerifySeq,
			"token":     token,
			"prev_hash": e.stableState.PrevHash,
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
	msgs := e.verifyBuffer.Pop(e.nextVerifySeq)
	if len(msgs) == 0 {
		return false
	}
	for _, msg := range msgs {
		_ = e.handleVerifyResponse(msg)
	}
	e.nextVerifySeq++
	return true
}

func (e *Exec) handleVerifyResponse(payload map[string]any) map[string]any {
	decision, _ := payload["decision"].(string)
	seqNum := common.GetInt(payload, "seq_num")
	agreedToken, _ := payload["token"].(string)

	e.mu.Lock()
	if seqNum <= e.stableState.SeqNum {
		e.mu.Unlock()
		return map[string]any{"status": "already_committed", "seq_num": seqNum}
	}
	pending, ok := e.pendingResponses[seqNum]
	if !ok {
		e.mu.Unlock()
		return map[string]any{"status": "no_pending_for_seq"}
	}

	// Handle verification response from verifier
	switch decision {
	case "commit":
		if pending.token == agreedToken {
			// Mark state as stable and release responses
			log.Printf("%s: Committing seq_num %d", e.Name, seqNum)
			e.stableState = State{
				KVStore:  common.CopyStringMap(pending.state),
				SeqNum:   seqNum,
				PrevHash: agreedToken,
				Verified: true,
			}
			e.forceSequential = false
			delete(e.pendingResponses, seqNum)
			outputs := pending.outputs
			e.mu.Unlock()

			// Send responses back to the server-shim for broadcasting to clients
			for _, output := range outputs {
				requestID := output["request_id"]
				responseMsg := map[string]any{
					"type":       "response",
					"request_id": requestID,
					"response":   output,
				}
				if e.ShimCh != nil {
					e.ShimCh <- responseMsg
				}
				log.Printf("%s: Sent response for request %v to shim", e.Name, requestID)
			}
			return map[string]any{"status": "processed", "decision": decision}
		}
		delete(e.pendingResponses, seqNum)
		e.mu.Unlock()
		// TODO: rollback? (I guess we need to introduce parallel pipelining first)
		// Our state diverged - need state transfer from a replica with correct state
		log.Printf("%s: State diverged at seq_num %d, requesting state transfer", e.Name, seqNum)
		if e.requestStateTransfer() {
			log.Printf("%s: State transfer successful for seq_num %d", e.Name, seqNum)
		} else {
			// If state transfer fails, fall back to rollback
			log.Printf("%s: State transfer failed, rolling back", e.Name)
			e.workingState.KVStore = common.CopyStringMap(e.stableState.KVStore)
			e.forceSequential = true
		}
	case "rollback":
		log.Printf("%s: Rolling back to seq_num %d", e.Name, e.stableState.SeqNum)
		e.workingState.KVStore = common.CopyStringMap(e.stableState.KVStore)
		e.forceSequential = true
		delete(e.pendingResponses, seqNum)
		e.mu.Unlock()
		log.Printf("%s: Will execute next batch sequentially", e.Name)
		return map[string]any{"status": "processed", "decision": decision}
	default:
		e.mu.Unlock()
	}

	// Cleanup
	return map[string]any{"status": "processed", "decision": decision}
}
