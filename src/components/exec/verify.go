package exec

import (
	"log"

	"aegean/common"
)

func (e *Exec) flushNextVerify() bool {
	e.mu.Lock()
	seq := e.nextVerifySeq
	pending, ok := e.pendingExecResults[seq]
	stableSeqNum := e.stableState.SeqNum
	prevHash := e.stableState.PrevHash
	view := e.view
	e.mu.Unlock()
	// Compute token with committed prevHash to avoid divergence for the next sequence.
	if ok && seq == stableSeqNum+1 && !pending.verifySent {
		if pending.merkle == nil {
			pending.merkle = NewMerkleTreeFromMap(pending.state)
			pending.merkleRoot = pending.merkle.Root()
		}
		token := e.computeStateHash(pending.merkleRoot, pending.outputs, prevHash, seq)
		pending.token = token
		pending.verifySent = true
		e.mu.Lock()
		// Guard against rollover while token was being computed
		if e.nextVerifySeq != seq || e.stableState.SeqNum != stableSeqNum {
			e.mu.Unlock()
			return false
		}
		e.pendingExecResults[seq] = pending
		e.mu.Unlock()

		// Broadcast verify request for this sequence to all verifiers
		verifyMsg := map[string]any{
			"type":      "verify",
			"view":      view,
			"seq_num":   seq,
			"token":     token,
			"prev_hash": prevHash,
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
	return false
}

func (e *Exec) finalizeCommit(seqNum int, pending pendingExecResult, agreedToken string) {
	log.Printf("%s: Committing seq_num %d", e.Name, seqNum)
	if pending.merkle == nil {
		pending.merkle = NewMerkleTreeFromMap(pending.state)
		pending.merkleRoot = pending.merkle.Root()
	}
	e.mu.Lock()
	delete(e.pendingExecResults, seqNum)
	e.mu.Unlock()

	e.stateMu.Lock()
	e.workingState.KVStore = common.CopyStringMap(pending.state)
	e.workingState.Merkle = pending.merkle.Clone()
	e.workingState.MerkleRoot = pending.merkleRoot
	e.stateMu.Unlock()

	e.mu.Lock()
	e.stableState = State{
		KVStore:    common.CopyStringMap(pending.state),
		Merkle:     pending.merkle.Clone(),
		MerkleRoot: pending.merkleRoot,
		SeqNum:     seqNum,
		PrevHash:   agreedToken,
		Verified:   true,
	}
	e.storeCheckpoint(seqNum, agreedToken, pending.merkle, pending.merkleRoot)
	for batchSeq := range e.replayableBatchInputs {
		if batchSeq <= seqNum {
			delete(e.replayableBatchInputs, batchSeq)
		}
	}
	e.forceSequential = false
	e.mu.Unlock()

	for _, output := range pending.outputs {
		requestID := output["request_id"]
		responseMsg := map[string]any{
			"type":       "response",
			"request_id": requestID,
			"response":   output,
		}
		if e.ShimCh != nil {
			e.ShimCh <- responseMsg
		}
	}
}
