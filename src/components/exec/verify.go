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
		log.Printf(
			"%s: assembled verify hash seq_num=%d view_num=%d stable_seq_num=%d prev_hash=%s state_root=%s final_hash=%s output_count=%d outputs=%v verifiers=%v",
			e.Name,
			seq,
			view,
			stableSeqNum,
			shortHash(prevHash),
			shortHash(pending.merkleRoot),
			shortHash(token),
			len(pending.outputs),
			pending.outputs,
			e.Verifiers,
		)
		if logExecStateDetails {
			log.Printf(
				"%s: assembled verify hash state seq_num=%d view_num=%d state=%v",
				e.Name,
				seq,
				view,
				pending.state,
			)
		}

		for _, verifier := range e.Verifiers {
			if verifier == e.Name && e.VerifierCh != nil {
				e.VerifierCh <- verifyMsg
				continue
			}
			_, _ = common.SendMessage(verifier, 8000, verifyMsg)
		}
		return true
	}
	return false
}

func (e *Exec) finalizeCommit(seqNum int, pending pendingExecResult, agreedToken string) {
	if pending.merkle == nil {
		pending.merkle = NewMerkleTreeFromMap(pending.state)
		pending.merkleRoot = pending.merkle.Root()
	}
	e.mu.Lock()
	delete(e.pendingExecResults, seqNum)
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
	// Important: do not always reset workingState to the just-committed snapshot.
	// If seq>seqNum already executed speculatively, resetting to committed state
	// would discard those speculative writes and make later verify tokens diverge
	// from peers (leading to unnecessary state transfer/rollback cycles).
	workingKV, workingMerkle, workingRoot := e.selectWorkingStateAfterCommitLocked(pending)
	e.mu.Unlock()

	e.stateMu.Lock()
	e.workingState.KVStore = workingKV
	e.workingState.Merkle = workingMerkle
	e.workingState.MerkleRoot = workingRoot
	e.stateMu.Unlock()

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

// Rebase workingState to the highest pending speculative snapshot
// This is necessary because the workingState of seq=n+1 may not capture the results from seq=n
func (e *Exec) selectWorkingStateAfterCommitLocked(committed pendingExecResult) (map[string]string, *MerkleTree, string) {
	maxSeq := -1
	var tip pendingExecResult
	for seq, candidate := range e.pendingExecResults {
		if seq > maxSeq {
			maxSeq = seq
			tip = candidate
		}
	}

	if maxSeq >= 0 {
		if tip.merkle == nil {
			tip.merkle = NewMerkleTreeFromMap(tip.state)
			tip.merkleRoot = tip.merkle.Root()
			e.pendingExecResults[maxSeq] = tip
		}
		return common.CopyStringMap(tip.state), tip.merkle.Clone(), tip.merkleRoot
	}

	return common.CopyStringMap(committed.state), committed.merkle.Clone(), committed.merkleRoot
}
