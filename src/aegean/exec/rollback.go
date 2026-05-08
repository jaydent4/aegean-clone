package exec

import (
	"log"
	"sort"

	"aegean/common"
)

func (e *Exec) rollbackTo(seqNum int, token string) bool {
	e.mu.Lock()
	checkpoint, ok := e.checkpoints[seqNum]
	if !ok || !e.validateRollbackCheckpoint(checkpoint, token) {
		e.mu.Unlock()
		log.Println("warning: invalid rollback checkpoint", ok, checkpoint.SeqNum, checkpoint.Token, checkpoint.ValidationHash, token)
		return false
	}

	replaySeqs := make([]int, 0)
	for batchSeq := range e.replayableBatchInputs {
		if batchSeq > seqNum {
			replaySeqs = append(replaySeqs, batchSeq)
		}
	}
	sort.Ints(replaySeqs)

	// Discard pending work above rollback point
	for pendingSeq := range e.pendingExecResults {
		if pendingSeq > seqNum {
			delete(e.pendingExecResults, pendingSeq)
		}
	}
	for _, replaySeq := range replaySeqs {
		e.ingressCh <- ingressEvent{kind: ingressBatchEvent, payload: e.replayableBatchInputs[replaySeq]}
	}
	e.nextBatchSeq = seqNum + 1
	e.nextVerifySeq = seqNum + 1
	checkpointState := common.CopyStringMap(checkpoint.State)
	e.stableState = State{
		KVStore:    checkpointState,
		Merkle:     nil,
		MerkleRoot: checkpoint.MerkleRoot,
		SeqNum:     checkpoint.SeqNum,
		PrevHash:   checkpoint.Token,
		Verified:   true,
	}
	e.forceSequential = true
	e.mu.Unlock()

	e.stateMu.Lock()
	e.workingState.KVStore = common.CopyStringMap(checkpointState)
	e.workingState.Merkle = nil
	e.workingState.MerkleRoot = checkpoint.MerkleRoot
	e.stateMu.Unlock()
	return true
}
