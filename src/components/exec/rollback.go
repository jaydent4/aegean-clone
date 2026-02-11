package exec

import (
	"sort"

	"aegean/common"
)

func (e *Exec) rollbackTo(seqNum int, token string) bool {
	e.mu.Lock()
	checkpoint, ok := e.checkpoints[seqNum]
	if !ok || !e.validateRollbackCheckpoint(checkpoint, token) {
		e.mu.Unlock()
		return false
	}

	replaySeqs := make([]int, 0)
	for batchSeq := range e.batchPayloads {
		if batchSeq > seqNum {
			replaySeqs = append(replaySeqs, batchSeq)
		}
	}
	sort.Ints(replaySeqs)

	// Discard pending work above rollback point
	for pendingSeq := range e.pendingResponses {
		if pendingSeq > seqNum {
			delete(e.pendingResponses, pendingSeq)
		}
	}
	e.batchBuffer.Clear()
	e.verifyBuffer.Clear()
	for _, replaySeq := range replaySeqs {
		e.batchBuffer.Add(replaySeq, e.batchPayloads[replaySeq])
	}
	e.nextBatchSeq = seqNum + 1
	e.nextVerifySeq = seqNum + 1
	checkpointMerkle := checkpoint.Merkle.Clone()
	checkpointState := checkpointMerkle.SnapshotMap()
	e.stableState = State{
		KVStore:    checkpointState,
		Merkle:     checkpointMerkle,
		MerkleRoot: checkpoint.MerkleRoot,
		SeqNum:     checkpoint.SeqNum,
		PrevHash:   checkpoint.Token,
		Verified:   true,
	}
	e.forceSequential = true
	e.mu.Unlock()

	e.stateMu.Lock()
	e.workingState.KVStore = common.CopyStringMap(checkpointState)
	e.workingState.Merkle = checkpointMerkle.Clone()
	e.workingState.MerkleRoot = checkpoint.MerkleRoot
	e.stateMu.Unlock()
	return true
}
