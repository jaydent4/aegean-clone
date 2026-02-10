package exec

import "aegean/common"

func (e *Exec) rollbackTo(seqNum int, token string) bool {
	e.mu.Lock()
	checkpoint, ok := e.checkpoints[seqNum]
	if !ok || !e.validateRollbackCheckpoint(checkpoint, token) {
		e.mu.Unlock()
		return false
	}

	// Discard pending work above rollback point
	for pendingSeq := range e.pendingResponses {
		if pendingSeq > seqNum {
			delete(e.pendingResponses, pendingSeq)
		}
	}
	e.batchBuffer.Clear()
	e.verifyBuffer.Clear()
	e.nextBatchSeq = seqNum + 1
	e.nextVerifySeq = seqNum + 1
	e.stableState = State{
		KVStore:  common.CopyStringMap(checkpoint.State),
		SeqNum:   checkpoint.SeqNum,
		PrevHash: checkpoint.Token,
		Verified: true,
	}
	e.forceSequential = true
	e.mu.Unlock()

	e.stateMu.Lock()
	e.workingState.KVStore = common.CopyStringMap(checkpoint.State)
	e.stateMu.Unlock()
	return true
}
