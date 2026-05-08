package exec

import "aegean/common"

type rollbackCheckpoint struct {
	SeqNum         int
	Token          string
	State          map[string]string
	MerkleRoot     string
	ValidationHash string
}

func (e *Exec) storeCheckpoint(seqNum int, token string, state map[string]string, merkleRoot string) {
	stateCopy := common.CopyStringMap(state)
	if merkleRoot == "" {
		merkleRoot = NewMerkleTreeFromMap(stateCopy).Root()
	}
	e.checkpoints[seqNum] = rollbackCheckpoint{
		SeqNum:         seqNum,
		Token:          token,
		State:          stateCopy,
		MerkleRoot:     merkleRoot,
		ValidationHash: computeCheckpointValidationHash(seqNum, token, merkleRoot),
	}
	e.gcStableCheckpoints(seqNum)
}

func (e *Exec) gcStableCheckpoints(newStableSeqNum int) {
	highestStableSeqNum := newStableSeqNum
	if e.stableState.SeqNum > highestStableSeqNum {
		highestStableSeqNum = e.stableState.SeqNum
	}
	for seqNum := range e.checkpoints {
		if seqNum < highestStableSeqNum {
			delete(e.checkpoints, seqNum)
		}
	}
}
