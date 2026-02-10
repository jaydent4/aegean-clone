package exec

import "aegean/common"

type rollbackCheckpoint struct {
	SeqNum          int
	Token           string
	State           map[string]string
	ValidationHash  string
}

func (e *Exec) storeCheckpoint(seqNum int, token string, state map[string]string) {
	stateCopy := common.CopyStringMap(state)
	e.checkpoints[seqNum] = rollbackCheckpoint{
		SeqNum:         seqNum,
		Token:          token,
		State:          stateCopy,
		ValidationHash: computeCheckpointValidationHash(seqNum, token, stateCopy),
	}
}
