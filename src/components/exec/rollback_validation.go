package exec

import (
	"crypto/sha256"
	"encoding/hex"
)

func computeCheckpointValidationHash(seqNum int, token string, state map[string]string) string {
	payload := map[string]any{
		"seq_num": seqNum,
		"token":   token,
		"state":   state,
	}
	sum := sha256.Sum256(marshalSorted(payload))
	return hex.EncodeToString(sum[:])
}

func (e *Exec) validateRollbackCheckpoint(cp rollbackCheckpoint, expectedToken string) bool {
	if cp.Token != expectedToken {
		return false
	}
	recomputed := computeCheckpointValidationHash(cp.SeqNum, cp.Token, cp.State)
	return recomputed == cp.ValidationHash
}
