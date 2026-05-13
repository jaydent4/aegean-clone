package exec

import (
	"crypto/sha256"
	"encoding/hex"
)

func computeCheckpointValidationHash(seqNum int, token string, stateRoot string) string {
	payload := map[string]any{
		"seq_num":    seqNum,
		"token":      token,
		"state_root": stateRoot,
	}
	sum := sha256.Sum256(marshalSorted(payload))
	return hex.EncodeToString(sum[:])
}

func (e *Exec) validateRollbackCheckpoint(cp rollbackCheckpoint, expectedToken string) bool {
	if cp.Token != expectedToken {
		return false
	}
	recomputed := computeCheckpointValidationHash(cp.SeqNum, cp.Token, cp.MerkleRoot)
	return recomputed == cp.ValidationHash
}
