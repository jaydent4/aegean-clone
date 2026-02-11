package exec

import (
	"fmt"
	"log"

	"aegean/common"
)

func (e *Exec) requestStateTransfer() bool {
	// TODO: should state transfer be async? Meaning that should state transfer request
	// spin and wait for a response before processing other requests
	// TODO: after state transfer, do we send back client the response?
	// Request state transfer from a replica that has the correct state
	for _, sourceExec := range e.Peers {
		log.Printf("%s: Requesting state transfer from %s", e.Name, sourceExec)
		e.mu.Lock()
		e.stableState.EnsureMerkle()
		knownSeq := e.stableState.SeqNum
		knownPrevHash := e.stableState.PrevHash
		knownRoot := e.stableState.MerkleRoot
		knownLeafHashes := e.stableState.Merkle.LeafHashes()
		e.mu.Unlock()

		requestMsg := map[string]any{
			"type":              "state_transfer_request",
			"requesting_exec":   e.Name,
			"known_seq_num":     knownSeq,
			"known_prev_hash":   knownPrevHash,
			"known_state_root":  knownRoot,
			"known_leaf_hashes": knownLeafHashes,
		}

		response, err := common.SendMessage(sourceExec, 8000, requestMsg)
		if err != nil {
			log.Printf("%s: Error requesting state transfer from %s: %v", e.Name, sourceExec, err)
			continue
		}

		if response == nil || response["status"] != "ok" {
			log.Printf("%s: State transfer from %s failed: %v", e.Name, sourceExec, response)
			continue
		}

		transferredStableSeqNum := common.GetInt(response, "stable_seq_num")
		transferredPrevHash, _ := response["prev_hash"].(string)
		transferredStateRoot, _ := response["state_root"].(string)
		mode, _ := response["mode"].(string)

		// Only apply if the provided stable seq num is higher than ours
		e.mu.Lock()
		currentStableSeq := e.stableState.SeqNum
		e.mu.Unlock()
		if transferredStableSeqNum <= currentStableSeq {
			log.Printf("%s: Received stable_seq_num %d from %s is not higher than ours (%d)", e.Name, transferredStableSeqNum, sourceExec, currentStableSeq)
			continue
		}

		e.mu.Lock()
		e.stableState.EnsureMerkle()
		merged := e.stableState.Merkle.SnapshotMap()
		switch mode {
		case "delta":
			updatesAny, _ := response["updates"].(map[string]any)
			for key, value := range updatesAny {
				merged[key] = fmt.Sprintf("%v", value)
			}
			if deletesAny, ok := response["deletes"].([]any); ok {
				for _, raw := range deletesAny {
					if key, ok := raw.(string); ok {
						delete(merged, key)
					}
				}
			}
		case "full":
			fullState, ok := response["state"].(map[string]any)
			if !ok {
				e.mu.Unlock()
				log.Printf("%s: Invalid full state transfer from %s", e.Name, sourceExec)
				continue
			}
			merged = make(map[string]string, len(fullState))
			for key, value := range fullState {
				merged[key] = fmt.Sprintf("%v", value)
			}
		default:
			e.mu.Unlock()
			log.Printf("%s: Invalid state transfer mode from %s: %q", e.Name, sourceExec, mode)
			continue
		}
		mergedMerkle := NewMerkleTreeFromMap(merged)
		if mergedMerkle.Root() != transferredStateRoot {
			e.mu.Unlock()
			log.Printf("%s: Reconstructed root mismatch from %s (local=%s remote=%s)", e.Name, sourceExec, common.TruncateToken(mergedMerkle.Root()), common.TruncateToken(transferredStateRoot))
			continue
		}
		e.mu.Unlock()

		e.stateMu.Lock()
		e.workingState.KVStore = common.CopyStringMap(merged)
		e.workingState.Merkle = mergedMerkle.Clone()
		e.workingState.MerkleRoot = mergedMerkle.Root()
		e.stateMu.Unlock()
		e.mu.Lock()
		e.stableState = State{
			KVStore:    common.CopyStringMap(merged),
			Merkle:     mergedMerkle.Clone(),
			MerkleRoot: mergedMerkle.Root(),
			SeqNum:     transferredStableSeqNum,
			PrevHash:   transferredPrevHash,
			Verified:   true,
		}
		e.storeCheckpoint(e.stableState.SeqNum, e.stableState.PrevHash, mergedMerkle, mergedMerkle.Root())
		e.forceSequential = false
		for seq := range e.pendingResponses {
			if seq <= e.stableState.SeqNum {
				delete(e.pendingResponses, seq)
			}
		}
		e.batchBuffer.Drop(e.stableState.SeqNum)
		e.verifyBuffer.Drop(e.stableState.SeqNum)
		if e.nextBatchSeq < e.stableState.SeqNum+1 {
			e.nextBatchSeq = e.stableState.SeqNum + 1
		}
		if e.nextVerifySeq < e.stableState.SeqNum+1 {
			e.nextVerifySeq = e.stableState.SeqNum + 1
		}
		appliedSeq := e.stableState.SeqNum
		e.mu.Unlock()

		log.Printf("%s: Successfully applied state transfer from %s, now at stable_seq_num %d", e.Name, sourceExec, appliedSeq)
		return true
	}
	return false
}

func (e *Exec) handleStateTransferRequest(payload map[string]any) map[string]any {
	requestingExec, _ := payload["requesting_exec"].(string)
	knownRoot, _ := payload["known_state_root"].(string)
	knownLeafHashes := map[string]string{}
	if leafAny, ok := payload["known_leaf_hashes"].(map[string]any); ok {
		for key, value := range leafAny {
			if hash, ok := value.(string); ok {
				knownLeafHashes[key] = hash
			}
		}
	}
	e.mu.Lock()
	e.stableState.EnsureMerkle()
	stableMerkle := e.stableState.Merkle.Clone()
	stableSeq := e.stableState.SeqNum
	stablePrevHash := e.stableState.PrevHash
	stableRoot := e.stableState.MerkleRoot
	e.mu.Unlock()
	log.Printf("%s: Received state transfer request from %s, providing stable state at seq_num %d", e.Name, requestingExec, stableSeq)

	if knownRoot == stableRoot {
		return map[string]any{
			"status":         "ok",
			"mode":           "delta",
			"updates":        map[string]string{},
			"deletes":        []string{},
			"stable_seq_num": stableSeq,
			"prev_hash":      stablePrevHash,
			"state_root":     stableRoot,
		}
	}
	updates, deletes := stableMerkle.DiffFromLeafHashes(knownLeafHashes)

	return map[string]any{
		"status":         "ok",
		"mode":           "delta",
		"updates":        updates,
		"deletes":        deletes,
		"stable_seq_num": stableSeq,
		"prev_hash":      stablePrevHash,
		"state_root":     stableRoot,
	}
}
