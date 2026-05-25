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

	replayPayloads := make([]map[string]any, 0, len(replaySeqs))
	replayRequestIDs := make([]string, 0)
	seenReplayRequestIDs := make(map[string]struct{})
	for _, replaySeq := range replaySeqs {
		payload := e.replayableBatchInputs[replaySeq]
		replayPayloads = append(replayPayloads, payload)
		for _, requestID := range requestIDsForBatchPayload(payload) {
			if _, seen := seenReplayRequestIDs[requestID]; seen {
				continue
			}
			seenReplayRequestIDs[requestID] = struct{}{}
			replayRequestIDs = append(replayRequestIDs, requestID)
		}
	}

	// Discard pending work above rollback point
	for pendingSeq := range e.pendingExecResults {
		if pendingSeq > seqNum {
			delete(e.pendingExecResults, pendingSeq)
		}
	}
	e.nextBatchSeq = seqNum + 1
	e.nextVerifySeq = seqNum + 1
	e.executionEpoch++
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
	e.batchCtx = nil
	e.stateMu.Unlock()

	e.scheduler.prepareRequestsForReplay(replayRequestIDs)
	e.nestedDispatchTracker.prepareParentsForReplay(replayRequestIDs)
	for _, payload := range replayPayloads {
		e.ingressCh <- ingressEvent{kind: ingressBatchEvent, payload: payload}
	}
	return true
}

func requestIDsForBatchPayload(payload map[string]any) []string {
	if payload == nil {
		return nil
	}
	parallelBatches, ok := payload["parallel_batches"].([][]map[string]any)
	if !ok {
		return nil
	}
	requestIDs := make([]string, 0)
	for _, batch := range parallelBatches {
		for _, request := range batch {
			requestID, ok := canonicalRequestID(request["request_id"])
			if !ok {
				continue
			}
			requestIDs = append(requestIDs, requestID)
		}
	}
	return requestIDs
}
