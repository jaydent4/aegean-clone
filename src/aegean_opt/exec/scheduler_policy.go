package exec

func (s *execScheduler) nextRunnableRequest(batch *parallelBatchRuntime) *scheduledRequest {
	total := len(batch.requests)
	if total == 0 {
		return nil
	}
	for scanned := 0; scanned < total; scanned++ {
		req := batch.requests[batch.nextReq]
		batch.nextReq = (batch.nextReq + 1) % total
		switch req.state {
		case requestFinished, requestExecuting:
			continue
		case requestWaiting:
			// Wake a waiting request only if the number of buffered nested
			// responses is greater than the count it had already seen when it
			// last blocked.
			if s.nestedResponseCount(req.id) > req.nestedSeen {
				req.state = requestRunnable
				return req
			}
		case requestRunnable:
			return req
		}
	}
	return nil
}

func (s *execScheduler) batchBySeq(batches []*parallelBatchRuntime, seq int) *parallelBatchRuntime {
	if seq < 0 || seq >= len(batches) {
		return nil
	}
	return batches[seq]
}

func (s *execScheduler) nextRunnableBatchSeq(batches []*parallelBatchRuntime, stableBatchSeq int, currentBatchSeq int) (int, bool) {
	if len(batches) == 0 {
		return 0, false
	}
	windowK := s.parallelWindowK
	if windowK <= 0 {
		windowK = 1
	}
	windowStart := stableBatchSeq + 1
	if windowStart >= len(batches) {
		return 0, false
	}
	windowSize := windowK
	remaining := len(batches) - windowStart
	if windowSize > remaining {
		windowSize = remaining
	}
	if windowSize <= 0 {
		return 0, false
	}

	startSeq := currentBatchSeq + 1
	if startSeq < windowStart || startSeq >= windowStart+windowSize {
		startSeq = windowStart
	}
	// Deterministic round-robin within the active window
	for scanned := 0; scanned < windowSize; scanned++ {
		seq := startSeq
		startSeq++
		// Deterministic window wrap: v+k+1 -> v+1
		if startSeq >= windowStart+windowSize {
			startSeq = windowStart
		}
		batch := batches[seq]
		if batch.done() {
			continue
		}
		if s.batchHasRunnableRequest(batch) {
			return seq, true
		}
	}
	return 0, false
}

func (s *execScheduler) batchHasRunnableRequest(batch *parallelBatchRuntime) bool {
	for _, req := range batch.requests {
		switch req.state {
		case requestRunnable:
			return true
		case requestWaiting:
			if s.nestedResponseCount(req.id) > req.nestedSeen {
				return true
			}
		}
	}
	return false
}

func (s *execScheduler) advanceStableBatchSeq(batches []*parallelBatchRuntime, stableBatchSeq int) int {
	// Advance v only over a contiguous finished prefix
	next := stableBatchSeq + 1
	for next < len(batches) && batches[next].done() {
		stableBatchSeq = next
		next++
	}
	return stableBatchSeq
}
