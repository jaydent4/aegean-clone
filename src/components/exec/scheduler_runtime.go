package exec

import (
	"aegean/common"
)

type parallelBatchRuntime struct {
	batchSeq int
	requests []*scheduledRequest
	finished int
	nextReq  int
}

type parallelWorkerTask struct {
	batch *parallelBatchRuntime
	req   *scheduledRequest
}

type parallelWorkerResult struct {
	batch  *parallelBatchRuntime
	req    *scheduledRequest
	output map[string]any
}

func (b *parallelBatchRuntime) done() bool {
	return b.finished >= len(b.requests)
}

func (s *execScheduler) executeParallelBatches(e *Exec, parallelBatches [][]map[string]any, ndSeed int64, ndTimestamp float64) []map[string]any {
	batches, allScheduled := s.initParallelBatchRuntimes(parallelBatches)
	if len(allScheduled) == 0 {
		return nil
	}

	s.registerScheduledRequests(allScheduled)
	defer s.unregisterScheduledRequests(e, allScheduled)

	totalRequests := len(allScheduled)
	taskCh := make(chan parallelWorkerTask, totalRequests)
	resultCh := make(chan parallelWorkerResult, totalRequests)
	workerCount := s.startParallelWorkers(e, taskCh, resultCh, totalRequests, ndSeed, ndTimestamp)

	totalFinished := 0
	activeWorkers := 0
	// stableBatchSeq tracks v: highest contiguous finished parallel batch
	stableBatchSeq := -1
	currentBatchSeq := stableBatchSeq + 1

	for totalFinished < totalRequests {
		dispatched := false
		if currentBatchSeq <= stableBatchSeq || currentBatchSeq >= len(batches) {
			currentBatchSeq = stableBatchSeq + 1
		}

		for activeWorkers < workerCount {
			batch := s.batchBySeq(batches, currentBatchSeq)
			if batch == nil {
				break
			}
			req := s.nextRunnableRequest(batch)
			if req == nil {
				break
			}
			taskCh <- parallelWorkerTask{batch: batch, req: req}
			req.state = requestExecuting
			activeWorkers++
			dispatched = true
		}

		if activeWorkers > 0 {
			res := <-resultCh
			activeWorkers--
			status := common.GetString(res.output, "status")
			if status == "blocked_for_nested_response" {
				res.req.state = requestWaiting
			} else {
				res.req.state = requestFinished
				res.req.output = res.output
				res.batch.finished++
				totalFinished++
				stableBatchSeq = s.advanceStableBatchSeq(batches, stableBatchSeq)
				if currentBatchSeq <= stableBatchSeq {
					currentBatchSeq = stableBatchSeq + 1
				}
			}
			continue
		}

		if !dispatched {
			// Current batch is blocked; deterministically probe the next batch in [v+1, v+k]
			if nextBatchSeq, ok := s.nextRunnableBatchSeq(batches, stableBatchSeq, currentBatchSeq); ok {
				currentBatchSeq = nextBatchSeq
				continue
			}
			// No batch in window can make progress; wait for nested response arrival
			<-s.nestedReadyCh
		}
	}

	close(taskCh)
	return s.collectParallelOutputs(batches, totalRequests)
}

func (s *execScheduler) executeSequentialBatches(e *Exec, parallelBatches [][]map[string]any, ndSeed int64, ndTimestamp float64) []map[string]any {
	batches, allScheduled := s.initParallelBatchRuntimes(parallelBatches)
	if len(allScheduled) == 0 {
		return nil
	}

	s.registerScheduledRequests(allScheduled)
	defer s.unregisterScheduledRequests(e, allScheduled)

	outputs := make([]map[string]any, 0, len(allScheduled))
	for _, batch := range batches {
		for _, req := range batch.requests {
			/*
			  This is based on the assumption that sequential execution is only triggered when
			  a previous parallel execution was rolled back. Because the parallel execution finished,
			  all the nested responses should be cached and reused during the sequential execution.
			  Therefore, we simply repeatedly call ExecuteRequest to reach the final state.
			  TODO: However, nested requests will still be emitted with the current implementation, which is
			  mostly fine because the shim will dedup it.
			  If some nested response is lost or another factor should prevent repeated calls to ExecuteRequest
			  from having the workflow reach the final state, this may cause unliveness. However, this usually
			  does not happen unless the workflow developer maliciously triggers this
			*/
			for {
				output := e.ExecuteRequest(e, req.payload, ndSeed, ndTimestamp)
				if common.GetString(output, "status") != "blocked_for_nested_response" {
					req.state = requestFinished
					req.output = output
					outputs = append(outputs, output)
					break
				}
			}
		}
	}
	return outputs
}

func (s *execScheduler) initParallelBatchRuntimes(parallelBatches [][]map[string]any) ([]*parallelBatchRuntime, []*scheduledRequest) {
	if len(parallelBatches) == 0 {
		return nil, nil
	}
	batches := make([]*parallelBatchRuntime, 0, len(parallelBatches))
	allScheduled := make([]*scheduledRequest, 0)
	for batchIdx, batchRequests := range parallelBatches {
		scheduled := make([]*scheduledRequest, 0, len(batchRequests))
		for reqIdx, req := range batchRequests {
			scheduledReq := &scheduledRequest{
				index:   reqIdx,
				id:      requestIDForSchedule(req, reqIdx),
				payload: req,
				state:   requestRunnable,
			}
			scheduled = append(scheduled, scheduledReq)
			allScheduled = append(allScheduled, scheduledReq)
		}
		batches = append(batches, &parallelBatchRuntime{
			batchSeq: batchIdx,
			requests: scheduled,
		})
	}
	return batches, allScheduled
}

func (s *execScheduler) startParallelWorkers(
	e *Exec,
	taskCh <-chan parallelWorkerTask,
	resultCh chan<- parallelWorkerResult,
	totalRequests int,
	ndSeed int64,
	ndTimestamp float64,
) int {
	workerCount := e.workerCount
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > totalRequests {
		workerCount = totalRequests
	}
	for i := 0; i < workerCount; i++ {
		go func() {
			for task := range taskCh {
				output := e.ExecuteRequest(e, task.req.payload, ndSeed, ndTimestamp)
				resultCh <- parallelWorkerResult{batch: task.batch, req: task.req, output: output}
			}
		}()
	}
	return workerCount
}

func (s *execScheduler) collectParallelOutputs(batches []*parallelBatchRuntime, totalRequests int) []map[string]any {
	outputs := make([]map[string]any, 0, totalRequests)
	for _, batch := range batches {
		for _, req := range batch.requests {
			if req.output != nil {
				outputs = append(outputs, req.output)
			}
		}
	}
	return outputs
}
