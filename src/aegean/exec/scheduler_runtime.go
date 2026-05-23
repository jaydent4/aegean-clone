package exec

import (
	"time"

	"aegean/common"
)

type parallelBatchRuntime struct {
	requests  []*scheduledRequest
	finished  int
	nextReq   int
	activated bool
}

type parallelWorkerTask struct {
	batch *parallelBatchRuntime
	req   *scheduledRequest
}

type parallelWorkerResult struct {
	batch    *parallelBatchRuntime
	req      *scheduledRequest
	output   map[string]any
	duration time.Duration
}

type executionSchedulerStats struct {
	workerCalls      int
	blockedResponses int
	nestedWaits      int
	nestedWait       time.Duration
	workerExec       time.Duration
	maxWorkerExec    time.Duration
	workflowStats    map[string]int64
}

const workflowStatsOutputKey = "_workflow_stats"

func (b *parallelBatchRuntime) done() bool {
	return b.finished >= len(b.requests)
}

func (s *execScheduler) executeParallelBatches(e *Exec, parallelBatches [][]map[string]any, ndSeed int64, ndTimestamp float64) ([]map[string]any, executionSchedulerStats) {
	var stats executionSchedulerStats
	batches, allScheduled := s.initParallelBatchRuntimes(parallelBatches)
	if len(allScheduled) == 0 {
		return nil, stats
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
			s.activateBatch(e, batch, workerCount)
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
			stats.workerCalls++
			stats.workerExec += res.duration
			if res.duration > stats.maxWorkerExec {
				stats.maxWorkerExec = res.duration
			}
			stats.addWorkflowStats(res.output)
			activeWorkers--
			status := common.GetString(res.output, "status")
			if status == "blocked_for_nested_response" {
				stats.blockedResponses++
				if s.promoteOneNestedResponse(res.req.id) {
					res.req.state = requestRunnable
				} else {
					res.req.state = requestWaiting
				}
			} else {
				e.endRequestDispatchWait(res.req.payload)
				e.startRequestSpan(
					res.req.payload,
					requestVerifyGateWaitSpanContextKey,
					"exec.request_verify_gate_wait",
				)
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
			waitStart := time.Now()
			<-s.nestedReadyCh
			stats.nestedWaits++
			stats.nestedWait += time.Since(waitStart)
		}
	}

	close(taskCh)
	return s.collectParallelOutputs(batches, totalRequests), stats
}

func (s *execScheduler) executeSequentialBatches(e *Exec, parallelBatches [][]map[string]any, ndSeed int64, ndTimestamp float64) ([]map[string]any, executionSchedulerStats) {
	var stats executionSchedulerStats
	batches, allScheduled := s.initParallelBatchRuntimes(parallelBatches)
	if len(allScheduled) == 0 {
		return nil, stats
	}

	s.registerScheduledRequests(allScheduled)
	defer s.unregisterScheduledRequests(e, allScheduled)

	outputs := make([]map[string]any, 0, len(allScheduled))
	for _, batch := range batches {
		s.activateBatch(e, batch, 1)
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
				callStart := time.Now()
				output := e.ExecuteRequest(e, req.payload, ndSeed, ndTimestamp)
				callDuration := time.Since(callStart)
				stats.workerCalls++
				stats.workerExec += callDuration
				if callDuration > stats.maxWorkerExec {
					stats.maxWorkerExec = callDuration
				}
				stats.addWorkflowStats(output)
				if common.GetString(output, "status") != "blocked_for_nested_response" {
					e.endRequestDispatchWait(req.payload)
					e.startRequestSpan(
						req.payload,
						requestVerifyGateWaitSpanContextKey,
						"exec.request_verify_gate_wait",
					)
					req.state = requestFinished
					req.output = output
					outputs = append(outputs, output)
					break
				}
				stats.blockedResponses++
				for !s.promoteOneNestedResponse(req.id) {
					waitStart := time.Now()
					<-s.nestedReadyCh
					stats.nestedWaits++
					stats.nestedWait += time.Since(waitStart)
				}
			}
		}
	}
	return outputs, stats
}

func (s *execScheduler) initParallelBatchRuntimes(parallelBatches [][]map[string]any) ([]*parallelBatchRuntime, []*scheduledRequest) {
	if len(parallelBatches) == 0 {
		return nil, nil
	}
	batches := make([]*parallelBatchRuntime, 0, len(parallelBatches))
	allScheduled := make([]*scheduledRequest, 0)
	for batchSeq, batchRequests := range parallelBatches {
		scheduled := make([]*scheduledRequest, 0, len(batchRequests))
		for reqIdx, req := range batchRequests {
			scheduledReq := &scheduledRequest{
				index:    reqIdx,
				batchSeq: batchSeq,
				id:       requestIDForSchedule(req, reqIdx),
				payload:  req,
				state:    requestRunnable,
			}
			scheduled = append(scheduled, scheduledReq)
			allScheduled = append(allScheduled, scheduledReq)
		}
		batches = append(batches, &parallelBatchRuntime{
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
				e.endRequestDispatchWait(task.req.payload)
				start := time.Now()
				output := e.ExecuteRequest(e, task.req.payload, ndSeed, ndTimestamp)
				duration := time.Since(start)
				resultCh <- parallelWorkerResult{batch: task.batch, req: task.req, output: output, duration: duration}
			}
		}()
	}
	return workerCount
}

func (s *executionSchedulerStats) addWorkflowStats(output map[string]any) {
	if output == nil {
		return
	}
	raw, ok := output[workflowStatsOutputKey]
	if !ok {
		return
	}
	delete(output, workflowStatsOutputKey)

	switch typed := raw.(type) {
	case map[string]int64:
		s.addWorkflowStatsInt64(typed)
	case map[string]any:
		s.addWorkflowStatsAny(typed)
	}
}

func (s *executionSchedulerStats) addWorkflowStatsInt64(fields map[string]int64) {
	if len(fields) == 0 {
		return
	}
	if s.workflowStats == nil {
		s.workflowStats = make(map[string]int64, len(fields))
	}
	for key, value := range fields {
		if key == "" {
			continue
		}
		s.workflowStats[key] += value
	}
}

func (s *executionSchedulerStats) addWorkflowStatsAny(fields map[string]any) {
	if len(fields) == 0 {
		return
	}
	if s.workflowStats == nil {
		s.workflowStats = make(map[string]int64, len(fields))
	}
	for key, raw := range fields {
		if key == "" {
			continue
		}
		switch value := raw.(type) {
		case int:
			s.workflowStats[key] += int64(value)
		case int64:
			s.workflowStats[key] += value
		case int32:
			s.workflowStats[key] += int64(value)
		case uint:
			s.workflowStats[key] += int64(value)
		case uint64:
			s.workflowStats[key] += int64(value)
		case uint32:
			s.workflowStats[key] += int64(value)
		case float64:
			s.workflowStats[key] += int64(value)
		case time.Duration:
			s.workflowStats[key] += value.Microseconds()
		}
	}
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

func (s *execScheduler) activateBatch(e *Exec, batch *parallelBatchRuntime, workerCount int) {
	if batch == nil || batch.activated {
		return
	}
	batch.activated = true
	_, _ = e, workerCount
}
