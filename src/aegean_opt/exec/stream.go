package exec

import (
	"sort"

	"aegean/aegean_opt/protocol"
	"aegean/common"
)

type streamWindowRuntime struct {
	seqNum            int
	requests          map[string]*streamScheduledRequest
	workerQueues      [][]*streamScheduledRequest
	closed            bool
	expectedCount     int
	completed         int
	started           bool
	workerBusy        []bool
	missingDependents map[string][]*streamScheduledRequest
}

type streamScheduledRequest struct {
	id          string
	seqNum      int
	index       int
	requestID   string
	payload     map[string]any
	worker      int
	dependsOn   []string
	dependents  []*streamScheduledRequest
	pendingDeps int
	state       requestState
	output      map[string]any
	nestedSeen  int
	ndSeed      int64
	ndTimestamp float64
	readKeys    []string
	writeKeys   []string
}

type streamWorkerTask struct {
	req *streamScheduledRequest
}

type streamWorkerResult struct {
	seqNum int
	nodeID string
	worker int
	output map[string]any
}

func newStreamWindow(seqNum int, workerCount int) *streamWindowRuntime {
	if workerCount <= 0 {
		workerCount = 1
	}
	return &streamWindowRuntime{
		seqNum:            seqNum,
		requests:          make(map[string]*streamScheduledRequest),
		workerQueues:      make([][]*streamScheduledRequest, workerCount),
		workerBusy:        make([]bool, workerCount),
		missingDependents: make(map[string][]*streamScheduledRequest),
	}
}

func (e *Exec) startStreamWorkers() {
	for workerID := range e.streamWorkerChs {
		ch := make(chan streamWorkerTask, 1)
		e.streamWorkerChs[workerID] = ch
		go e.runStreamWorker(workerID, ch)
	}
}

func (e *Exec) runStreamWorker(workerID int, tasks <-chan streamWorkerTask) {
	for task := range tasks {
		req := task.req
		e.endRequestDispatchWait(req.payload)
		output := e.ExecuteRequest(e, req.payload, req.ndSeed, req.ndTimestamp)
		e.ingressCh <- ingressEvent{
			kind: ingressStreamExecutionCompleteEvent,
			streamResult: &streamWorkerResult{
				seqNum: req.seqNum,
				nodeID: req.id,
				worker: workerID,
				output: output,
			},
		}
	}
}

func (e *Exec) applyScheduledRequestEvent(payload map[string]any) {
	seqNum := common.GetInt(payload, protocol.FieldSeqNum)
	if seqNum <= 0 {
		return
	}
	window := e.streamWindow(seqNum)
	req := scheduledRequestFromPayload(payload, e.workerCount)
	if req == nil {
		return
	}
	if _, exists := window.requests[req.id]; exists {
		return
	}
	_ = e.SetRequestContextValue(req.requestID, requestBatchSeqContextKey, req.seqNum)
	window.addRequest(req)
	e.startRequestSpan(
		req.payload,
		batchBufferWaitSpanContextKey,
		"exec.stream_buffer_wait",
	)
}

func (e *Exec) applyVerificationWindowClosedEvent(payload map[string]any) {
	seqNum := common.GetInt(payload, protocol.FieldSeqNum)
	if seqNum <= 0 {
		return
	}
	window := e.streamWindow(seqNum)
	window.closed = true
	window.expectedCount = common.GetInt(payload, protocol.FieldCount)
}

func (e *Exec) applyStreamExecutionResult(result *streamWorkerResult) {
	if result == nil {
		return
	}
	window := e.streamWindows[result.seqNum]
	if window == nil {
		return
	}
	req := window.requests[result.nodeID]
	if req == nil {
		return
	}
	if result.worker >= 0 && result.worker < len(window.workerBusy) {
		window.workerBusy[result.worker] = false
	}
	status := common.GetString(result.output, "status")
	if status == "blocked_for_nested_response" {
		req.nestedSeen = e.scheduler.nestedResponseCount(req.requestID)
		req.state = requestWaiting
		return
	}

	e.endRequestDispatchWait(req.payload)
	e.endRequestSpan(req.requestID, batchBufferWaitSpanContextKey)
	e.startRequestSpan(
		req.payload,
		requestVerifyGateWaitSpanContextKey,
		"exec.request_verify_gate_wait",
	)
	req.state = requestFinished
	req.output = result.output
	window.completed++
	for _, dependent := range req.dependents {
		if dependent.pendingDeps > 0 {
			dependent.pendingDeps--
		}
	}
}

func (e *Exec) streamWindow(seqNum int) *streamWindowRuntime {
	if window, ok := e.streamWindows[seqNum]; ok {
		return window
	}
	window := newStreamWindow(seqNum, e.workerCount)
	e.streamWindows[seqNum] = window
	return window
}

func (w *streamWindowRuntime) addRequest(req *streamScheduledRequest) {
	w.requests[req.id] = req
	if req.worker < 0 || req.worker >= len(w.workerQueues) {
		req.worker = 0
	}
	for _, depID := range req.dependsOn {
		dep := w.requests[depID]
		if dep == nil {
			req.pendingDeps++
			w.missingDependents[depID] = append(w.missingDependents[depID], req)
			continue
		}
		if dep.state == requestFinished {
			continue
		}
		req.pendingDeps++
		dep.dependents = append(dep.dependents, req)
	}
	for _, dependent := range w.missingDependents[req.id] {
		req.dependents = append(req.dependents, dependent)
	}
	delete(w.missingDependents, req.id)
	w.workerQueues[req.worker] = append(w.workerQueues[req.worker], req)
}

func (e *Exec) flushStreamWindows() bool {
	progressed := false
	if e.startNextStreamWindow() {
		progressed = true
	}
	if e.dispatchStreamWork() {
		progressed = true
	}
	if e.finishActiveStreamWindow() {
		progressed = true
	}
	return progressed
}

func (e *Exec) startNextStreamWindow() bool {
	if e.streamActiveSeq != 0 {
		return false
	}
	e.mu.Lock()
	seq := e.nextBatchSeq
	if e.batchExecuting {
		e.mu.Unlock()
		return false
	}
	e.mu.Unlock()

	window := e.streamWindows[seq]
	if window == nil || len(window.requests) == 0 {
		return false
	}
	e.beginBatchMerkleContext()
	window.started = true
	e.streamActiveSeq = seq
	e.mu.Lock()
	e.batchExecuting = true
	e.mu.Unlock()
	return true
}

func (e *Exec) dispatchStreamWork() bool {
	if e.streamActiveSeq == 0 {
		return false
	}
	window := e.streamWindows[e.streamActiveSeq]
	if window == nil {
		return false
	}

	e.mu.Lock()
	forceSequential := e.forceSequential
	e.mu.Unlock()

	progressed := false
	if forceSequential {
		if len(window.workerBusy) > 0 && !window.workerBusy[0] {
			if req := e.nextSequentialStreamRequest(window); req != nil {
				window.workerBusy[0] = true
				req.state = requestExecuting
				e.streamWorkerChs[0] <- streamWorkerTask{req: req}
				progressed = true
			}
		}
		return progressed
	}

	for worker := range window.workerQueues {
		if window.workerBusy[worker] {
			continue
		}
		req := e.nextRunnableStreamRequest(window, worker)
		if req == nil {
			continue
		}
		window.workerBusy[worker] = true
		req.state = requestExecuting
		e.streamWorkerChs[worker] <- streamWorkerTask{req: req}
		progressed = true
	}
	return progressed
}

func (e *Exec) nextSequentialStreamRequest(window *streamWindowRuntime) *streamScheduledRequest {
	for _, req := range window.requestsInOrder() {
		switch req.state {
		case requestFinished, requestExecuting:
			continue
		case requestWaiting:
			if e.scheduler.nestedResponseCount(req.requestID) > req.nestedSeen {
				req.state = requestRunnable
				return req
			}
			return nil
		case requestRunnable:
			return req
		}
	}
	return nil
}

func (e *Exec) nextRunnableStreamRequest(window *streamWindowRuntime, worker int) *streamScheduledRequest {
	scanLimit := e.scheduler.parallelWindowK
	if scanLimit <= 0 {
		scanLimit = 1
	}
	scanned := 0
	for _, req := range window.workerQueues[worker] {
		switch req.state {
		case requestFinished, requestExecuting:
			continue
		}
		scanned++
		if scanned > scanLimit {
			return nil
		}
		if req.pendingDeps > 0 {
			continue
		}
		switch req.state {
		case requestWaiting:
			if e.scheduler.nestedResponseCount(req.requestID) > req.nestedSeen {
				req.state = requestRunnable
				return req
			}
		case requestRunnable:
			return req
		}
	}
	return nil
}

func (e *Exec) finishActiveStreamWindow() bool {
	if e.streamActiveSeq == 0 {
		return false
	}
	window := e.streamWindows[e.streamActiveSeq]
	if window == nil || !window.closed {
		return false
	}
	if window.expectedCount > 0 && window.completed < window.expectedCount {
		return false
	}
	if window.expectedCount == 0 && window.completed < len(window.requests) {
		return false
	}

	e.finalizeBatchMerkleContext()
	e.stateMu.Lock()
	e.workingState.EnsureMerkle()
	stateSnapshot := common.CopyStringMap(e.workingState.KVStore)
	stateRoot := e.workingState.MerkleRoot
	e.stateMu.Unlock()

	result := &batchExecutionResult{
		seqNum:     window.seqNum,
		payload:    window.replayPayload(),
		outputs:    window.outputsInOrder(),
		state:      stateSnapshot,
		merkleRoot: stateRoot,
	}
	e.applyBatchExecutionResult(result)
	e.mu.Lock()
	if e.nextBatchSeq == window.seqNum {
		e.nextBatchSeq++
	}
	e.mu.Unlock()
	delete(e.streamWindows, window.seqNum)
	e.streamActiveSeq = 0
	return true
}

func scheduledRequestFromPayload(payload map[string]any, workerCount int) *streamScheduledRequest {
	nodeID, _ := payload[protocol.FieldNodeID].(string)
	if nodeID == "" {
		nodeID = protocol.NodeID(common.GetInt(payload, protocol.FieldSeqNum), common.GetInt(payload, protocol.FieldRequestIdx))
	}
	request, ok := payload[protocol.FieldRequest].(map[string]any)
	if !ok {
		return nil
	}
	worker := common.GetInt(payload, protocol.FieldWorker)
	if worker < 0 || worker >= workerCount {
		worker = 0
	}
	requestID := common.GetString(payload, protocol.FieldRequestID)
	if requestID == "" {
		requestID = common.GetString(request, protocol.FieldRequestID)
	}
	return &streamScheduledRequest{
		id:          nodeID,
		seqNum:      common.GetInt(payload, protocol.FieldSeqNum),
		index:       common.GetInt(payload, protocol.FieldRequestIdx),
		requestID:   requestID,
		payload:     request,
		worker:      worker,
		dependsOn:   normalizeStringSlice(payload[protocol.FieldDependsOn]),
		state:       requestRunnable,
		ndSeed:      common.GetInt64(payload, protocol.FieldNDSeed),
		ndTimestamp: common.GetFloat(payload, protocol.FieldNDTimestamp),
		readKeys:    normalizeStringSlice(payload[protocol.FieldReadKeys]),
		writeKeys:   normalizeStringSlice(payload[protocol.FieldWriteKeys]),
	}
}

func (w *streamWindowRuntime) requestsInOrder() []*streamScheduledRequest {
	requests := make([]*streamScheduledRequest, 0, len(w.requests))
	for _, req := range w.requests {
		requests = append(requests, req)
	}
	sort.Slice(requests, func(i, j int) bool {
		if requests[i].index == requests[j].index {
			return requests[i].id < requests[j].id
		}
		return requests[i].index < requests[j].index
	})
	return requests
}

func (w *streamWindowRuntime) outputsInOrder() []map[string]any {
	requests := w.requestsInOrder()
	outputs := make([]map[string]any, 0, len(requests))
	for _, req := range requests {
		if req.output != nil {
			outputs = append(outputs, req.output)
		}
	}
	return outputs
}

func (w *streamWindowRuntime) replayPayload() map[string]any {
	requests := w.requestsInOrder()
	scheduled := make([]map[string]any, 0, len(requests))
	rawRequests := make([]map[string]any, 0, len(requests))
	for _, req := range requests {
		scheduled = append(scheduled, req.replayPayload(w.seqNum))
		rawRequests = append(rawRequests, req.payload)
	}
	return map[string]any{
		protocol.FieldType:     protocol.MessageTypeVerificationWindowReplay,
		protocol.FieldSeqNum:   w.seqNum,
		protocol.FieldCount:    w.expectedCount,
		protocol.FieldSchedule: scheduled,
		protocol.FieldRequests: rawRequests,
	}
}

func (r *streamScheduledRequest) replayPayload(seqNum int) map[string]any {
	return map[string]any{
		protocol.FieldType:        protocol.MessageTypeScheduledRequest,
		protocol.FieldSeqNum:      seqNum,
		protocol.FieldRequestIdx:  r.index,
		protocol.FieldRequestID:   r.requestID,
		protocol.FieldRequest:     r.payload,
		protocol.FieldNodeID:      r.id,
		protocol.FieldWorker:      r.worker,
		protocol.FieldDependsOn:   append([]string{}, r.dependsOn...),
		protocol.FieldNDSeed:      r.ndSeed,
		protocol.FieldNDTimestamp: r.ndTimestamp,
		protocol.FieldReadKeys:    append([]string{}, r.readKeys...),
		protocol.FieldWriteKeys:   append([]string{}, r.writeKeys...),
	}
}

func normalizeStringSlice(raw any) []string {
	switch typed := raw.(type) {
	case []string:
		return append([]string{}, typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if value, ok := item.(string); ok {
				out = append(out, value)
			}
		}
		return out
	default:
		return nil
	}
}

func (e *Exec) enqueueReplayInput(payload map[string]any) {
	if payload == nil {
		return
	}
	if common.GetString(payload, protocol.FieldType) != protocol.MessageTypeVerificationWindowReplay {
		e.ingressCh <- ingressEvent{kind: ingressBatchEvent, payload: payload}
		return
	}
	for _, scheduled := range normalizeMapSlice(payload[protocol.FieldSchedule]) {
		e.ingressCh <- ingressEvent{kind: ingressScheduledRequestEvent, payload: scheduled}
	}
	e.ingressCh <- ingressEvent{
		kind: ingressVerificationWindowClosedEvent,
		payload: map[string]any{
			protocol.FieldType:   protocol.MessageTypeVerificationWindowClosed,
			protocol.FieldSeqNum: common.GetInt(payload, protocol.FieldSeqNum),
			protocol.FieldCount:  common.GetInt(payload, protocol.FieldCount),
		},
	}
}

func normalizeMapSlice(raw any) []map[string]any {
	switch typed := raw.(type) {
	case []map[string]any:
		return append([]map[string]any{}, typed...)
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			if value, ok := item.(map[string]any); ok {
				out = append(out, value)
			}
		}
		return out
	default:
		return nil
	}
}
