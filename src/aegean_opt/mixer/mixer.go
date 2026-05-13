package mixer

import (
	mixerkeys "aegean/aegean_opt/mixer/keys"
	"aegean/aegean_opt/protocol"
	"aegean/common"
	"aegean/telemetry"
	"context"
	"fmt"
	"sort"
	"sync"
)

type Mixer struct {
	Name            string
	NextCh          chan<- map[string]any
	SocialMixerMode string
	WorkerCount     int
	mu              sync.Mutex
	windows         map[int]*scheduleWindow
	seenRequests    map[string]struct{}
}

type scheduleWindow struct {
	seqNum              int
	lastWriter          map[string]string
	activeReaders       map[string]map[string]struct{}
	workerByNode        map[string]int
	requestIndexByNode  map[string]int
	workerQueuedLengths []int
	closed              bool
	count               int
}

func NewMixer(name string, nextCh chan<- map[string]any, runConfig map[string]any) *Mixer {
	if nextCh == nil {
		panic("mixer component requires non-nil nextCh")
	}
	m := &Mixer{
		Name:            name,
		NextCh:          nextCh,
		SocialMixerMode: mixerkeys.ResolveSocialMixerMode(runConfig),
		WorkerCount:     common.IntOrDefault(runConfig, "worker_count", 1),
		windows:         make(map[int]*scheduleWindow),
		seenRequests:    make(map[string]struct{}),
	}
	if m.WorkerCount <= 0 {
		m.WorkerCount = 1
	}
	return m
}

func (m *Mixer) getKeys(request map[string]any) (map[string]struct{}, map[string]struct{}) {
	payload, _ := request["op_payload"].(map[string]any)

	readKeys := make(map[string]struct{})
	writeKeys := make(map[string]struct{})

	mixerkeys.AddGenericWorkflowKeys(request, payload, readKeys, writeKeys)
	mixerkeys.AddHotelWorkflowKeys(request, payload, readKeys, writeKeys)
	mixerkeys.AddSocialWorkflowKeys(request, payload, readKeys, writeKeys, string(m.SocialMixerMode))
	mixerkeys.AddMediaWorkflowKeys(request, payload, readKeys, writeKeys)

	return readKeys, writeKeys
}

func hasIntersection(a, b map[string]struct{}) bool {
	for key := range a {
		if _, ok := b[key]; ok {
			return true
		}
	}
	return false
}

func (m *Mixer) hasConflict(reqRead, reqWrite, batchReads, batchWrites map[string]struct{}) bool {
	if hasIntersection(reqWrite, batchWrites) {
		return true
	}
	if hasIntersection(reqWrite, batchReads) {
		return true
	}
	if hasIntersection(reqRead, batchWrites) {
		return true
	}
	return false
}

type parallelBatch struct {
	requests []map[string]any
	reads    map[string]struct{}
	writes   map[string]struct{}
}

func (m *Mixer) partitionIntoParallelBatches(batch []map[string]any) [][]map[string]any {
	parallelBatches := []parallelBatch{}

	for _, request := range batch {
		reqRead, reqWrite := m.getKeys(request)
		placed := false

		for i := range parallelBatches {
			pb := &parallelBatches[i]
			if !m.hasConflict(reqRead, reqWrite, pb.reads, pb.writes) {
				pb.requests = append(pb.requests, request)
				for key := range reqRead {
					pb.reads[key] = struct{}{}
				}
				for key := range reqWrite {
					pb.writes[key] = struct{}{}
				}
				placed = true
				break
			}
		}

		if !placed {
			pb := parallelBatch{
				requests: []map[string]any{request},
				reads:    reqRead,
				writes:   reqWrite,
			}
			parallelBatches = append(parallelBatches, pb)
		}
	}

	result := make([][]map[string]any, 0, len(parallelBatches))
	for _, pb := range parallelBatches {
		result = append(result, pb.requests)
	}
	return result
}

func (m *Mixer) HandleBatchMessage(payload map[string]any) map[string]any {
	ctx := telemetry.ExtractContext(context.Background(), payload)

	seqNum := common.GetInt(payload, "seq_num")
	requests, ok := normalizeRequestSlice(payload["requests"])
	if !ok {
		return map[string]any{"status": "error", "error": "invalid requests"}
	}

	parallelBatches := m.partitionIntoParallelBatches(requests)

	message := map[string]any{
		"type":             "batch",
		"seq_num":          seqNum,
		"parallel_batches": parallelBatches,
		"nd_seed":          payload["nd_seed"],
		"nd_timestamp":     payload["nd_timestamp"],
	}
	telemetry.InjectContext(ctx, message)

	if m.NextCh != nil {
		m.NextCh <- message
	} else {
	}

	return map[string]any{"status": "mixed", "seq_num": seqNum}
}

func (m *Mixer) HandleInLogRequestMessage(payload map[string]any) map[string]any {
	ctx := telemetry.ExtractContext(context.Background(), payload)
	seqNum := common.GetInt(payload, protocol.FieldSeqNum)
	requestIndex := common.GetInt(payload, protocol.FieldRequestIdx)
	request, ok := normalizeInLogRequest(payload[protocol.FieldRequest])
	if !ok {
		return map[string]any{"status": "error", "error": "invalid request"}
	}

	requestID, _ := canonicalRequestID(payload[protocol.FieldRequestID])
	if requestID == "" {
		requestID, _ = canonicalRequestID(request[protocol.FieldRequestID])
	}

	m.mu.Lock()
	if requestID != "" {
		if _, duplicate := m.seenRequests[requestID]; duplicate {
			m.mu.Unlock()
			return map[string]any{"status": "duplicate_skipped", "seq_num": seqNum, "request_id": requestID}
		}
		m.seenRequests[requestID] = struct{}{}
	}
	window := m.windowForLocked(seqNum)
	plan := m.planRequestLocked(window, payload, request, requestID, requestIndex)
	m.mu.Unlock()

	telemetry.InjectContext(ctx, plan)
	if m.NextCh != nil {
		m.NextCh <- plan
	}
	return map[string]any{"status": "scheduled", "seq_num": seqNum, "request_id": requestID}
}

func (m *Mixer) HandleInLogBatchFormedMessage(payload map[string]any) map[string]any {
	ctx := telemetry.ExtractContext(context.Background(), payload)
	seqNum := common.GetInt(payload, protocol.FieldSeqNum)
	count := common.GetInt(payload, protocol.FieldCount)

	m.mu.Lock()
	window := m.windowForLocked(seqNum)
	window.closed = true
	window.count = count
	m.mu.Unlock()

	message := map[string]any{
		protocol.FieldType:   protocol.MessageTypeVerificationWindowClosed,
		protocol.FieldSeqNum: seqNum,
		protocol.FieldCount:  count,
	}
	telemetry.InjectContext(ctx, message)
	if m.NextCh != nil {
		m.NextCh <- message
	}
	return map[string]any{"status": "window_closed", "seq_num": seqNum, "count": count}
}

func (m *Mixer) windowForLocked(seqNum int) *scheduleWindow {
	if window, ok := m.windows[seqNum]; ok {
		return window
	}
	window := &scheduleWindow{
		seqNum:              seqNum,
		lastWriter:          make(map[string]string),
		activeReaders:       make(map[string]map[string]struct{}),
		workerByNode:        make(map[string]int),
		requestIndexByNode:  make(map[string]int),
		workerQueuedLengths: make([]int, m.WorkerCount),
	}
	m.windows[seqNum] = window
	return window
}

func (m *Mixer) planRequestLocked(window *scheduleWindow, payload map[string]any, request map[string]any, requestID string, requestIndex int) map[string]any {
	readKeys, writeKeys := m.getKeys(request)
	depSet := windowDependencies(window, readKeys, writeKeys)
	deps := sortedSet(depSet)
	worker := m.chooseWorkerLocked(window, deps)
	nodeID := protocol.NodeID(window.seqNum, requestIndex)

	window.workerByNode[nodeID] = worker
	window.requestIndexByNode[nodeID] = requestIndex
	window.workerQueuedLengths[worker]++
	updateWindowFrontier(window, nodeID, readKeys, writeKeys)

	return map[string]any{
		protocol.FieldType:        protocol.MessageTypeScheduledRequest,
		protocol.FieldSeqNum:      window.seqNum,
		protocol.FieldRequestIdx:  requestIndex,
		protocol.FieldRequestID:   requestID,
		protocol.FieldRequest:     request,
		protocol.FieldNodeID:      nodeID,
		protocol.FieldWorker:      worker,
		protocol.FieldDependsOn:   deps,
		protocol.FieldNDSeed:      payload[protocol.FieldNDSeed],
		protocol.FieldNDTimestamp: payload[protocol.FieldNDTimestamp],
		protocol.FieldReadKeys:    sortedSet(readKeys),
		protocol.FieldWriteKeys:   sortedSet(writeKeys),
	}
}

func windowDependencies(window *scheduleWindow, readKeys map[string]struct{}, writeKeys map[string]struct{}) map[string]struct{} {
	deps := make(map[string]struct{})
	for key := range readKeys {
		if writer := window.lastWriter[key]; writer != "" {
			deps[writer] = struct{}{}
		}
	}
	for key := range writeKeys {
		if writer := window.lastWriter[key]; writer != "" {
			deps[writer] = struct{}{}
		}
		for reader := range window.activeReaders[key] {
			deps[reader] = struct{}{}
		}
	}
	return deps
}

func (m *Mixer) chooseWorkerLocked(window *scheduleWindow, deps []string) int {
	if len(deps) == 0 {
		best := 0
		for worker := 1; worker < len(window.workerQueuedLengths); worker++ {
			if window.workerQueuedLengths[worker] < window.workerQueuedLengths[best] {
				best = worker
			}
		}
		return best
	}

	workers := make(map[int]struct{})
	for _, dep := range deps {
		worker, ok := window.workerByNode[dep]
		if ok {
			workers[worker] = struct{}{}
		}
	}
	if len(workers) == 1 {
		for worker := range workers {
			return worker
		}
	}

	chosenDep := ""
	chosenIdx := -1
	for _, dep := range deps {
		idx, ok := window.requestIndexByNode[dep]
		if !ok {
			continue
		}
		if idx > chosenIdx || (idx == chosenIdx && (chosenDep == "" || dep < chosenDep)) {
			chosenIdx = idx
			chosenDep = dep
		}
	}
	if chosenDep != "" {
		return window.workerByNode[chosenDep]
	}
	return 0
}

func updateWindowFrontier(window *scheduleWindow, nodeID string, readKeys map[string]struct{}, writeKeys map[string]struct{}) {
	for key := range writeKeys {
		window.lastWriter[key] = nodeID
		delete(window.activeReaders, key)
	}
	for key := range readKeys {
		if _, isWrite := writeKeys[key]; isWrite {
			continue
		}
		if _, ok := window.activeReaders[key]; !ok {
			window.activeReaders[key] = make(map[string]struct{})
		}
		window.activeReaders[key][nodeID] = struct{}{}
	}
}

func sortedSet(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func normalizeInLogRequest(v any) (map[string]any, bool) {
	req, ok := v.(map[string]any)
	return req, ok
}

func canonicalRequestID(id any) (string, bool) {
	if id == nil {
		return "", false
	}
	return fmt.Sprintf("%v", id), true
}

func normalizeRequestSlice(v any) ([]map[string]any, bool) {
	switch typed := v.(type) {
	case []map[string]any:
		return typed, true
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			req, ok := item.(map[string]any)
			if !ok {
				return nil, false
			}
			out = append(out, req)
		}
		return out, true
	default:
		return nil, false
	}
}
