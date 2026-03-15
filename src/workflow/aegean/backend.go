package aegeanworkflow

import (
	"sync"
	"sync/atomic"
	"time"

	"aegean/common"
	"aegean/components/exec"
)

var divNodeCounters sync.Map

// Improvable: sometimes may cause unintended unliveness
func shouldDiverge(nodeName string, targetNodes map[string]struct{}, everyN uint64) bool {
	// nil targetNodes means all nodes are targeted
	if targetNodes != nil {
		if _, targeted := targetNodes[nodeName]; !targeted {
			return false
		}
	}
	// everyN <= 1 means always diverge
	if everyN <= 1 {
		return true
	}
	counterAny, _ := divNodeCounters.LoadOrStore(nodeName, &atomic.Uint64{})
	counter := counterAny.(*atomic.Uint64)
	requestCount := counter.Add(1)
	return requestCount%everyN == 0
}

func executeRequestBase(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64, targetNodes map[string]struct{}, everyN uint64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	opPayload, _ := request["op_payload"].(map[string]any)
	injectDivergence := shouldDiverge(e.Name, targetNodes, everyN)

	response := map[string]any{"request_id": requestID}

	switch op {
	case "spin_write_read":
		spinTime := common.GetFloat(opPayload, "spin_time")
		writeKey := common.GetString(opPayload, "write_key")
		writeValue := common.GetString(opPayload, "write_value")
		readKey := common.GetString(opPayload, "read_key")

		if spinTime > 0 {
			time.Sleep(time.Duration(spinTime * float64(time.Second)))
		}

		if injectDivergence {
			writeKey = writeKey + "_divergent_" + e.Name
			writeValue = writeValue + "_divergent_" + e.Name
		}

		e.WriteKV(writeKey, writeValue)
		response["read_value"] = e.ReadKV(readKey)
		response["status"] = "ok"
	default:
		response["status"] = "error"
		response["error"] = "Unknown op: " + op
	}

	return response
}

func ExecuteRequestBackend(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return executeRequestBase(e, request, ndSeed, ndTimestamp, map[string]struct{}{}, 0)
}

func ExecuteRequestBackendDivergeOneNode(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return executeRequestBase(e, request, ndSeed, ndTimestamp, map[string]struct{}{
		"node4": {},
	}, 4)
}

func ExecuteRequestBackendDivergeTwoNode(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return executeRequestBase(e, request, ndSeed, ndTimestamp, map[string]struct{}{
		"node4": {},
		"node5": {},
	}, 20)
}

func ExecuteRequestBackendDivergeThreeNode(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return executeRequestBase(e, request, ndSeed, ndTimestamp, map[string]struct{}{
		"node4": {},
		"node5": {},
		"node6": {},
	}, 20)
}
