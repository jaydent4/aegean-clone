package aegeanworkflow

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"aegean/common"
	"aegean/components/exec"
)

const fanoutBaseResponseContextKey = "fanout_base_response"
const divergenceEveryNRequests uint64 = 3

var divergenceTargetNodeNames = map[string]struct{}{
	"node7": {},
	"node8": {},
}

var targetedNodeRequestCounters sync.Map

func shouldInjectArtificialDivergence(execNodeName string) bool {
	if _, targeted := divergenceTargetNodeNames[execNodeName]; !targeted {
		return false
	}
	counterAny, _ := targetedNodeRequestCounters.LoadOrStore(execNodeName, &atomic.Uint64{})
	counter := counterAny.(*atomic.Uint64)
	requestCount := counter.Add(1)
	return requestCount%divergenceEveryNRequests == 0
}

func ExecuteRequest(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	requestID := request["request_id"]
	op, _ := request["op"].(string)
	opPayload, _ := request["op_payload"].(map[string]any)
	injectDivergence := shouldInjectArtificialDivergence(e.Name)

	// Execute a single request and return the response
	response := map[string]any{"request_id": requestID}

	switch op {
	case "spin_write_read":
		spinTime := common.GetFloat(opPayload, "spin_time")
		writeKey := common.GetString(opPayload, "write_key")
		writeValue := common.GetString(opPayload, "write_value")
		readKey := common.GetString(opPayload, "read_key")

		// Spin for the given time
		if spinTime > 0 {
			time.Sleep(time.Duration(spinTime * float64(time.Second)))
		}

		if injectDivergence {
			writeKey = writeKey + "_divergent_" + e.Name
			writeValue = writeValue + "_divergent_" + e.Name
			log.Printf("%s: injecting artificial divergence on request %v (write_key=%q write_value=%q)", e.Name, requestID, writeKey, writeValue)
		}

		// Write to key
		e.WriteKV(writeKey, writeValue)
		// Read from key
		response["read_value"] = e.ReadKV(readKey)
		response["status"] = "ok"
	default:
		response["status"] = "error"
		response["error"] = "Unknown op: " + op
	}

	_ = ndSeed
	_ = ndTimestamp
	return response
}

func ExecuteRequestFanout(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	requestID := request["request_id"]
	// First stage: do local work and fan out nested requests asynchronously.
	if _, started := e.GetRequestContextValue(requestID, fanoutBaseResponseContextKey); !started {
		response := ExecuteRequest(e, request, ndSeed, ndTimestamp)
		if !e.SetRequestContextValue(requestID, fanoutBaseResponseContextKey, response) {
			return map[string]any{
				"status":     "error",
				"request_id": requestID,
				"error":      "failed to initialize request continuation context",
			}
		}

		fanoutTargets := []string{"node7", "node8", "node9"}
		for _, target := range fanoutTargets {
			outgoing := map[string]any{
				"type":       "request",
				"request_id": requestID,
				"timestamp":  request["timestamp"],
				"sender":     e.Name,
				"op":         request["op"],
				"op_payload": request["op_payload"],
			}
			go func(target string, outgoing map[string]any) {
				_, err := common.SendMessage(target, 8000, outgoing)
				if err != nil {
					log.Printf("Fanout from %s to %s failed: %v", e.Name, target, err)
				}
			}(target, outgoing)
		}

		return map[string]any{
			"status":     "blocked_for_nested_response",
			"request_id": requestID,
		}
	}

	// Continuation stage: consume next nested response from scheduler-owned queue.
	nested, ok := e.ConsumeNestedResponse(requestID)
	if !ok || nested == nil {
		return map[string]any{
			"status":     "blocked_for_nested_response",
			"request_id": requestID,
		}
	}
	if fanoutDone, output := processNestedFanoutResponse(e, requestID, nested); fanoutDone {
		e.ClearRequestContext(requestID)
		return output
	}
	return map[string]any{
		"status":     "blocked_for_nested_response",
		"request_id": requestID,
	}
}

func processNestedFanoutResponse(e *exec.Exec, requestID any, nested map[string]any) (bool, map[string]any) {
	if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
		return false, nil
	}

	output := map[string]any{
		"request_id": requestID,
		"status":     "ok",
	}
	if baseAny, ok := e.GetRequestContextValue(requestID, fanoutBaseResponseContextKey); ok && baseAny != nil {
		if base, ok := baseAny.(map[string]any); ok {
			for key, value := range base {
				if _, exists := output[key]; !exists {
					output[key] = value
				}
			}
		}
	}
	// Keep client-facing response flat so it matches expected_result shape in traces
	if selectedResponse, ok := nested["response"].(map[string]any); ok {
		for key, value := range selectedResponse {
			if _, exists := output[key]; !exists {
				output[key] = value
			}
		}
	}
	return true, output
}
