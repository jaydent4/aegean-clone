package aegeanworkflow

import (
	"aegean/aegean/unreplicated"
	"strconv"
	"strings"
	"time"

	"aegean/common"
)

const (
	directFanoutStageContextKey        = "fanout_stage"
	directFanoutBaseResponseContextKey = "fanout_base_response"
	directFanoutStageAwaitNested       = "await_nested"
)

func InitStateDirect(e *unreplicated.Engine) map[string]string {
	keyCount := common.MustInt(e.RunConfig, "key_count")
	valueLength := common.MustInt(e.RunConfig, "value_length")

	initial := make(map[string]string, keyCount)
	value := strings.Repeat("x", valueLength)
	for i := 1; i <= keyCount; i++ {
		initial[strconv.Itoa(i)] = value
	}
	return initial
}

func executeRequestBaseDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64, targetNodes map[string]struct{}, everyN uint64) map[string]any {
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

func ExecuteRequestBackendDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return executeRequestBaseDirect(e, request, ndSeed, ndTimestamp, map[string]struct{}{}, 0)
}

func ExecuteRequestMiddleDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, directFanoutStageContextKey)
	stage, _ := stageAny.(string)

	switch stage {
	case "":
		response := executeRequestBaseDirect(e, request, ndSeed, ndTimestamp, map[string]struct{}{}, 0)
		if !e.SetRequestContextValue(requestID, directFanoutBaseResponseContextKey, response) {
			return map[string]any{"status": "error", "request_id": requestID, "error": "failed to initialize request continuation context"}
		}
		if !e.SetRequestContextValue(requestID, directFanoutStageContextKey, directFanoutStageAwaitNested) {
			return map[string]any{"status": "error", "request_id": requestID, "error": "failed to set fanout stage context"}
		}

		nestedRequest := map[string]any{
			"type":       "request",
			"request_id": requestID,
			"timestamp":  request["timestamp"],
			"op":         request["op"],
			"op_payload": request["op_payload"],
		}
		e.DispatchNestedRequestDirect(request, []string{"node4"}, nestedRequest)
		return map[string]any{"status": "blocked_for_nested_response", "request_id": requestID}
	case directFanoutStageAwaitNested:
		nestedResponses, hasNested := e.GetNestedResponses(requestID)
		if !hasNested || len(nestedResponses) == 0 {
			return map[string]any{"status": "blocked_for_nested_response", "request_id": requestID}
		}
		nested := nestedResponses[0]
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
			return map[string]any{"status": "blocked_for_nested_response", "request_id": requestID}
		}

		output := map[string]any{"request_id": requestID, "status": "ok"}
		if baseAny, ok := e.GetRequestContextValue(requestID, directFanoutBaseResponseContextKey); ok && baseAny != nil {
			if base, ok := baseAny.(map[string]any); ok {
				for key, value := range base {
					if _, exists := output[key]; !exists {
						output[key] = value
					}
				}
			}
		}
		if selectedResponse, ok := nested["response"].(map[string]any); ok {
			for key, value := range selectedResponse {
				if _, exists := output[key]; !exists {
					output[key] = value
				}
			}
		}

		e.DeleteRequestContextValue(requestID, directFanoutStageContextKey)
		e.DeleteRequestContextValue(requestID, directFanoutBaseResponseContextKey)
		return output
	default:
		return map[string]any{"status": "error", "request_id": requestID, "error": "unknown fanout stage: " + stage}
	}
}
