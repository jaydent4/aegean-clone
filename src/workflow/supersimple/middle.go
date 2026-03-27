package supersimpleworkflow

import (
	"aegean/common"
	"aegean/components/exec"
	netx "aegean/net"
	"math/rand/v2"
)

const (
	supersimpleMiddleStageContextKey = "supersimple_middle_stage"

	supersimpleMiddleStageAwaitNested = "await_nested"
)

var supersimpleBackendTargets = []string{"node4"}

func ExecuteRequestMiddle(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, supersimpleMiddleStageContextKey)
	stage, _ := stageAny.(string)
	block_for_nested_response := common.BoolOrDefault(e.RunConfig, "supersimple_nesres_block", false)

	switch stage {
	case "":
		if !e.SetRequestContextValue(requestID, supersimpleMiddleStageContextKey, supersimpleMiddleStageAwaitNested) {
			return map[string]any{
				"request_id": requestID,
				"status":     "error",
				"error":      "failed to initialize request continuation context",
			}
		}

		go dispatchNestedRequest(e, request)

		if block_for_nested_response {
			return blockedForNestedResponse(requestID)
		} else {
			return map[string]any{
				"request_id": requestID,
				"status":     "ok",
			}
		}

	case supersimpleMiddleStageAwaitNested:
		nestedResponses, hasNested := e.GetNestedResponses(requestID)
		if !hasNested || len(nestedResponses) == 0 {
			return blockedForNestedResponse(requestID)
		}

		nested := nestedResponses[0]
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
			return blockedForNestedResponse(requestID)
		}

		response := map[string]any{
			"request_id": requestID,
			"status":     "ok",
		}
		if nestedResponse, ok := nested["response"].(map[string]any); ok {
			for key, value := range nestedResponse {
				if _, exists := response[key]; !exists {
					response[key] = value
				}
			}
		}

		e.ClearRequestContext(requestID)
		return response

	default:
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "unknown middle stage: " + stage,
		}
	}
}

func blockedForNestedResponse(requestID any) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "blocked_for_nested_response",
	}
}

func dispatchNestedRequest(e *exec.Exec, request map[string]any) {
	forward_prob := common.MustFloat64(e.RunConfig, "supersimple_forward_prob")
	requestID := request["request_id"]
	for _, target := range supersimpleBackendTargets {
		outgoing := map[string]any{
			"type":       "request",
			"request_id": requestID,
			"timestamp":  request["timestamp"],
			"sender":     e.Name,
			"op":         "default",
			"op_payload": map[string]any{},
		}

		if rand.Float64() < forward_prob {
			_, _ = netx.SendMessage(target, 8000, outgoing)
		}
	}
}
