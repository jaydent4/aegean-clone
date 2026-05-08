package externalsrvworkflow

import (
	"aegean/aegean/exec"
	"aegean/common"
	"fmt"
)

func ExecuteRequestServer(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, "external_srv_stage")
	stage, _ := stageAny.(string)
	op, _ := request["op"].(string)
	if op != "external_call" {
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "unsupported op: " + op,
		}
	}

	switch stage {
	case "":
		if !e.SetRequestContextValue(requestID, "external_srv_stage", "await_nested") {
			return map[string]any{
				"request_id": requestID,
				"status":     "error",
				"error":      "failed to initialize request continuation context",
			}
		}

		dispatchExternalServiceRequest(e, request)
		return map[string]any{
			"request_id": requestID,
			"status":     "blocked_for_nested_response",
		}
	case "await_nested":
		nestedResponses, hasNested := e.GetNestedResponses(requestID)
		if !hasNested {
			return map[string]any{
				"request_id": requestID,
				"status":     "blocked_for_nested_response",
			}
		}

		expectedNestedRequestID := fmt.Sprintf("%v/external_service", requestID)
		for _, nested := range nestedResponses {
			if nestedRequestID, _ := nested["request_id"].(string); nestedRequestID != expectedNestedRequestID {
				continue
			}
			if response, ok := nested["response"].(map[string]any); ok {
				final := map[string]any{
					"request_id": requestID,
				}
				for key, value := range response {
					if key == "request_id" {
						continue
					}
					final[key] = value
				}
				e.ClearRequestContext(requestID)
				return final
			}
		}

		return map[string]any{
			"request_id": requestID,
			"status":     "blocked_for_nested_response",
		}
	default:
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "unknown external_srv stage: " + stage,
		}
	}
}

func InitState(e *exec.Exec) map[string]string {
	_ = e
	return map[string]string{}
}

func dispatchExternalServiceRequest(e *exec.Exec, request map[string]any) {
	requestID := request["request_id"]
	outgoing := map[string]any{
		"type":              "request",
		"request_id":        fmt.Sprintf("%v/external_service", requestID),
		"parent_request_id": requestID,
		"timestamp":         request["timestamp"],
		"op":                "external_call",
		"op_payload":        request["op_payload"],
	}
	if common.BoolOrDefault(e.RunConfig, "nested_use_eo", false) {
		e.DispatchNestedRequestEO(request, []string{"node4"}, outgoing)
		return
	}
	e.DispatchNestedRequestDirect(request, []string{"node4"}, outgoing)
}
