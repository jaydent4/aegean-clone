package reqraceworkflow

import (
	"aegean/common"
	"aegean/components/exec"
)

const middleFanoutStartedContextKey = "req_race_middle_fanout_started"

var backend1Targets = []string{"node5", "node6", "node7"}
var backend2Targets = []string{"node8", "node9", "node10"}
var backend3Targets = []string{"node11", "node12", "node13"}

// ExecuteRequestMiddle fans out to both backends and completes on the first nested response.
func ExecuteRequestMiddle(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	nestedResponses, hasNested := e.GetNestedResponses(requestID)
	_, started := e.GetRequestContextValue(requestID, middleFanoutStartedContextKey)

	// Stage 1: no local continuation context and no cached nested response.
	if !started && (!hasNested || len(nestedResponses) == 0) {
		if !e.SetRequestContextValue(requestID, middleFanoutStartedContextKey, true) {
			return map[string]any{
				"request_id": requestID,
				"status":     "error",
				"error":      "failed to initialize request continuation context",
			}
		}

		sendFanoutRequest := func(target string, outgoing map[string]any) {
			_, err := common.SendMessage(target, 8000, outgoing)
			if err != nil {
			}
		}

		groups := [][]string{backend1Targets, backend2Targets, backend3Targets}
		for _, targets := range groups {
			for i := 0; i < 2; i++ {
				for _, target := range targets {
					outgoing := map[string]any{
						"type":       "request",
						"request_id": requestID,
						"timestamp":  request["timestamp"],
						"sender":     e.Name,
						"op":         "default",
						"op_payload": map[string]any{},
					}
					go sendFanoutRequest(target, outgoing)
				}
			}
		}

		return map[string]any{
			"request_id": requestID,
			"status":     "blocked_for_nested_response",
		}
	}

	// Stage 2: either continuation context exists or a nested response is cached.
	if !hasNested || len(nestedResponses) == 0 {
		return map[string]any{
			"request_id": requestID,
			"status":     "blocked_for_nested_response",
		}
	}
	nested := nestedResponses[0]
	if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
		return map[string]any{
			"request_id": requestID,
			"status":     "blocked_for_nested_response",
		}
	}

	response := map[string]any{
		"request_id": requestID,
		"status":     "ok",
	}
	if nestedSender, _ := nested["sender"].(string); nestedSender != "" {
		response["backend_sender"] = nestedSender
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
}
