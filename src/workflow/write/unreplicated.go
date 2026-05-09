package writeworkflow

import (
	"fmt"
	"strings"

	"aegean/aegean/unreplicated"
	"aegean/common"
)

const (
	writeDirectMiddleStageContextKey = "write_direct_middle_stage"

	writeDirectMiddleStageAwaitNested = "await_nested"
)

func InitStateDirect(e *unreplicated.Engine) map[string]string {
	_ = e
	return map[string]string{}
}

func writeConfiguredValueDirect(e *unreplicated.Engine, requestID any, service string) {
	valueLength := common.MustInt(e.RunConfig, "write_value_size_bytes")
	value := strings.Repeat("x", valueLength)
	e.WriteKV(fmt.Sprintf("%s:%v", service, requestID), value)
}

func ExecuteRequestBackendDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	writeConfiguredValueDirect(e, requestID, "backend")

	response := map[string]any{
		"request_id": requestID,
		"status":     "ok",
	}
	if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
		response["parent_request_id"] = parentRequestID
	}
	return response
}

func ExecuteRequestMiddleDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, writeDirectMiddleStageContextKey)
	stage, _ := stageAny.(string)

	switch stage {
	case "":
		writeConfiguredValueDirect(e, requestID, "middle")
		if !e.SetRequestContextValue(requestID, writeDirectMiddleStageContextKey, writeDirectMiddleStageAwaitNested) {
			return map[string]any{
				"request_id": requestID,
				"status":     "error",
				"error":      "failed to initialize request continuation context",
			}
		}

		e.DispatchNestedRequestDirect(request, []string{"node4"}, map[string]any{
			"type":       "request",
			"request_id": requestID,
			"timestamp":  request["timestamp"],
		})
		return blockedForWriteNestedResponse(requestID)

	case writeDirectMiddleStageAwaitNested:
		nestedResponses, hasNested := e.GetNestedResponses(requestID)
		if !hasNested || len(nestedResponses) == 0 {
			return blockedForWriteNestedResponse(requestID)
		}
		nested := nestedResponses[0]
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
			return blockedForWriteNestedResponse(requestID)
		}

		e.ClearRequestContext(requestID)
		return map[string]any{
			"request_id": requestID,
			"status":     "ok",
		}

	default:
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "unknown middle stage: " + stage,
		}
	}
}
