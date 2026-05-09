package writeworkflow

import (
	"fmt"
	"strings"

	"aegean/common"
	"aegean/pbeo"
)

var writePBEOBackendTargets = []string{"node4", "node5", "node6"}

func InitStatePBEO(runConfig map[string]any) map[string]string {
	_ = runConfig
	return map[string]string{}
}

func writeConfiguredValuePBEO(tx *pbeo.Txn, requestID any, service string) {
	valueLength := common.MustInt(tx.GetRunConfig(), "write_value_size_bytes")
	value := strings.Repeat("x", valueLength)
	tx.WriteKV(fmt.Sprintf("%s:%v", service, requestID), value)
}

func ExecuteRequestBackendPBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	requestID := request["request_id"]
	writeConfiguredValuePBEO(tx, requestID, "backend")

	response := map[string]any{
		"request_id": requestID,
		"status":     "ok",
	}
	if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
		response["parent_request_id"] = parentRequestID
	}
	return response
}

func ExecuteRequestMiddlePBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	requestID := request["request_id"]
	writeConfiguredValuePBEO(tx, requestID, "middle")

	tx.DispatchNestedRequestDirect(request, writePBEOBackendTargets, map[string]any{
		"type":              "request",
		"request_id":        requestID,
		"parent_request_id": requestID,
		"timestamp":         request["timestamp"],
	})

	nestedResponses, _ := tx.WaitForNestedResponse(requestID)
	if !hasCommittedNestedResponse(nestedResponses) {
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "missing committed nested response",
		}
	}

	return map[string]any{
		"request_id": requestID,
		"status":     "ok",
	}
}

func hasCommittedNestedResponse(nestedResponses []map[string]any) bool {
	for _, nested := range nestedResponses {
		if committed, _ := nested["pbeo_committed"].(bool); committed {
			return true
		}
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); shimAggregated {
			return true
		}
	}
	return false
}
