package aegeanworkflow

import (
	"strconv"
	"strings"
	"time"

	"aegean/common"
	"aegean/components/pbeo"
)

var pbeoBackendTargets = []string{"node4", "node5", "node6"}

func InitStatePBEO(runConfig map[string]any) map[string]string {
	keyCount := common.MustInt(runConfig, "key_count")
	valueLength := common.MustInt(runConfig, "value_length")

	initial := make(map[string]string, keyCount)
	value := strings.Repeat("x", valueLength)
	for i := 1; i <= keyCount; i++ {
		initial[strconv.Itoa(i)] = value
	}
	return initial
}

func executeRequestBasePBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	requestID := request["request_id"]
	op, _ := request["op"].(string)
	opPayload, _ := request["op_payload"].(map[string]any)

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

		tx.WriteKV(writeKey, writeValue)
		response["read_value"] = tx.ReadKV(readKey)
		response["status"] = "ok"
	default:
		response["status"] = "error"
		response["error"] = "Unknown op: " + op
	}

	return response
}

func ExecuteRequestBackendPBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	response := executeRequestBasePBEO(tx, request)
	if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
		response["parent_request_id"] = parentRequestID
	}
	return response
}

func ExecuteRequestMiddlePBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	requestID := request["request_id"]

	baseResponse := executeRequestBasePBEO(tx, request)
	if status, _ := baseResponse["status"].(string); status != "ok" {
		return baseResponse
	}

	nestedRequest := map[string]any{
		"type":              "request",
		"request_id":        requestID,
		"parent_request_id": requestID,
		"timestamp":         request["timestamp"],
		"op":                request["op"],
		"op_payload":        request["op_payload"],
	}
	tx.DispatchNestedRequestDirect(request, pbeoBackendTargets, nestedRequest)

	nestedResponses, _ := tx.WaitForNestedResponse(requestID)
	nested := firstCommittedNestedResponse(nestedResponses)
	if nested == nil {
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "missing committed nested response",
		}
	}

	output := map[string]any{
		"request_id": requestID,
		"status":     "ok",
	}
	for key, value := range baseResponse {
		if _, exists := output[key]; !exists {
			output[key] = value
		}
	}
	if selectedResponse, ok := nested["response"].(map[string]any); ok {
		for key, value := range selectedResponse {
			if key == "parent_request_id" {
				continue
			}
			if _, exists := output[key]; !exists {
				output[key] = value
			}
		}
	}

	return output
}

func firstCommittedNestedResponse(nestedResponses []map[string]any) map[string]any {
	for _, nested := range nestedResponses {
		if committed, _ := nested["pbeo_committed"].(bool); committed {
			return nested
		}
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); shimAggregated {
			return nested
		}
	}
	if len(nestedResponses) > 0 {
		return nestedResponses[0]
	}
	return nil
}
