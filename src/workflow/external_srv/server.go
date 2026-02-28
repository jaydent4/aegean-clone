package externalsrvworkflow

import (
	"aegean/common"
	"aegean/components/exec"
)

func ExecuteRequestServer(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "external_call" {
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "unsupported op: " + op,
		}
	}

	externalServiceNode := "node5"
	externalReq := map[string]any{
		"type":       "request",
		"request_id": requestID,
		"timestamp":  request["timestamp"],
		"sender":     e.Name,
		"op":         op,
		"op_payload": request["op_payload"],
	}

	// TODO: This should be 2 stages
	externalResp, err := common.SendMessage(externalServiceNode, 8000, externalReq)
	if err != nil {
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "external_service_call_failed",
		}
	}

	if _, ok := externalResp["request_id"]; !ok {
		externalResp["request_id"] = requestID
	}
	return externalResp
}

func InitState(e *exec.Exec) map[string]string {
	_ = e
	return map[string]string{}
}
