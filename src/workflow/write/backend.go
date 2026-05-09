package writeworkflow

import "aegean/aegean/exec"

func ExecuteRequestBackend(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	writeConfiguredValue(e, requestID, "backend")

	return map[string]any{
		"request_id": requestID,
		"status":     "ok",
	}
}
