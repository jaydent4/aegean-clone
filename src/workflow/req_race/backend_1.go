package reqraceworkflow

import (
	"aegean/aegean/exec"
)

// ExecuteRequestBackend1 returns a fixed backend-specific value
func ExecuteRequestBackend1(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = e
	_ = ndSeed
	_ = ndTimestamp

	return map[string]any{
		"request_id":        request["request_id"],
		"status":            "ok",
		"value":             1,
		"parent_request_id": request["parent_request_id"],
	}
}
