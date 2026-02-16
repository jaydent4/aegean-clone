package reqraceworkflow

import (
	"aegean/components/exec"
)

// ExecuteRequestBackend2 returns a fixed backend-specific value
func ExecuteRequestBackend2(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = e
	_ = ndSeed
	_ = ndTimestamp

	return map[string]any{
		"request_id": request["request_id"],
		"status":     "ok",
		"value":      2,
	}
}
