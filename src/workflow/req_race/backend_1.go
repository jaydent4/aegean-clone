package reqraceworkflow

import (
	"math/rand"
	"time"

	"aegean/components/exec"
)

// ExecuteRequestBackend1 returns a fixed backend-specific value
func ExecuteRequestBackend1(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = e
	_ = ndSeed
	_ = ndTimestamp

	time.Sleep(time.Duration(rand.Float64() * 0.1 * float64(time.Second)))

	return map[string]any{
		"request_id": request["request_id"],
		"status":     "ok",
		"value":      1,
	}
}
