package supersimpleworkflow

import "aegean/components/exec"

func ExecuteRequestServer(e *exec.Exec, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = e
	_ = ndSeed
	_ = ndTimestamp

	return map[string]any{
		"request_id": request["request_id"],
		"status":     "ok",
		"op":         request["op"],
	}
}

func InitState(e *exec.Exec) map[string]string {
	_ = e
	return map[string]string{}
}
