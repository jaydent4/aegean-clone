package aegeanworkflow

import (
	"aegean/components/exec"
)

func ResponseNoop(_ *exec.Exec, payload map[string]any) map[string]any {
	return map[string]any{"status": "ignored_response", "request_id": payload["request_id"]}
}
