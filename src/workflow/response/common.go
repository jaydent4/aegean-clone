package responseworkflow

import (
	"strings"
	"sync"

	"aegean/common"
)

const (
	responseMiddleStageContextKey  = "response_middle_stage"
	responseMiddleStageAwaitNested = "await_nested"

	defaultResponseSizeBytes = 10 * 1024 * 1024
)

var responsePayloadCache sync.Map

func blockedForNestedResponse(requestID any) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "blocked_for_nested_response",
	}
}

func makeBackendResponse(runConfig map[string]any, requestID any) map[string]any {
	size := common.IntOrDefault(runConfig, "response_size_bytes", defaultResponseSizeBytes)
	if size < 0 {
		size = 0
	}

	payloadAny, _ := responsePayloadCache.LoadOrStore(size, strings.Repeat("x", size))
	return map[string]any{
		"request_id": requestID,
		"status":     "ok",
		"payload":    payloadAny.(string),
	}
}

func nestedRequestFrom(request map[string]any) map[string]any {
	nested := make(map[string]any, len(request)+1)
	for key, value := range request {
		nested[key] = value
	}
	nested["type"] = "request"
	return nested
}
