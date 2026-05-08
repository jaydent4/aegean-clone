package exec

import (
	"fmt"
	"math"
	"strconv"
)

type requestState int

const (
	requestRunnable requestState = iota
	requestExecuting
	requestWaiting
	requestFinished
)

type scheduledRequest struct {
	index      int
	batchSeq   int
	id         string
	payload    map[string]any
	state      requestState
	output     map[string]any
	nestedSeen int
}

type workerResult struct {
	req    *scheduledRequest
	output map[string]any
}

func requestIDForSchedule(request map[string]any, fallback int) string {
	if request == nil {
		return fmt.Sprintf("__internal_%d", fallback)
	}
	id, ok := request["request_id"]
	if !ok || id == nil {
		return fmt.Sprintf("__internal_%d", fallback)
	}
	if canonical, ok := canonicalRequestID(id); ok {
		return canonical
	}
	return fmt.Sprintf("__internal_%d", fallback)
}

func canonicalRequestID(id any) (string, bool) {
	if id == nil {
		return "", false
	}
	switch v := id.(type) {
	case string:
		return v, true
	case int:
		return strconv.Itoa(v), true
	case int64:
		return strconv.FormatInt(v, 10), true
	case float64:
		if math.Trunc(v) == v {
			return strconv.FormatInt(int64(v), 10), true
		}
		return strconv.FormatFloat(v, 'f', -1, 64), true
	case float32:
		f := float64(v)
		if math.Trunc(f) == f {
			return strconv.FormatInt(int64(f), 10), true
		}
		return strconv.FormatFloat(f, 'f', -1, 32), true
	default:
		return fmt.Sprintf("%v", v), true
	}
}
