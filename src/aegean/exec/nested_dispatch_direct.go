package exec

import (
	"context"
	"sort"
	"time"

	netx "aegean/net"
	"aegean/telemetry"
)

func (e *Exec) DispatchNestedRequestDirect(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
	prepared, ok := e.prepareNestedDispatchPayload(sourceRequest, outgoing)
	if !ok || len(targets) == 0 {
		return
	}

	sendNestedRequestDirect(targets, prepared)
}

func (e *Exec) PrepareNestedRequestPayload(sourceRequest map[string]any, outgoing map[string]any) (map[string]any, bool) {
	return e.prepareNestedDispatchPayload(sourceRequest, outgoing)
}

func (e *Exec) prepareNestedDispatchPayload(sourceRequest map[string]any, outgoing map[string]any) (map[string]any, bool) {
	if outgoing == nil {
		return nil, false
	}

	prepared := cloneMapAny(outgoing)
	prepared["sender"] = e.Name
	if e.nestedTimingLogsEnabled() {
		prepared[nestedTraceEnabledKey] = true
	}
	telemetry.InjectContext(telemetry.ExtractContext(context.Background(), sourceRequest), prepared)
	return prepared, true
}

func sendNestedRequestDirect(targets []string, outgoing map[string]any) {
	serviceTargets := append([]string{}, targets...)
	sort.Strings(serviceTargets)
	traceEnabled := nestedTraceEnabled(outgoing)
	var dispatchUnixNano int64
	if traceEnabled {
		dispatchUnixNano = time.Now().UnixNano()
	}
	for _, target := range serviceTargets {
		duplicated := cloneMapAny(outgoing)
		if traceEnabled {
			duplicated[nestedDispatchSendUnixNanoKey] = dispatchUnixNano
			if sender, ok := duplicated["sender"].(string); ok && sender != "" {
				duplicated[nestedDispatchSourceKey] = sender
			}
		}
		go func(target string, outgoing map[string]any) {
			_, _ = netx.SendMessage(target, 8000, outgoing)
		}(target, duplicated)
	}
}
