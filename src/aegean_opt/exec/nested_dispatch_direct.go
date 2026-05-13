package exec

import (
	"context"
	"sort"

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
	telemetry.InjectContext(telemetry.ExtractContext(context.Background(), sourceRequest), prepared)
	return prepared, true
}

func sendNestedRequestDirect(targets []string, outgoing map[string]any) {
	serviceTargets := append([]string{}, targets...)
	sort.Strings(serviceTargets)
	for _, target := range serviceTargets {
		duplicated := cloneMapAny(outgoing)
		go func(target string, outgoing map[string]any) {
			_, _ = netx.SendMessage(target, 8000, outgoing)
		}(target, duplicated)
	}
}
