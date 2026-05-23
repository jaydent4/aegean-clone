package telemetry

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
)

func AttrsFromPayload(payload map[string]any) []attribute.KeyValue {
	if !DetailedSpansEnabled() && !SummarySpansEnabled() {
		return nil
	}
	if payload == nil {
		return nil
	}
	attrs := make([]attribute.KeyValue, 0, 4)
	if requestID, ok := payload["request_id"]; ok && requestID != nil {
		attrs = append(attrs, attribute.String("request.id", fmt.Sprintf("%v", requestID)))
	}
	if seqNum, ok := payload["seq_num"]; ok && seqNum != nil {
		attrs = append(attrs, attribute.String("batch.seq_num", fmt.Sprintf("%v", seqNum)))
	}
	if msgType, ok := payload["type"].(string); ok && msgType != "" {
		attrs = append(attrs, attribute.String("msg.type", msgType))
	}
	if sender, ok := payload["sender"].(string); ok && sender != "" {
		attrs = append(attrs, attribute.String("msg.sender", sender))
	}
	return attrs
}
