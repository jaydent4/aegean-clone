package noseexternalsrvworkflow

import (
	"aegean/nodes"
	"math/rand/v2"
)

func InitExternalService(es *nodes.ExternalService) {
	_ = es
}

func ExternalServiceLogic(es *nodes.ExternalService, payload map[string]any) map[string]any {
	_ = es
	return map[string]any{
		"request_id": payload["request_id"],
		"status":     "ok",
		"value":      rand.IntN(1_000_000),
	}
}
