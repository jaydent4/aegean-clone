package spinworkflow

import "aegean/pbeo"

var spinPBEOBackendTargets = []string{"node4", "node5", "node6"}

func InitStatePBEO(runConfig map[string]any) map[string]string {
	_ = runConfig
	return map[string]string{}
}

func ExecuteRequestChainPBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	requestID := request["request_id"]

	spinForRequest(tx.GetRunConfig())
	if spinChainIsTerminal(tx.GetRunConfig()) {
		return spinOKResponse(requestID)
	}

	targets := spinChainNextTargets(tx.GetRunConfig())
	if len(targets) == 0 {
		return spinErrorResponse(requestID, "missing next service targets")
	}
	tx.DispatchNestedRequestDirect(request, targets, nestedSpinRequestFrom(request))

	nestedResponses, _ := tx.WaitForNestedResponse(requestID)
	if !hasCommittedSpinNestedResponse(nestedResponses) {
		return spinErrorResponse(requestID, "missing committed nested response")
	}

	return spinOKResponse(requestID)
}

func ExecuteRequestWideMiddlePBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	requestID := request["request_id"]
	runConfig := tx.GetRunConfig()
	nextBackend := spinWideNextBackend(runConfig, func() (any, bool) {
		return tx.GetRequestContextValue(requestID, spinWideNextBackendKey)
	})
	if nextBackend == 0 {
		spinForRequest(runConfig)
		nextBackend = 1
		if !tx.SetRequestContextValue(requestID, spinWideNextBackendKey, nextBackend) {
			return spinErrorResponse(requestID, "failed to initialize request continuation context")
		}
	}

	for nextBackend <= spinWideWidth(runConfig) {
		childID := spinWideChildRequestID(requestID, nextBackend)
		nestedResponses, _ := tx.GetNestedResponses(requestID)
		if !hasCommittedSpinNestedResponseByRequestID(nestedResponses, childID) {
			targets := spinWideBackendTargets(runConfig, nextBackend)
			if len(targets) == 0 {
				return spinErrorResponse(requestID, "missing backend service targets")
			}
			tx.DispatchNestedRequestDirect(request, targets, nestedSpinWideRequestFrom(request, nextBackend))
			return blockedForSpinNestedResponse(requestID)
		}

		nextBackend++
		if nextBackend <= spinWideWidth(runConfig) {
			if !tx.SetRequestContextValue(requestID, spinWideNextBackendKey, nextBackend) {
				return spinErrorResponse(requestID, "failed to update request continuation context")
			}
		}
	}

	tx.ClearRequestContext(requestID)
	return spinOKResponse(requestID)
}

func ExecuteRequestMiddlePBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	requestID := request["request_id"]

	spinForRequest(tx.GetRunConfig())
	tx.DispatchNestedRequestDirect(request, spinPBEOBackendTargets, nestedSpinRequestFrom(request))

	nestedResponses, _ := tx.WaitForNestedResponse(requestID)
	if !hasCommittedSpinNestedResponse(nestedResponses) {
		return spinErrorResponse(requestID, "missing committed nested response")
	}

	return spinOKResponse(requestID)
}

func ExecuteRequestBackendPBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	spinForRequest(tx.GetRunConfig())
	response := map[string]any{
		"request_id": request["request_id"],
		"status":     "ok",
	}
	if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
		response["parent_request_id"] = parentRequestID
	}
	return response
}

func hasCommittedSpinNestedResponse(nestedResponses []map[string]any) bool {
	for _, nested := range nestedResponses {
		if committed, _ := nested["pbeo_committed"].(bool); committed {
			return true
		}
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); shimAggregated {
			return true
		}
	}
	return false
}

func hasCommittedSpinNestedResponseByRequestID(nestedResponses []map[string]any, requestID string) bool {
	for _, nested := range nestedResponses {
		nestedRequestID, _ := nested["request_id"].(string)
		if nestedRequestID != requestID {
			continue
		}
		if committed, _ := nested["pbeo_committed"].(bool); committed {
			return true
		}
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); shimAggregated {
			return true
		}
	}
	return false
}
