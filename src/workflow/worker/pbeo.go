package workerworkflow

import "aegean/pbeo"

var workerPBEOBackendTargets = []string{"node4", "node5", "node6"}

func InitStatePBEO(runConfig map[string]any) map[string]string {
	_ = runConfig
	return map[string]string{}
}

func ExecuteRequestMiddlePBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	requestID := request["request_id"]

	spinForWorkerRequest(tx.GetRunConfig())
	tx.DispatchNestedRequestDirect(request, workerPBEOBackendTargets, nestedWorkerRequestFrom(request))

	nestedResponses, _ := tx.WaitForNestedResponse(requestID)
	if !hasCommittedWorkerNestedResponse(nestedResponses) {
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "missing committed nested response",
		}
	}

	return map[string]any{
		"request_id": requestID,
		"status":     "ok",
	}
}

func ExecuteRequestBackendPBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	spinForWorkerRequest(tx.GetRunConfig())
	response := map[string]any{
		"request_id": request["request_id"],
		"status":     "ok",
	}
	if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
		response["parent_request_id"] = parentRequestID
	}
	return response
}

func hasCommittedWorkerNestedResponse(nestedResponses []map[string]any) bool {
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
