package hotelworkflow

const (
	searchStageContextKey   = "hotel_search_stage"
	searchPayloadContextKey = "hotel_search_payload"

	searchStageAwaitGeo  = "await_geo"
	searchStageAwaitRate = "await_rate"
)

var (
	hotelGeoTargets  = []string{"node7", "node8", "node9"}
	hotelRateTargets = []string{"node10", "node11", "node12"}
)

func ExecuteRequestSearch(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "search_nearby" {
		return hotelErrorResponse(requestID, "unsupported op: "+op)
	}

	stageAny, _ := e.GetRequestContextValue(requestID, searchStageContextKey)
	stage, _ := stageAny.(string)
	payload := hotelPayload(request)

	switch stage {
	case "":
		normalizedPayload := normalizeHotelSearchPayload(payload)
		if errResponse := validateFrontendSearchPayload(requestID, normalizedPayload); errResponse != nil {
			return errResponse
		}
		if !e.SetRequestContextValue(requestID, searchPayloadContextKey, normalizedPayload) {
			return hotelErrorResponse(requestID, "failed to initialize search context")
		}
		if !e.SetRequestContextValue(requestID, searchStageContextKey, searchStageAwaitGeo) {
			return hotelErrorResponse(requestID, "failed to set search stage")
		}
		geoRequest := hotelNewNestedRequest(requestID, "geo", ndTimestamp, "nearby", map[string]any{
			"lat": normalizedPayload["lat"],
			"lon": normalizedPayload["lon"],
		})
		hotelDispatchNestedRequest(e, request, hotelGeoTargets, geoRequest)
		return hotelBlockedForNestedResponse(requestID)

	case searchStageAwaitGeo:
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok || !hotelNestedResponsesReady(nestedResponses, requestID, "geo") {
			return hotelBlockedForNestedResponse(requestID)
		}
		geoPayload := hotelNestedResponsePayload(hotelSelectedNestedResponse(nestedResponses, requestID, "geo"))
		hotelIDs := hotelPayloadStringSlice(geoPayload, "hotel_ids")
		if len(hotelIDs) == 0 {
			contextPayload := hotelSearchContextPayload(e, requestID, payload)
			ledger := hotelUpdateSearchLedger(e, contextPayload, []string{}, ndTimestamp)
			e.ClearRequestContext(requestID)
			return hotelAttachParentRequestID(request, map[string]any{
				"request_id":   requestID,
				"status":       "ok",
				"hotel_ids":    []string{},
				"search_count": ledger.Count,
			})
		}

		contextPayload := hotelSearchContextPayload(e, requestID, payload)
		if !e.SetRequestContextValue(requestID, searchStageContextKey, searchStageAwaitRate) {
			return hotelErrorResponse(requestID, "failed to advance search stage")
		}
		rateRequest := hotelNewNestedRequest(requestID, "rate", ndTimestamp, "get_rates", map[string]any{
			"hotel_ids": hotelIDs,
			"in_date":   contextPayload["in_date"],
			"out_date":  contextPayload["out_date"],
		})
		hotelDispatchNestedRequest(e, request, hotelRateTargets, rateRequest)
		return hotelBlockedForNestedResponse(requestID)

	case searchStageAwaitRate:
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok || !hotelNestedResponsesReady(nestedResponses, requestID, "rate") {
			return hotelBlockedForNestedResponse(requestID)
		}
		ratePayload := hotelNestedResponsePayload(hotelSelectedNestedResponse(nestedResponses, requestID, "rate"))
		hotelIDs := hotelUniqueStable(hotelPayloadStringSlice(ratePayload, "hotel_ids"))
		contextPayload := hotelSearchContextPayload(e, requestID, payload)
		ledger := hotelUpdateSearchLedger(e, contextPayload, hotelIDs, ndTimestamp)
		e.ClearRequestContext(requestID)
		return hotelAttachParentRequestID(request, map[string]any{
			"request_id":   requestID,
			"status":       "ok",
			"hotel_ids":    hotelIDs,
			"search_count": ledger.Count,
		})
	default:
		return hotelErrorResponse(requestID, "unknown search stage: "+stage)
	}
}

func normalizeHotelSearchPayload(payload map[string]any) map[string]any {
	normalized := copyHotelPayload(payload)
	if _, ok := hotelPayloadInt(normalized, "room_number"); !ok {
		normalized["room_number"] = 1
	}
	return normalized
}

func hotelSearchRoomNumber(payload map[string]any) int {
	roomNumber, ok := hotelPayloadInt(payload, "room_number")
	if !ok || roomNumber <= 0 {
		return 1
	}
	return roomNumber
}

func hotelSearchContextPayload(e workflowRuntime, requestID any, fallback map[string]any) map[string]any {
	contextPayloadAny, _ := e.GetRequestContextValue(requestID, searchPayloadContextKey)
	if contextPayload, ok := contextPayloadAny.(map[string]any); ok && contextPayload != nil {
		return contextPayload
	}
	return normalizeHotelSearchPayload(fallback)
}

func hotelUpdateSearchLedger(e workflowRuntime, payload map[string]any, hotelIDs []string, ndTimestamp float64) HotelSearchLedger {
	key := hotelSearchLedgerKey(payload)
	ledger, _ := decodeHotelSearchLedger(hotelReadKV(e, key))
	ledger.Count++
	ledger.LastSeenMicros = hotelTimestampMicros(ndTimestamp)
	ledger.LastHotelIDs = append([]string{}, hotelIDs...)
	hotelWriteKV(e, key, encodeJSON(ledger))
	return ledger
}
