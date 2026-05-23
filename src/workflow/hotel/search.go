package hotelworkflow

import "time"

const (
	searchStageContextKey   = "hotel_search_stage"
	searchPayloadContextKey = "hotel_search_payload"
	searchWorkflowStatsKey  = "_workflow_stats"

	searchStageAwaitGeo  = "await_geo"
	searchStageAwaitRate = "await_rate"
)

var (
	hotelGeoTargets  = []string{"node3", "node4", "node5"}
	hotelRateTargets = []string{"node6", "node7", "node8"}
)

type hotelSearchWorkflowStats map[string]int64

func newHotelSearchWorkflowStats() hotelSearchWorkflowStats {
	return make(hotelSearchWorkflowStats, 32)
}

func (s hotelSearchWorkflowStats) inc(key string) {
	if s == nil {
		return
	}
	s[key]++
}

func (s hotelSearchWorkflowStats) add(key string, value int64) {
	if s == nil {
		return
	}
	s[key] += value
}

func (s hotelSearchWorkflowStats) addDuration(key string, duration time.Duration) {
	if s == nil {
		return
	}
	s[key] += duration.Microseconds()
}

func (s hotelSearchWorkflowStats) finish(response map[string]any, started time.Time) map[string]any {
	s.addDuration("hotel_search_total_us", time.Since(started))
	response[searchWorkflowStatsKey] = map[string]int64(s)
	return response
}

func (s hotelSearchWorkflowStats) finishBlocked(requestID any, started time.Time) map[string]any {
	s.inc("hotel_search_blocked_outputs")
	return s.finish(hotelBlockedForNestedResponse(requestID), started)
}

func ExecuteRequestSearch(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	callStart := time.Now()
	stats := newHotelSearchWorkflowStats()
	stats.inc("hotel_search_calls")
	_ = ndSeed

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "search_nearby" {
		return stats.finish(hotelErrorResponse(requestID, "unsupported op: "+op), callStart)
	}

	stageStart := time.Now()
	stageAny, _ := e.GetRequestContextValue(requestID, searchStageContextKey)
	stats.addDuration("hotel_search_stage_get_us", time.Since(stageStart))
	stage, _ := stageAny.(string)
	payloadStart := time.Now()
	payload := hotelPayload(request)
	stats.addDuration("hotel_search_payload_us", time.Since(payloadStart))

	switch stage {
	case "":
		stats.inc("hotel_search_stage_initial")
		normalizeStart := time.Now()
		normalizedPayload := normalizeHotelSearchPayload(payload)
		stats.addDuration("hotel_search_normalize_us", time.Since(normalizeStart))
		validateStart := time.Now()
		if errResponse := validateFrontendSearchPayload(requestID, normalizedPayload); errResponse != nil {
			stats.addDuration("hotel_search_validate_us", time.Since(validateStart))
			return stats.finish(errResponse, callStart)
		}
		stats.addDuration("hotel_search_validate_us", time.Since(validateStart))
		contextSetStart := time.Now()
		if !e.SetRequestContextValue(requestID, searchPayloadContextKey, normalizedPayload) {
			stats.addDuration("hotel_search_context_set_us", time.Since(contextSetStart))
			return stats.finish(hotelErrorResponse(requestID, "failed to initialize search context"), callStart)
		}
		if !e.SetRequestContextValue(requestID, searchStageContextKey, searchStageAwaitGeo) {
			stats.addDuration("hotel_search_context_set_us", time.Since(contextSetStart))
			return stats.finish(hotelErrorResponse(requestID, "failed to set search stage"), callStart)
		}
		stats.addDuration("hotel_search_context_set_us", time.Since(contextSetStart))
		nestedBuildStart := time.Now()
		geoRequest := hotelNewNestedRequest(requestID, "geo", ndTimestamp, "nearby", map[string]any{
			"lat": normalizedPayload["lat"],
			"lon": normalizedPayload["lon"],
		})
		stats.addDuration("hotel_search_nested_build_us", time.Since(nestedBuildStart))
		dispatchStart := time.Now()
		hotelDispatchNestedRequest(e, request, hotelServiceTargets(e, "geo", hotelGeoTargets), geoRequest)
		stats.addDuration("hotel_search_dispatch_us", time.Since(dispatchStart))
		stats.inc("hotel_search_dispatches")
		return stats.finishBlocked(requestID, callStart)

	case searchStageAwaitGeo:
		stats.inc("hotel_search_stage_await_geo")
		nestedStart := time.Now()
		geoNested, nestedEntries := hotelSelectedNestedResponseFromRuntime(e, requestID, "geo")
		stats.addDuration("hotel_search_get_nested_us", time.Since(nestedStart))
		stats.add("hotel_search_nested_response_entries", int64(nestedEntries))
		readyStart := time.Now()
		ready := geoNested != nil
		stats.addDuration("hotel_search_nested_ready_us", time.Since(readyStart))
		if !ready {
			stats.inc("hotel_search_wait_again_outputs")
			return stats.finishBlocked(requestID, callStart)
		}
		selectStart := time.Now()
		geoPayload := hotelNestedResponsePayload(geoNested)
		hotelIDs := hotelPayloadStringSlice(geoPayload, "hotel_ids")
		stats.addDuration("hotel_search_nested_select_us", time.Since(selectStart))
		if len(hotelIDs) == 0 {
			contextPayloadStart := time.Now()
			contextPayload := hotelSearchContextPayload(e, requestID, payload)
			stats.addDuration("hotel_search_context_payload_us", time.Since(contextPayloadStart))
			ledgerStart := time.Now()
			ledger := hotelUpdateSearchLedgerWithStats(e, contextPayload, []string{}, ndTimestamp, stats)
			stats.addDuration("hotel_search_ledger_us", time.Since(ledgerStart))
			clearStart := time.Now()
			e.ClearRequestContext(requestID)
			stats.addDuration("hotel_search_clear_context_us", time.Since(clearStart))
			responseBuildStart := time.Now()
			response := hotelAttachParentRequestID(request, map[string]any{
				"request_id":   requestID,
				"status":       "ok",
				"hotel_ids":    []string{},
				"search_count": ledger.Count,
			})
			stats.addDuration("hotel_search_response_build_us", time.Since(responseBuildStart))
			stats.inc("hotel_search_complete_outputs")
			stats.inc("hotel_search_empty_geo_outputs")
			return stats.finish(response, callStart)
		}

		contextPayloadStart := time.Now()
		contextPayload := hotelSearchContextPayload(e, requestID, payload)
		stats.addDuration("hotel_search_context_payload_us", time.Since(contextPayloadStart))
		contextSetStart := time.Now()
		if !e.SetRequestContextValue(requestID, searchStageContextKey, searchStageAwaitRate) {
			stats.addDuration("hotel_search_context_set_us", time.Since(contextSetStart))
			return stats.finish(hotelErrorResponse(requestID, "failed to advance search stage"), callStart)
		}
		stats.addDuration("hotel_search_context_set_us", time.Since(contextSetStart))
		nestedBuildStart := time.Now()
		rateRequest := hotelNewNestedRequest(requestID, "rate", ndTimestamp, "get_rates", map[string]any{
			"hotel_ids": hotelIDs,
			"in_date":   contextPayload["in_date"],
			"out_date":  contextPayload["out_date"],
		})
		stats.addDuration("hotel_search_nested_build_us", time.Since(nestedBuildStart))
		dispatchStart := time.Now()
		hotelDispatchNestedRequest(e, request, hotelServiceTargets(e, "rate", hotelRateTargets), rateRequest)
		stats.addDuration("hotel_search_dispatch_us", time.Since(dispatchStart))
		stats.inc("hotel_search_dispatches")
		return stats.finishBlocked(requestID, callStart)

	case searchStageAwaitRate:
		stats.inc("hotel_search_stage_await_rate")
		nestedStart := time.Now()
		rateNested, nestedEntries := hotelSelectedNestedResponseFromRuntime(e, requestID, "rate")
		stats.addDuration("hotel_search_get_nested_us", time.Since(nestedStart))
		stats.add("hotel_search_nested_response_entries", int64(nestedEntries))
		readyStart := time.Now()
		ready := rateNested != nil
		stats.addDuration("hotel_search_nested_ready_us", time.Since(readyStart))
		if !ready {
			stats.inc("hotel_search_wait_again_outputs")
			return stats.finishBlocked(requestID, callStart)
		}
		selectStart := time.Now()
		ratePayload := hotelNestedResponsePayload(rateNested)
		hotelIDs := hotelUniqueStable(hotelPayloadStringSlice(ratePayload, "hotel_ids"))
		stats.addDuration("hotel_search_nested_select_us", time.Since(selectStart))
		contextPayloadStart := time.Now()
		contextPayload := hotelSearchContextPayload(e, requestID, payload)
		stats.addDuration("hotel_search_context_payload_us", time.Since(contextPayloadStart))
		ledgerStart := time.Now()
		ledger := hotelUpdateSearchLedgerWithStats(e, contextPayload, hotelIDs, ndTimestamp, stats)
		stats.addDuration("hotel_search_ledger_us", time.Since(ledgerStart))
		clearStart := time.Now()
		e.ClearRequestContext(requestID)
		stats.addDuration("hotel_search_clear_context_us", time.Since(clearStart))
		responseBuildStart := time.Now()
		response := hotelAttachParentRequestID(request, map[string]any{
			"request_id":   requestID,
			"status":       "ok",
			"hotel_ids":    hotelIDs,
			"search_count": ledger.Count,
		})
		stats.addDuration("hotel_search_response_build_us", time.Since(responseBuildStart))
		stats.inc("hotel_search_complete_outputs")
		return stats.finish(response, callStart)
	default:
		return stats.finish(hotelErrorResponse(requestID, "unknown search stage: "+stage), callStart)
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
	return hotelUpdateSearchLedgerWithStats(e, payload, hotelIDs, ndTimestamp, nil)
}

func hotelUpdateSearchLedgerWithStats(e workflowRuntime, payload map[string]any, hotelIDs []string, ndTimestamp float64, stats hotelSearchWorkflowStats) HotelSearchLedger {
	keyStart := time.Now()
	key := hotelSearchLedgerKey(payload)
	stats.addDuration("hotel_search_ledger_key_us", time.Since(keyStart))
	readStart := time.Now()
	stateReadStart := time.Now()
	raw := e.ReadKV(key)
	stats.addDuration("hotel_search_ledger_state_read_us", time.Since(stateReadStart))
	stats.addDuration("hotel_search_ledger_read_us", time.Since(readStart))
	decodeStart := time.Now()
	ledger, _ := decodeHotelSearchLedger(raw)
	stats.addDuration("hotel_search_ledger_decode_us", time.Since(decodeStart))
	mutateStart := time.Now()
	ledger.Count++
	ledger.LastSeenMicros = hotelTimestampMicros(ndTimestamp)
	ledger.LastHotelIDs = append([]string{}, hotelIDs...)
	stats.addDuration("hotel_search_ledger_mutate_us", time.Since(mutateStart))
	encodeStart := time.Now()
	encoded := encodeJSON(ledger)
	stats.addDuration("hotel_search_ledger_encode_us", time.Since(encodeStart))
	writeStart := time.Now()
	stateWriteStart := time.Now()
	e.WriteKV(key, encoded)
	stats.addDuration("hotel_search_ledger_state_write_us", time.Since(stateWriteStart))
	stats.addDuration("hotel_search_ledger_write_us", time.Since(writeStart))
	return ledger
}
