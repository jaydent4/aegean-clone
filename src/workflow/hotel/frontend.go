package hotelworkflow

const (
	frontendStageContextKey   = "hotel_frontend_stage"
	frontendPayloadContextKey = "hotel_frontend_payload"

	frontendStageAwaitSearch                 = "await_search"
	frontendStageAwaitAvailability           = "await_availability"
	frontendStageAwaitSearchProfiles         = "await_search_profiles"
	frontendStageAwaitRecommendation         = "await_recommendation"
	frontendStageAwaitRecommendationProfiles = "await_recommendation_profiles"
	frontendStageAwaitUserCheck              = "await_user_check"
	frontendStageAwaitReservationUser        = "await_reservation_user"
	frontendStageAwaitReservationWrite       = "await_reservation_write"
)

var (
	hotelSearchTargets         = []string{"node4", "node5", "node6"}
	hotelReservationTargets    = []string{"node13", "node14", "node15"}
	hotelProfileTargets        = []string{"node16", "node17", "node18"}
	hotelRecommendationTargets = []string{"node19", "node20", "node21"}
	hotelUserTargets           = []string{"node22", "node23", "node24"}
)

func ExecuteRequestFrontend(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	stageAny, _ := e.GetRequestContextValue(requestID, frontendStageContextKey)
	stage, _ := stageAny.(string)

	switch op {
	case "search_hotels":
		return executeFrontendSearch(e, request, stage, ndTimestamp)
	case "recommend_hotels":
		return executeFrontendRecommendation(e, request, stage, ndTimestamp)
	case "check_user":
		return executeFrontendCheckUser(e, request, stage, ndTimestamp)
	case "make_reservation":
		return executeFrontendReservation(e, request, stage, ndTimestamp)
	default:
		return hotelErrorResponse(requestID, "unsupported op: "+op)
	}
}

func executeFrontendSearch(e workflowRuntime, request map[string]any, stage string, ndTimestamp float64) map[string]any {
	requestID := request["request_id"]
	payload := hotelPayload(request)

	switch stage {
	case "":
		normalizedPayload := normalizeFrontendSearchPayload(payload)
		if errResponse := validateFrontendSearchPayload(requestID, normalizedPayload); errResponse != nil {
			return errResponse
		}
		if !e.SetRequestContextValue(requestID, frontendPayloadContextKey, normalizedPayload) {
			return hotelErrorResponse(requestID, "failed to initialize frontend search context")
		}
		if !e.SetRequestContextValue(requestID, frontendStageContextKey, frontendStageAwaitSearch) {
			return hotelErrorResponse(requestID, "failed to set frontend search stage")
		}
		searchRequest := hotelNewNestedRequest(requestID, "search", ndTimestamp, "search_nearby", map[string]any{
			"lat":         normalizedPayload["lat"],
			"lon":         normalizedPayload["lon"],
			"in_date":     normalizedPayload["in_date"],
			"out_date":    normalizedPayload["out_date"],
			"room_number": normalizedPayload["room_number"],
		})
		hotelDispatchNestedRequest(e, request, hotelSearchTargets, searchRequest)
		return hotelBlockedForNestedResponse(requestID)

	case frontendStageAwaitSearch:
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok || !hotelNestedResponsesReady(nestedResponses, requestID, "search") {
			return hotelBlockedForNestedResponse(requestID)
		}
		searchPayload := hotelNestedResponsePayload(hotelSelectedNestedResponse(nestedResponses, requestID, "search"))
		hotelIDs := hotelPayloadStringSlice(searchPayload, "hotel_ids")
		if len(hotelIDs) == 0 {
			e.ClearRequestContext(requestID)
			return hotelAttachParentRequestID(request, map[string]any{
				"request_id": requestID,
				"status":     "ok",
				"result":     hotelGeoJSONResponse(nil),
			})
		}

		contextPayloadAny, _ := e.GetRequestContextValue(requestID, frontendPayloadContextKey)
		contextPayload, _ := contextPayloadAny.(map[string]any)
		if !e.SetRequestContextValue(requestID, frontendStageContextKey, frontendStageAwaitAvailability) {
			return hotelErrorResponse(requestID, "failed to advance frontend search stage")
		}
		reservationRequest := hotelNewNestedRequest(requestID, "reservation", ndTimestamp, "check_availability", map[string]any{
			"hotel_ids":   hotelIDs,
			"in_date":     contextPayload["in_date"],
			"out_date":    contextPayload["out_date"],
			"room_number": contextPayload["room_number"],
		})
		hotelDispatchNestedRequest(e, request, hotelReservationTargets, reservationRequest)
		return hotelBlockedForNestedResponse(requestID)

	case frontendStageAwaitAvailability:
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok || !hotelNestedResponsesReady(nestedResponses, requestID, "reservation") {
			return hotelBlockedForNestedResponse(requestID)
		}
		reservationPayload := hotelNestedResponsePayload(hotelSelectedNestedResponse(nestedResponses, requestID, "reservation"))
		hotelIDs := hotelPayloadStringSlice(reservationPayload, "hotel_ids")
		if len(hotelIDs) == 0 {
			e.ClearRequestContext(requestID)
			return hotelAttachParentRequestID(request, map[string]any{
				"request_id": requestID,
				"status":     "ok",
				"result":     hotelGeoJSONResponse(nil),
			})
		}

		contextPayloadAny, _ := e.GetRequestContextValue(requestID, frontendPayloadContextKey)
		contextPayload, _ := contextPayloadAny.(map[string]any)
		if !e.SetRequestContextValue(requestID, frontendStageContextKey, frontendStageAwaitSearchProfiles) {
			return hotelErrorResponse(requestID, "failed to advance frontend profile stage")
		}
		profileRequest := hotelNewNestedRequest(requestID, "profile", ndTimestamp, "get_profiles", map[string]any{
			"hotel_ids": hotelIDs,
			"locale":    contextPayload["locale"],
		})
		hotelDispatchNestedRequest(e, request, hotelProfileTargets, profileRequest)
		return hotelBlockedForNestedResponse(requestID)

	case frontendStageAwaitSearchProfiles:
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok || !hotelNestedResponsesReady(nestedResponses, requestID, "profile") {
			return hotelBlockedForNestedResponse(requestID)
		}
		profilePayload := hotelNestedResponsePayload(hotelSelectedNestedResponse(nestedResponses, requestID, "profile"))
		profiles := hotelProfilesFromPayload(profilePayload["profiles"])
		e.ClearRequestContext(requestID)
		return hotelAttachParentRequestID(request, map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"result":     hotelGeoJSONResponse(profiles),
		})
	default:
		return hotelErrorResponse(requestID, "unknown frontend search stage: "+stage)
	}
}

func executeFrontendRecommendation(e workflowRuntime, request map[string]any, stage string, ndTimestamp float64) map[string]any {
	requestID := request["request_id"]
	payload := hotelPayload(request)

	switch stage {
	case "":
		normalizedPayload := normalizeFrontendRecommendationPayload(payload)
		if errResponse := validateFrontendRecommendationPayload(requestID, normalizedPayload); errResponse != nil {
			return errResponse
		}
		if !e.SetRequestContextValue(requestID, frontendPayloadContextKey, normalizedPayload) {
			return hotelErrorResponse(requestID, "failed to initialize frontend recommendation context")
		}
		if !e.SetRequestContextValue(requestID, frontendStageContextKey, frontendStageAwaitRecommendation) {
			return hotelErrorResponse(requestID, "failed to set frontend recommendation stage")
		}
		recommendationRequest := hotelNewNestedRequest(requestID, "recommendation", ndTimestamp, "get_recommendations", map[string]any{
			"require": normalizedPayload["require"],
			"lat":     normalizedPayload["lat"],
			"lon":     normalizedPayload["lon"],
		})
		hotelDispatchNestedRequest(e, request, hotelRecommendationTargets, recommendationRequest)
		return hotelBlockedForNestedResponse(requestID)

	case frontendStageAwaitRecommendation:
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok || !hotelNestedResponsesReady(nestedResponses, requestID, "recommendation") {
			return hotelBlockedForNestedResponse(requestID)
		}
		recommendationPayload := hotelNestedResponsePayload(hotelSelectedNestedResponse(nestedResponses, requestID, "recommendation"))
		hotelIDs := hotelPayloadStringSlice(recommendationPayload, "hotel_ids")
		if len(hotelIDs) == 0 {
			e.ClearRequestContext(requestID)
			return hotelAttachParentRequestID(request, map[string]any{
				"request_id": requestID,
				"status":     "ok",
				"result":     hotelGeoJSONResponse(nil),
			})
		}

		contextPayloadAny, _ := e.GetRequestContextValue(requestID, frontendPayloadContextKey)
		contextPayload, _ := contextPayloadAny.(map[string]any)
		if !e.SetRequestContextValue(requestID, frontendStageContextKey, frontendStageAwaitRecommendationProfiles) {
			return hotelErrorResponse(requestID, "failed to advance recommendation profile stage")
		}
		profileRequest := hotelNewNestedRequest(requestID, "profile", ndTimestamp, "get_profiles", map[string]any{
			"hotel_ids": hotelIDs,
			"locale":    contextPayload["locale"],
		})
		hotelDispatchNestedRequest(e, request, hotelProfileTargets, profileRequest)
		return hotelBlockedForNestedResponse(requestID)

	case frontendStageAwaitRecommendationProfiles:
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok || !hotelNestedResponsesReady(nestedResponses, requestID, "profile") {
			return hotelBlockedForNestedResponse(requestID)
		}
		profilePayload := hotelNestedResponsePayload(hotelSelectedNestedResponse(nestedResponses, requestID, "profile"))
		profiles := hotelProfilesFromPayload(profilePayload["profiles"])
		e.ClearRequestContext(requestID)
		return hotelAttachParentRequestID(request, map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"result":     hotelGeoJSONResponse(profiles),
		})
	default:
		return hotelErrorResponse(requestID, "unknown frontend recommendation stage: "+stage)
	}
}

func executeFrontendCheckUser(e workflowRuntime, request map[string]any, stage string, ndTimestamp float64) map[string]any {
	requestID := request["request_id"]
	payload := hotelPayload(request)

	switch stage {
	case "":
		if errResponse := validateFrontendUserPayload(requestID, payload); errResponse != nil {
			return errResponse
		}
		if !e.SetRequestContextValue(requestID, frontendStageContextKey, frontendStageAwaitUserCheck) {
			return hotelErrorResponse(requestID, "failed to set frontend user stage")
		}
		userRequest := hotelNewNestedRequest(requestID, "user", ndTimestamp, "check_user", payload)
		hotelDispatchNestedRequest(e, request, hotelUserTargets, userRequest)
		return hotelBlockedForNestedResponse(requestID)

	case frontendStageAwaitUserCheck:
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok || !hotelNestedResponsesReady(nestedResponses, requestID, "user") {
			return hotelBlockedForNestedResponse(requestID)
		}
		userPayload := hotelNestedResponsePayload(hotelSelectedNestedResponse(nestedResponses, requestID, "user"))
		message := "Login successfully!"
		if correct, _ := userPayload["correct"].(bool); !correct {
			message = "Failed. Please check your username and password."
		}
		e.ClearRequestContext(requestID)
		return hotelAttachParentRequestID(request, map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"message":    message,
		})
	default:
		return hotelErrorResponse(requestID, "unknown frontend user stage: "+stage)
	}
}

func executeFrontendReservation(e workflowRuntime, request map[string]any, stage string, ndTimestamp float64) map[string]any {
	requestID := request["request_id"]
	payload := hotelPayload(request)

	switch stage {
	case "":
		if errResponse := validateFrontendReservationPayload(requestID, payload); errResponse != nil {
			return errResponse
		}
		contextPayload := map[string]any{
			"hotel_id":      payload["hotel_id"],
			"in_date":       payload["in_date"],
			"out_date":      payload["out_date"],
			"customer_name": payload["customer_name"],
			"username":      payload["username"],
			"password":      payload["password"],
			"room_number":   payload["room_number"],
			"user_correct":  false,
		}
		if !e.SetRequestContextValue(requestID, frontendPayloadContextKey, contextPayload) {
			return hotelErrorResponse(requestID, "failed to initialize frontend reservation context")
		}
		if !e.SetRequestContextValue(requestID, frontendStageContextKey, frontendStageAwaitReservationUser) {
			return hotelErrorResponse(requestID, "failed to set frontend reservation stage")
		}
		userRequest := hotelNewNestedRequest(requestID, "user", ndTimestamp, "check_user", map[string]any{
			"username": payload["username"],
			"password": payload["password"],
		})
		hotelDispatchNestedRequest(e, request, hotelUserTargets, userRequest)
		return hotelBlockedForNestedResponse(requestID)

	case frontendStageAwaitReservationUser:
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok || !hotelNestedResponsesReady(nestedResponses, requestID, "user") {
			return hotelBlockedForNestedResponse(requestID)
		}
		userPayload := hotelNestedResponsePayload(hotelSelectedNestedResponse(nestedResponses, requestID, "user"))
		contextPayloadAny, _ := e.GetRequestContextValue(requestID, frontendPayloadContextKey)
		contextPayload, _ := contextPayloadAny.(map[string]any)
		contextPayload["user_correct"], _ = userPayload["correct"].(bool)
		if !e.SetRequestContextValue(requestID, frontendPayloadContextKey, contextPayload) {
			return hotelErrorResponse(requestID, "failed to update frontend reservation context")
		}
		if !e.SetRequestContextValue(requestID, frontendStageContextKey, frontendStageAwaitReservationWrite) {
			return hotelErrorResponse(requestID, "failed to advance frontend reservation stage")
		}
		reservationRequest := hotelNewNestedRequest(requestID, "reservation", ndTimestamp, "make_reservation", map[string]any{
			"hotel_id":      contextPayload["hotel_id"],
			"in_date":       contextPayload["in_date"],
			"out_date":      contextPayload["out_date"],
			"customer_name": contextPayload["customer_name"],
			"room_number":   contextPayload["room_number"],
		})
		hotelDispatchNestedRequest(e, request, hotelReservationTargets, reservationRequest)
		return hotelBlockedForNestedResponse(requestID)

	case frontendStageAwaitReservationWrite:
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok || !hotelNestedResponsesReady(nestedResponses, requestID, "reservation") {
			return hotelBlockedForNestedResponse(requestID)
		}
		contextPayloadAny, _ := e.GetRequestContextValue(requestID, frontendPayloadContextKey)
		contextPayload, _ := contextPayloadAny.(map[string]any)
		reservationPayload := hotelNestedResponsePayload(hotelSelectedNestedResponse(nestedResponses, requestID, "reservation"))
		userCorrect, _ := contextPayload["user_correct"].(bool)
		reserved, _ := reservationPayload["reserved"].(bool)

		message := "Reserve successfully!"
		if !userCorrect {
			message = "Failed. Please check your username and password."
		} else if !reserved {
			message = "Failed. Already reserved."
		}

		e.ClearRequestContext(requestID)
		return hotelAttachParentRequestID(request, map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"message":    message,
			"reserved":   reserved,
		})
	default:
		return hotelErrorResponse(requestID, "unknown frontend reservation stage: "+stage)
	}
}

func validateFrontendSearchPayload(requestID any, payload map[string]any) map[string]any {
	if _, ok := hotelPayloadFloat64(payload, "lat"); !ok {
		return hotelErrorResponse(requestID, "missing lat")
	}
	if _, ok := hotelPayloadFloat64(payload, "lon"); !ok {
		return hotelErrorResponse(requestID, "missing lon")
	}
	if hotelPayloadString(payload, "in_date") == "" || hotelPayloadString(payload, "out_date") == "" {
		return hotelErrorResponse(requestID, "missing in_date or out_date")
	}
	if _, ok := hotelMustDateRange(hotelPayloadString(payload, "in_date"), hotelPayloadString(payload, "out_date")); !ok {
		return hotelErrorResponse(requestID, "invalid date range")
	}
	if roomNumber, ok := hotelPayloadInt(payload, "room_number"); ok && roomNumber <= 0 {
		return hotelErrorResponse(requestID, "room_number must be positive")
	}
	return nil
}

func validateFrontendRecommendationPayload(requestID any, payload map[string]any) map[string]any {
	requirement := hotelPayloadString(payload, "require")
	if requirement != "dis" && requirement != "rate" && requirement != "price" {
		return hotelErrorResponse(requestID, "invalid require")
	}
	if _, ok := hotelPayloadFloat64(payload, "lat"); !ok {
		return hotelErrorResponse(requestID, "missing lat")
	}
	if _, ok := hotelPayloadFloat64(payload, "lon"); !ok {
		return hotelErrorResponse(requestID, "missing lon")
	}
	return nil
}

func validateFrontendUserPayload(requestID any, payload map[string]any) map[string]any {
	if hotelPayloadString(payload, "username") == "" || hotelPayloadString(payload, "password") == "" {
		return hotelErrorResponse(requestID, "missing username or password")
	}
	return nil
}

func validateFrontendReservationPayload(requestID any, payload map[string]any) map[string]any {
	if errResponse := validateFrontendUserPayload(requestID, payload); errResponse != nil {
		return errResponse
	}
	if hotelPayloadString(payload, "hotel_id") == "" {
		return hotelErrorResponse(requestID, "missing hotel_id")
	}
	if hotelPayloadString(payload, "customer_name") == "" {
		return hotelErrorResponse(requestID, "missing customer_name")
	}
	if hotelPayloadString(payload, "in_date") == "" || hotelPayloadString(payload, "out_date") == "" {
		return hotelErrorResponse(requestID, "missing in_date or out_date")
	}
	roomNumber, ok := hotelPayloadInt(payload, "room_number")
	if !ok || roomNumber <= 0 {
		return hotelErrorResponse(requestID, "room_number must be positive")
	}
	if _, ok := hotelMustDateRange(hotelPayloadString(payload, "in_date"), hotelPayloadString(payload, "out_date")); !ok {
		return hotelErrorResponse(requestID, "invalid date range")
	}
	return nil
}

func normalizeFrontendSearchPayload(payload map[string]any) map[string]any {
	normalized := copyHotelPayload(payload)
	if _, ok := hotelPayloadInt(normalized, "room_number"); !ok {
		normalized["room_number"] = 1
	}
	if hotelPayloadString(normalized, "locale") == "" {
		normalized["locale"] = "en"
	}
	return normalized
}

func normalizeFrontendRecommendationPayload(payload map[string]any) map[string]any {
	normalized := copyHotelPayload(payload)
	if hotelPayloadString(normalized, "locale") == "" {
		normalized["locale"] = "en"
	}
	return normalized
}

func copyHotelPayload(payload map[string]any) map[string]any {
	duplicated := make(map[string]any, len(payload))
	for key, value := range payload {
		duplicated[key] = value
	}
	return duplicated
}
