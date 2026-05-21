package mediaworkflow

const (
	mediaComposeReviewStageContextKey  = "media_compose_review_stage"
	mediaComposeReviewReviewContextKey = "media_compose_review_review"
	mediaComposeReviewStageAwait       = "await_storage_writes"

	mediaComponentReviewID = "review_id"
	mediaComponentMovieID  = "movie_id"
	mediaComponentUserID   = "user_id"
	mediaComponentText     = "text"
	mediaComponentRating   = "rating"
)

var (
	mediaComposeReviewTargets = []string{"node13", "node14", "node15"}
	mediaReviewStorageTargets = []string{"node16", "node17", "node18"}
	mediaUserReviewTargets    = []string{"node19", "node20", "node21"}
	mediaMovieReviewTargets   = []string{"node22", "node23", "node24"}
)

func ExecuteRequestComposeReview(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed

	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, mediaComposeReviewStageContextKey)
	stage, _ := stageAny.(string)
	switch stage {
	case "":
		component, value, reviewRequestID, responseExtra, errResponse := mediaExtractComposeComponent(request)
		if errResponse != nil {
			return errResponse
		}
		e.WriteKV(mediaComposeComponentKey(reviewRequestID, component), value)

		if e.ReadKV(mediaComposeUploadedKey(reviewRequestID)) != "" {
			response := mediaNestedOkResponse(request)
			for key, value := range responseExtra {
				response[key] = value
			}
			return response
		}

		review, ok := mediaTryBuildReview(e, reviewRequestID, ndTimestamp)
		if !ok {
			response := mediaNestedOkResponse(request)
			for key, value := range responseExtra {
				response[key] = value
			}
			return response
		}

		e.WriteKV(mediaComposeUploadedKey(reviewRequestID), mediaInt64String(review.ReviewID))
		if !e.SetRequestContextValue(requestID, mediaComposeReviewStageContextKey, mediaComposeReviewStageAwait) {
			return mediaErrorResponse(requestID, "failed to initialize compose review context")
		}
		if !e.SetRequestContextValue(requestID, mediaComposeReviewReviewContextKey, review) {
			return mediaErrorResponse(requestID, "failed to store compose review context")
		}
		dispatchComposedReviewWrites(e, request, review, ndTimestamp)
		return mediaBlockedForNestedResponse(requestID)
	case mediaComposeReviewStageAwait:
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok || !mediaNestedResponsesReady(nestedResponses, requestID, "review_storage", "user_review", "movie_review") {
			return mediaBlockedForNestedResponse(requestID)
		}
		reviewAny, _ := e.GetRequestContextValue(requestID, mediaComposeReviewReviewContextKey)
		review, _ := reviewAny.(MediaReview)
		e.ClearRequestContext(requestID)
		response := mediaNestedOkResponse(request)
		if review.ReviewID != 0 {
			response["review_id"] = review.ReviewID
		}
		return response
	default:
		return mediaErrorResponse(requestID, "unknown compose review stage: "+stage)
	}
}

func mediaCompleteAfterComposeResponse(e workflowRuntime, request map[string]any) map[string]any {
	requestID := request["request_id"]
	nestedResponses, ok := e.GetNestedResponses(requestID)
	if !ok || !mediaNestedResponsesReady(nestedResponses, requestID, "compose_review") {
		return mediaBlockedForNestedResponse(requestID)
	}
	response := mediaNestedOkResponse(request)
	if composeNested := mediaSelectedNestedResponse(nestedResponses, requestID, "compose_review"); composeNested != nil {
		composePayload := mediaNestedResponsePayload(composeNested)
		if reviewID, ok := mediaPayloadInt64(composePayload, "review_id"); ok {
			response["review_id"] = reviewID
		}
	}
	e.ClearRequestContext(requestID)
	return response
}

func mediaExtractComposeComponent(request map[string]any) (string, string, string, map[string]any, map[string]any) {
	requestID := request["request_id"]
	op, _ := request["op"].(string)
	payload := mediaRequestPayload(request)
	reviewRequestID := mediaReviewRequestIDFromPayload(payload, requestID)
	if reviewRequestID == "" {
		return "", "", "", nil, mediaErrorResponse(requestID, "missing review_request_id")
	}

	switch op {
	case "upload_unique_id":
		reviewID, ok := mediaPayloadInt64(payload, "review_id")
		if !ok {
			return "", "", "", nil, mediaErrorResponse(requestID, "missing review_id")
		}
		return mediaComponentReviewID, mediaInt64String(reviewID), reviewRequestID, map[string]any{"review_id": reviewID}, nil
	case "upload_movie_id":
		movieID := mediaPayloadString(payload, "movie_id")
		if movieID == "" {
			return "", "", "", nil, mediaErrorResponse(requestID, "missing movie_id")
		}
		return mediaComponentMovieID, movieID, reviewRequestID, nil, nil
	case "upload_user_id":
		userID, ok := mediaPayloadInt64(payload, "user_id")
		if !ok {
			return "", "", "", nil, mediaErrorResponse(requestID, "missing user_id")
		}
		return mediaComponentUserID, mediaInt64String(userID), reviewRequestID, nil, nil
	case "upload_text":
		text := mediaPayloadString(payload, "text")
		if text == "" {
			return "", "", "", nil, mediaErrorResponse(requestID, "missing text")
		}
		return mediaComponentText, text, reviewRequestID, nil, nil
	case "upload_rating":
		rating, ok := mediaPayloadInt(payload, "rating")
		if !ok {
			return "", "", "", nil, mediaErrorResponse(requestID, "missing rating")
		}
		return mediaComponentRating, mediaInt64String(int64(rating)), reviewRequestID, nil, nil
	default:
		return "", "", "", nil, mediaErrorResponse(requestID, "unsupported op: "+op)
	}
}

func mediaTryBuildReview(e workflowRuntime, reviewRequestID string, ndTimestamp float64) (MediaReview, bool) {
	rawReviewID := e.ReadKV(mediaComposeComponentKey(reviewRequestID, mediaComponentReviewID))
	rawMovieID := e.ReadKV(mediaComposeComponentKey(reviewRequestID, mediaComponentMovieID))
	rawUserID := e.ReadKV(mediaComposeComponentKey(reviewRequestID, mediaComponentUserID))
	rawText := e.ReadKV(mediaComposeComponentKey(reviewRequestID, mediaComponentText))
	rawRating := e.ReadKV(mediaComposeComponentKey(reviewRequestID, mediaComponentRating))
	if rawReviewID == "" || rawMovieID == "" || rawUserID == "" || rawText == "" || rawRating == "" {
		return MediaReview{}, false
	}
	reviewID, okReviewID := mediaPayloadInt64(map[string]any{"review_id": rawReviewID}, "review_id")
	userID, okUserID := mediaPayloadInt64(map[string]any{"user_id": rawUserID}, "user_id")
	rating, okRating := mediaPayloadInt(map[string]any{"rating": rawRating}, "rating")
	if !okReviewID || !okUserID || !okRating {
		return MediaReview{}, false
	}
	return MediaReview{
		ReviewID:  reviewID,
		UserID:    userID,
		ReqID:     reviewRequestID,
		Text:      rawText,
		MovieID:   rawMovieID,
		Rating:    rating,
		Timestamp: mediaTimestamp(ndTimestamp),
	}, true
}

func dispatchComposedReviewWrites(e workflowRuntime, request map[string]any, review MediaReview, ndTimestamp float64) {
	requestID := request["request_id"]
	reviewPayload := mediaReviewToPayload(review)

	reviewStorageRequest := mediaNewNestedRequest(requestID, "review_storage", ndTimestamp, "store_review", map[string]any{
		"review": reviewPayload,
	})
	mediaDispatchNestedRequest(e, request, mediaServiceTargets(e, "review_storage", mediaReviewStorageTargets), reviewStorageRequest)

	userReviewRequest := mediaNewNestedRequest(requestID, "user_review", ndTimestamp, "upload_user_review", map[string]any{
		"user_id":   review.UserID,
		"review_id": review.ReviewID,
		"timestamp": review.Timestamp,
	})
	mediaDispatchNestedRequest(e, request, mediaServiceTargets(e, "user_review", mediaUserReviewTargets), userReviewRequest)

	movieReviewRequest := mediaNewNestedRequest(requestID, "movie_review", ndTimestamp, "upload_movie_review", map[string]any{
		"movie_id":  review.MovieID,
		"review_id": review.ReviewID,
		"timestamp": review.Timestamp,
	})
	mediaDispatchNestedRequest(e, request, mediaServiceTargets(e, "movie_review", mediaMovieReviewTargets), movieReviewRequest)
}
