package mediaworkflow

const (
	mediaMovieIDStageContextKey = "media_movie_id_stage"
	mediaMovieIDStageAwait      = "await_compose_and_rating"
)

var mediaRatingTargets = []string{"node10", "node11", "node12"}

func ExecuteRequestMovieID(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "upload_movie_id" {
		return mediaErrorResponse(requestID, "unsupported op: "+op)
	}

	stageAny, _ := e.GetRequestContextValue(requestID, mediaMovieIDStageContextKey)
	stage, _ := stageAny.(string)
	switch stage {
	case "":
		payload := mediaRequestPayload(request)
		title := mediaPayloadString(payload, "title")
		if title == "" {
			return mediaErrorResponse(requestID, "missing title")
		}
		rating, ok := mediaPayloadInt(payload, "rating")
		if !ok {
			return mediaErrorResponse(requestID, "missing rating")
		}
		movieID := mediaReadKV(e, mediaMovieLookupKey(title))
		if movieID == "" {
			return mediaErrorResponse(requestID, "movie not found")
		}
		if !e.SetRequestContextValue(requestID, mediaMovieIDStageContextKey, mediaMovieIDStageAwait) {
			return mediaErrorResponse(requestID, "failed to initialize movie id context")
		}

		reviewRequestID := mediaReviewRequestIDFromPayload(payload, requestID)
		composeRequest := mediaNewNestedRequest(requestID, "compose_review", ndTimestamp, "upload_movie_id", map[string]any{
			"review_request_id": reviewRequestID,
			"movie_id":          movieID,
		})
		mediaDispatchNestedRequest(e, request, mediaServiceTargets(e, "compose_review", mediaComposeReviewTargets), composeRequest)

		ratingRequest := mediaNewNestedRequest(requestID, "rating", ndTimestamp, "upload_rating", map[string]any{
			"review_request_id": reviewRequestID,
			"movie_id":          movieID,
			"rating":            rating,
		})
		mediaDispatchNestedRequest(e, request, mediaServiceTargets(e, "rating", mediaRatingTargets), ratingRequest)
		return mediaBlockedForNestedResponse(requestID)
	case mediaMovieIDStageAwait:
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok || !mediaNestedResponsesReady(nestedResponses, requestID, "compose_review", "rating") {
			return mediaBlockedForNestedResponse(requestID)
		}
		e.ClearRequestContext(requestID)
		return mediaNestedOkResponse(request)
	default:
		return mediaErrorResponse(requestID, "unknown movie id stage: "+stage)
	}
}
