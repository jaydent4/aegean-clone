package mediaworkflow

import (
	"strconv"
)

const (
	mediaRatingStageContextKey = "media_rating_stage"
	mediaRatingStageAwait      = "await_compose_review"
)

func ExecuteRequestRating(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "upload_rating" {
		return mediaErrorResponse(requestID, "unsupported op: "+op)
	}

	stageAny, _ := e.GetRequestContextValue(requestID, mediaRatingStageContextKey)
	stage, _ := stageAny.(string)
	switch stage {
	case "":
		payload := mediaRequestPayload(request)
		movieID := mediaPayloadString(payload, "movie_id")
		if movieID == "" {
			return mediaErrorResponse(requestID, "missing movie_id")
		}
		rating, ok := mediaPayloadInt(payload, "rating")
		if !ok {
			return mediaErrorResponse(requestID, "missing rating")
		}

		currentSum, _ := strconv.ParseInt(e.ReadKV(mediaRatingSumKey(movieID)), 10, 64)
		currentCount, _ := strconv.ParseInt(e.ReadKV(mediaRatingCountKey(movieID)), 10, 64)
		e.WriteKV(mediaRatingSumKey(movieID), mediaInt64String(currentSum+int64(rating)))
		e.WriteKV(mediaRatingCountKey(movieID), mediaInt64String(currentCount+1))

		if !e.SetRequestContextValue(requestID, mediaRatingStageContextKey, mediaRatingStageAwait) {
			return mediaErrorResponse(requestID, "failed to initialize rating context")
		}
		reviewRequestID := mediaReviewRequestIDFromPayload(payload, requestID)
		outgoing := mediaNewNestedRequest(requestID, "compose_review", ndTimestamp, "upload_rating", map[string]any{
			"review_request_id": reviewRequestID,
			"rating":            rating,
		})
		mediaDispatchNestedRequest(e, request, mediaServiceTargets(e, "compose_review", mediaComposeReviewTargets), outgoing)
		return mediaBlockedForNestedResponse(requestID)
	case mediaRatingStageAwait:
		return mediaCompleteAfterComposeResponse(e, request)
	default:
		return mediaErrorResponse(requestID, "unknown rating stage: "+stage)
	}
}
