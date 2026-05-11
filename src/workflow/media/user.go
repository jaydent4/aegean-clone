package mediaworkflow

import (
	"strconv"
)

const (
	mediaUserStageContextKey = "media_user_stage"
	mediaUserStageAwait      = "await_compose_review"
)

func ExecuteRequestUser(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "upload_user_with_username" {
		return mediaErrorResponse(requestID, "unsupported op: "+op)
	}

	stageAny, _ := e.GetRequestContextValue(requestID, mediaUserStageContextKey)
	stage, _ := stageAny.(string)
	switch stage {
	case "":
		payload := mediaRequestPayload(request)
		username := mediaPayloadString(payload, "username")
		if username == "" {
			return mediaErrorResponse(requestID, "missing username")
		}
		rawUserID := mediaReadKV(e, mediaUserLookupKey(username))
		if rawUserID == "" {
			return mediaErrorResponse(requestID, "user not found")
		}
		userID, err := strconv.ParseInt(rawUserID, 10, 64)
		if err != nil {
			return mediaErrorResponse(requestID, "invalid user id")
		}
		if !e.SetRequestContextValue(requestID, mediaUserStageContextKey, mediaUserStageAwait) {
			return mediaErrorResponse(requestID, "failed to initialize user context")
		}

		reviewRequestID := mediaReviewRequestIDFromPayload(payload, requestID)
		outgoing := mediaNewNestedRequest(requestID, "compose_review", ndTimestamp, "upload_user_id", map[string]any{
			"review_request_id": reviewRequestID,
			"user_id":           userID,
		})
		mediaDispatchNestedRequest(e, request, mediaServiceTargets(e, "compose_review", mediaComposeReviewTargets), outgoing)
		return mediaBlockedForNestedResponse(requestID)
	case mediaUserStageAwait:
		return mediaCompleteAfterComposeResponse(e, request)
	default:
		return mediaErrorResponse(requestID, "unknown user stage: "+stage)
	}
}
