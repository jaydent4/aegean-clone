package mediaworkflow

const (
	mediaTextStageContextKey = "media_text_stage"
	mediaTextStageAwait      = "await_compose_review"
)

func ExecuteRequestText(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	if op != "upload_text" {
		return mediaErrorResponse(requestID, "unsupported op: "+op)
	}

	stageAny, _ := e.GetRequestContextValue(requestID, mediaTextStageContextKey)
	stage, _ := stageAny.(string)
	switch stage {
	case "":
		payload := mediaRequestPayload(request)
		text := mediaPayloadString(payload, "text")
		if text == "" {
			return mediaErrorResponse(requestID, "missing text")
		}
		if !e.SetRequestContextValue(requestID, mediaTextStageContextKey, mediaTextStageAwait) {
			return mediaErrorResponse(requestID, "failed to initialize text context")
		}
		reviewRequestID := mediaReviewRequestIDFromPayload(payload, requestID)
		outgoing := mediaNewNestedRequest(requestID, "compose_review", ndTimestamp, "upload_text", map[string]any{
			"review_request_id": reviewRequestID,
			"text":              text,
		})
		mediaDispatchNestedRequest(e, request, mediaServiceTargets(e, "compose_review", mediaComposeReviewTargets), outgoing)
		return mediaBlockedForNestedResponse(requestID)
	case mediaTextStageAwait:
		return mediaCompleteAfterComposeResponse(e, request)
	default:
		return mediaErrorResponse(requestID, "unknown text stage: "+stage)
	}
}
