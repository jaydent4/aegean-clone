package mediaworkflow

func ExecuteRequestReviewStorage(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	switch op {
	case "store_review":
		payload := mediaRequestPayload(request)
		review, ok := mediaReviewFromPayload(payload["review"])
		if !ok {
			return mediaErrorResponse(requestID, "missing review")
		}
		e.WriteKV(mediaReviewKey(review.ReviewID), encodeMediaReview(review))
		return mediaNestedOkResponse(request)
	default:
		return mediaErrorResponse(requestID, "unsupported op: "+op)
	}
}
