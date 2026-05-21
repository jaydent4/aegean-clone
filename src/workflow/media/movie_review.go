package mediaworkflow

import (
	"aegean/common"
)

func ExecuteRequestMovieReview(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	switch op {
	case "upload_movie_review":
		payload := mediaRequestPayload(request)
		movieID := mediaPayloadString(payload, "movie_id")
		reviewID, okReviewID := mediaPayloadInt64(payload, "review_id")
		timestamp, okTimestamp := mediaPayloadInt64(payload, "timestamp")
		if movieID == "" || !okReviewID || !okTimestamp {
			return mediaErrorResponse(requestID, "missing movie review payload")
		}
		key := mediaMovieReviewKey(movieID)
		existing := decodeMediaReviewIndex(e.ReadKV(key))
		maxEntries := common.IntOrDefault(e.GetRunConfig(), "media_max_reviews_per_index", 100)
		updated := prependMediaReviewIndex(existing, MediaReviewIndexEntry{ReviewID: reviewID, Timestamp: timestamp}, maxEntries)
		e.WriteKV(key, encodeMediaReviewIndex(updated))
		return mediaNestedOkResponse(request)
	default:
		return mediaErrorResponse(requestID, "unsupported op: "+op)
	}
}
