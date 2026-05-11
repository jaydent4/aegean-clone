package mediaworkflow

import (
	"aegean/common"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

const mediaReviewCustomEpochMillis int64 = 1514764800000
const mediaJSONSafePositiveInt64Mask uint64 = (1 << 53) - 1

func mediaUserLookupKey(username string) string {
	return "media:user:username:" + username
}

func mediaMovieLookupKey(title string) string {
	return "media:movie:title:" + title
}

func mediaRatingSumKey(movieID string) string {
	return "media:rating:" + movieID + ":sum"
}

func mediaRatingCountKey(movieID string) string {
	return "media:rating:" + movieID + ":count"
}

func mediaComposeComponentKey(reviewRequestID string, component string) string {
	return "media:compose:" + reviewRequestID + ":" + component
}

func mediaComposeUploadedKey(reviewRequestID string) string {
	return "media:compose:" + reviewRequestID + ":uploaded"
}

func mediaReviewKey(reviewID int64) string {
	return "media:review:" + strconv.FormatInt(reviewID, 10)
}

func mediaUserReviewKey(userID int64) string {
	return "media:user_review:" + strconv.FormatInt(userID, 10)
}

func mediaMovieReviewKey(movieID string) string {
	return "media:movie_review:" + movieID
}

func mediaNestedRequestID(parentRequestID any, serviceName string) string {
	return fmt.Sprintf("%v/%s", parentRequestID, serviceName)
}

func mediaReviewRequestIDFromPayload(payload map[string]any, fallback any) string {
	if reviewRequestID := mediaPayloadString(payload, "review_request_id"); reviewRequestID != "" {
		return reviewRequestID
	}
	return fmt.Sprintf("%v", fallback)
}

func mediaRequestPayload(request map[string]any) map[string]any {
	opPayload, _ := request["op_payload"].(map[string]any)
	if opPayload == nil {
		return map[string]any{}
	}
	return opPayload
}

func mediaPayloadString(payload map[string]any, key string) string {
	value, _ := payload[key].(string)
	return value
}

func mediaPayloadInt(payload map[string]any, key string) (int, bool) {
	switch typed := payload[key].(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	case string:
		value, err := strconv.Atoi(typed)
		return value, err == nil
	default:
		return 0, false
	}
}

func mediaPayloadInt64(payload map[string]any, key string) (int64, bool) {
	switch typed := payload[key].(type) {
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case float64:
		return int64(typed), true
	case string:
		value, err := strconv.ParseInt(typed, 10, 64)
		return value, err == nil
	default:
		return 0, false
	}
}

func mediaNestedTargets(runConfig map[string]any, replicas []string) []string {
	if len(replicas) == 0 {
		return nil
	}
	if !common.BoolOrDefault(runConfig, "media_nested_send_all_replicas", false) {
		return []string{replicas[0]}
	}
	return append([]string{}, replicas...)
}

func mediaServiceTargets(e workflowRuntime, serviceName string, fallback []string) []string {
	return common.ServiceNodesOrDefault(e.GetRunConfig(), serviceName, fallback)
}

func mediaDispatchNestedRequest(e workflowRuntime, sourceRequest map[string]any, targets []string, outgoing map[string]any) {
	selectedTargets := mediaNestedTargets(e.GetRunConfig(), targets)
	if common.BoolOrDefault(e.GetRunConfig(), "media_nested_use_eo", false) {
		e.DispatchNestedRequestEO(sourceRequest, selectedTargets, outgoing)
		return
	}
	e.DispatchNestedRequestDirect(sourceRequest, selectedTargets, outgoing)
}

func mediaNewNestedRequest(parentRequestID any, childName string, ndTimestamp float64, op string, opPayload map[string]any) map[string]any {
	return map[string]any{
		"type":              "request",
		"request_id":        mediaNestedRequestID(parentRequestID, childName),
		"parent_request_id": parentRequestID,
		"timestamp":         ndTimestamp,
		"op":                op,
		"op_payload":        opPayload,
	}
}

func mediaNestedResponsesReady(nestedResponses []map[string]any, parentRequestID any, serviceNames ...string) bool {
	for _, serviceName := range serviceNames {
		if mediaSelectedNestedResponse(nestedResponses, parentRequestID, serviceName) == nil {
			return false
		}
	}
	return true
}

func mediaSelectedNestedResponse(nestedResponses []map[string]any, parentRequestID any, serviceName string) map[string]any {
	expectedRequestID := mediaNestedRequestID(parentRequestID, serviceName)
	for _, nested := range nestedResponses {
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
			continue
		}
		requestID, _ := nested["request_id"].(string)
		if requestID == expectedRequestID {
			return nested
		}
	}
	return nil
}

func mediaNestedResponsePayload(nested map[string]any) map[string]any {
	response, _ := nested["response"].(map[string]any)
	if response == nil {
		return map[string]any{}
	}
	return response
}

func mediaBlockedForNestedResponse(requestID any) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "blocked_for_nested_response",
	}
}

func mediaErrorResponse(requestID any, message string) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "error",
		"error":      message,
	}
}

func mediaNestedOkResponse(request map[string]any) map[string]any {
	return mediaAttachParentRequestID(request, map[string]any{
		"request_id": request["request_id"],
		"status":     "ok",
	})
}

func mediaAttachParentRequestID(request map[string]any, response map[string]any) map[string]any {
	if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
		response["parent_request_id"] = parentRequestID
	}
	return response
}

func mediaTimestamp(ndTimestamp float64) int64 {
	if ndTimestamp == 0 {
		return 0
	}
	return int64(ndTimestamp * 1000)
}

func deterministicMediaReviewID(reviewRequestID string, ndSeed int64, ndTimestamp float64) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(reviewRequestID))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(strconv.FormatInt(ndSeed, 10)))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(strconv.FormatInt(mediaTimestamp(ndTimestamp)-mediaReviewCustomEpochMillis, 10)))
	value := int64(h.Sum64() & mediaJSONSafePositiveInt64Mask)
	if value == 0 {
		return 1
	}
	return value
}

func encodeMediaReview(review MediaReview) string {
	bytes, err := json.Marshal(review)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func decodeMediaReview(raw string) (MediaReview, bool) {
	var review MediaReview
	if raw == "" {
		return review, false
	}
	if err := json.Unmarshal([]byte(raw), &review); err != nil {
		return MediaReview{}, false
	}
	return review, true
}

func mediaReviewToPayload(review MediaReview) map[string]any {
	return map[string]any{
		"review_id": review.ReviewID,
		"user_id":   review.UserID,
		"req_id":    review.ReqID,
		"text":      review.Text,
		"movie_id":  review.MovieID,
		"rating":    review.Rating,
		"timestamp": review.Timestamp,
	}
}

func mediaReviewFromPayload(value any) (MediaReview, bool) {
	payload, _ := value.(map[string]any)
	if payload == nil {
		return MediaReview{}, false
	}
	reviewID, okReviewID := mediaPayloadInt64(payload, "review_id")
	userID, okUserID := mediaPayloadInt64(payload, "user_id")
	rating, okRating := mediaPayloadInt(payload, "rating")
	timestamp, okTimestamp := mediaPayloadInt64(payload, "timestamp")
	movieID := mediaPayloadString(payload, "movie_id")
	text := mediaPayloadString(payload, "text")
	reqID := mediaPayloadString(payload, "req_id")
	if !okReviewID || !okUserID || !okRating || !okTimestamp || movieID == "" {
		return MediaReview{}, false
	}
	return MediaReview{
		ReviewID:  reviewID,
		UserID:    userID,
		ReqID:     reqID,
		Text:      text,
		MovieID:   movieID,
		Rating:    rating,
		Timestamp: timestamp,
	}, true
}

func encodeMediaReviewIndex(entries []MediaReviewIndexEntry) string {
	bytes, err := json.Marshal(entries)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func decodeMediaReviewIndex(raw string) []MediaReviewIndexEntry {
	if raw == "" {
		return []MediaReviewIndexEntry{}
	}
	var entries []MediaReviewIndexEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return []MediaReviewIndexEntry{}
	}
	return entries
}

func prependMediaReviewIndex(existing []MediaReviewIndexEntry, entry MediaReviewIndexEntry, maxEntries int) []MediaReviewIndexEntry {
	filtered := make([]MediaReviewIndexEntry, 0, len(existing)+1)
	filtered = append(filtered, entry)
	for _, current := range existing {
		if current.ReviewID == entry.ReviewID {
			continue
		}
		filtered = append(filtered, current)
	}
	if maxEntries > 0 && len(filtered) > maxEntries {
		return filtered[:maxEntries]
	}
	return filtered
}

func mediaInt64String(value int64) string {
	return strconv.FormatInt(value, 10)
}

func mediaReadInt64(e interface{ ReadKV(string) string }, key string) int64 {
	raw := e.ReadKV(key)
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0
	}
	return value
}

func mediaSeedUsername(userIdx int) string {
	return fmt.Sprintf("username_%d", userIdx)
}

func mediaSeedMovieTitle(movieIdx int) string {
	return fmt.Sprintf("movie-title-%d", movieIdx)
}

func mediaSeedMovieID(movieIdx int) string {
	return fmt.Sprintf("movie-%d", movieIdx)
}
