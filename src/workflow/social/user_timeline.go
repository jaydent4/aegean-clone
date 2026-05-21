package socialworkflow

const (
	userTimelineStageContextKey   = "social_user_timeline_stage"
	userTimelinePayloadContextKey = "social_user_timeline_payload"

	userTimelineStageAwaitPosts = "await_posts"
)

var userTimelinePostStorageTargets = []string{"node4", "node5", "node6"}

// UserTimeline stores each author's own recent post IDs and resolves them to
// full posts on read via post_storage.
func ExecuteRequestUserTimeline(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	stageAny, _ := e.GetRequestContextValue(requestID, userTimelineStageContextKey)
	stage, _ := stageAny.(string)

	switch op {
	case "write_user_timeline":
		// write_user_timeline: append new post IDs into the author's local
		// user_timeline state. This op does not call other services.
		// Append the newly created post ID to the author's own timeline index.
		userID := commonPayloadString(request, "user_id")
		if userID == "" {
			return errorResponse(requestID, "missing user_id")
		}
		postIDs := commonPayloadStringSlice(request, "post_ids")
		existing := decodeStringSlice(e.ReadKV(userTimelineKey(userID)))
		e.WriteKV(userTimelineKey(userID), encodeStringSlice(appendTimelineEntries(existing, postIDs, 10)))
		return nestedOkResponse(request)
	case "read_user_timeline", "ro_read_user_timeline":
		// read_user_timeline: read the author's stored post IDs locally, then call
		// post_storage to materialize the final post list.
		switch stage {
		case "":
			// Stage 1: load the stored post IDs for this user and delegate post
			// materialization to post_storage.
			userID := commonPayloadString(request, "user_id")
			if userID == "" {
				return errorResponse(requestID, "missing user_id")
			}
			postIDs := decodeStringSlice(e.ReadKV(userTimelineKey(userID)))
			payload := map[string]any{
				"user_id":  userID,
				"post_ids": postIDs,
			}
			if !e.SetRequestContextValue(requestID, userTimelinePayloadContextKey, payload) {
				return errorResponse(requestID, "failed to initialize user timeline context")
			}
			if !e.SetRequestContextValue(requestID, userTimelineStageContextKey, userTimelineStageAwaitPosts) {
				return errorResponse(requestID, "failed to set user timeline stage")
			}

			outgoing := map[string]any{
				"type":              "request",
				"request_id":        nestedRequestID(requestID, "post_storage"),
				"parent_request_id": requestID,
				"timestamp":         ndTimestamp,
				"op":                "ro_read_posts",
				"op_payload": map[string]any{
					"post_ids": postIDs,
				},
			}
			socialDispatchNestedRequest(e, request, socialServiceTargets(e, "post_storage", userTimelinePostStorageTargets), outgoing)
			return blockedForNestedResponse(requestID)

		case userTimelineStageAwaitPosts:
			// Stage 2: return the fully expanded post list from post_storage.
			nestedResponses, ok := e.GetNestedResponses(requestID)
			if !ok || len(nestedResponses) == 0 {
				return blockedForNestedResponse(requestID)
			}
			var selected map[string]any
			for _, nested := range nestedResponses {
				if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
					continue
				}
				childRequestID, _ := nested["request_id"].(string)
				if childRequestID == nestedRequestID(requestID, "post_storage") {
					selected = nested
					break
				}
			}
			if selected == nil {
				return blockedForNestedResponse(requestID)
			}

			payloadAny, ok := e.GetRequestContextValue(requestID, userTimelinePayloadContextKey)
			if !ok {
				return errorResponse(requestID, "missing user timeline payload")
			}
			payload, _ := payloadAny.(map[string]any)
			userID, _ := payload["user_id"].(string)

			nestedResponse, _ := selected["response"].(map[string]any)
			posts, _ := nestedResponse["posts"].([]any)

			e.ClearRequestContext(requestID)
			return attachParentRequestID(request, map[string]any{
				"request_id": requestID,
				"status":     "ok",
				"user_id":    userID,
				"posts":      posts,
			})

		default:
			return errorResponse(requestID, "unknown user timeline stage: "+stage)
		}
	default:
		return errorResponse(requestID, "unsupported op: "+op)
	}
}
