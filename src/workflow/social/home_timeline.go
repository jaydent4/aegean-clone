package socialworkflow

const (
	homeTimelineStageContextKey   = "social_home_timeline_stage"
	homeTimelinePayloadContextKey = "social_home_timeline_payload"

	homeTimelineStageAwaitFollowers = "await_followers"
	homeTimelineStageAwaitPosts     = "await_posts"
)

var socialGraphTargets = []string{"node11", "node12", "node13"}
var homeTimelinePostStorageTargets = []string{"node4", "node5", "node6"}

// HomeTimeline has two independent responsibilities:
// write_home_timeline fans a post out to followers, while read_home_timeline
// resolves stored post IDs into full post objects through post_storage.
func ExecuteRequestHomeTimeline(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	stageAny, _ := e.GetRequestContextValue(requestID, homeTimelineStageContextKey)
	stage, _ := stageAny.(string)

	switch op {
	case "write_home_timeline":
		// write_home_timeline: call social_graph to resolve followers, then append
		// the new post IDs into each follower's local home_timeline state.
		switch stage {
		case "":
			// Stage 1: resolve the creator's followers before mutating any follower
			// home timeline entries.
			userID := commonPayloadString(request, "user_id")
			if userID == "" {
				return errorResponse(requestID, "missing user_id")
			}
			postIDs := commonPayloadStringSlice(request, "post_ids")
			payload := map[string]any{
				"user_id":  userID,
				"post_ids": postIDs,
			}
			if !e.SetRequestContextValue(requestID, homeTimelinePayloadContextKey, payload) {
				return errorResponse(requestID, "failed to initialize home timeline context")
			}
			if !e.SetRequestContextValue(requestID, homeTimelineStageContextKey, homeTimelineStageAwaitFollowers) {
				return errorResponse(requestID, "failed to set home timeline stage")
			}

			outgoing := map[string]any{
				"type":              "request",
				"request_id":        nestedRequestID(requestID, "social_graph"),
				"parent_request_id": requestID,
				"timestamp":         ndTimestamp,
				"op":                "get_followers",
				"op_payload": map[string]any{
					"user_id": userID,
				},
			}
			socialDispatchNestedRequest(e, request, socialServiceTargets(e, "social_graph", socialGraphTargets), outgoing)
			return blockedForNestedResponse(requestID)

		case homeTimelineStageAwaitFollowers:
			// Stage 2: apply the fanout locally once the social_graph child returns.
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
				if childRequestID == nestedRequestID(requestID, "social_graph") {
					selected = nested
					break
				}
			}
			if selected == nil {
				return blockedForNestedResponse(requestID)
			}

			payloadAny, ok := e.GetRequestContextValue(requestID, homeTimelinePayloadContextKey)
			if !ok {
				return errorResponse(requestID, "missing home timeline payload")
			}
			payload, _ := payloadAny.(map[string]any)
			userID, _ := payload["user_id"].(string)
			postIDs := commonPayloadStringSlice(map[string]any{"op_payload": payload}, "post_ids")

			nestedResponse, _ := selected["response"].(map[string]any)
			followers := extractStringSlice(nestedResponse["followers"])
			for _, follower := range followers {
				existing := decodeStringSlice(socialReadKV(e, homeTimelineKey(follower)))
				socialWriteKV(e, homeTimelineKey(follower), encodeStringSlice(appendTimelineEntries(existing, postIDs, 10)))
			}
			e.ClearRequestContext(requestID)
			response := map[string]any{
				"request_id": requestID,
				"status":     "ok",
				"user_id":    userID,
				"followers":  followers,
			}
			if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
				response["parent_request_id"] = parentRequestID
			}
			return response

		default:
			return errorResponse(requestID, "unknown home timeline stage: "+stage)
		}
	case "read_home_timeline", "ro_read_home_timeline":
		// read_home_timeline: read stored home-timeline post IDs locally, then call
		// post_storage to expand those IDs into full post objects for the response.
		switch stage {
		case "":
			// Stage 1: read the user's timeline index locally, then fetch the actual
			// posts from post_storage.
			userID := commonPayloadString(request, "user_id")
			if userID == "" {
				return errorResponse(requestID, "missing user_id")
			}
			postIDs := decodeStringSlice(socialReadKV(e, homeTimelineKey(userID)))
			payload := map[string]any{
				"user_id":  userID,
				"post_ids": postIDs,
			}
			if !e.SetRequestContextValue(requestID, homeTimelinePayloadContextKey, payload) {
				return errorResponse(requestID, "failed to initialize home timeline read context")
			}
			if !e.SetRequestContextValue(requestID, homeTimelineStageContextKey, homeTimelineStageAwaitPosts) {
				return errorResponse(requestID, "failed to set home timeline read stage")
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
			socialDispatchNestedRequest(e, request, socialServiceTargets(e, "post_storage", homeTimelinePostStorageTargets), outgoing)
			return blockedForNestedResponse(requestID)

		case homeTimelineStageAwaitPosts:
			// Stage 2: return the materialized posts once post_storage responds.
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

			payloadAny, ok := e.GetRequestContextValue(requestID, homeTimelinePayloadContextKey)
			if !ok {
				return errorResponse(requestID, "missing home timeline read payload")
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
			return errorResponse(requestID, "unknown home timeline read stage: "+stage)
		}
	default:
		return errorResponse(requestID, "unsupported op: "+op)
	}
}
