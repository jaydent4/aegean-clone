package socialworkflow

const (
	composeStageContextKey   = "social_compose_post_stage"
	composePayloadContextKey = "social_compose_post_payload"

	composeStageAwaitNested = "await_nested"
)

var composeNestedTargets = map[string][]string{
	"post_storage":  {"node2", "node3", "node4"},
	"user_timeline": {"node5", "node6", "node7"},
	"home_timeline": {"node8", "node9", "node10"},
}

// ComposePost is the top-level write workflow:
// store the post body, append the author's user timeline, and fan the post out
// into followers' home timelines.
func ExecuteRequestComposePost(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed

	requestID := request["request_id"]
	stageAny, _ := e.GetRequestContextValue(requestID, composeStageContextKey)
	stage, _ := stageAny.(string)

	switch stage {
	case "":
		// compose_post: validate input, derive the post payload, and fan out to
		// post_storage, user_timeline, and home_timeline. compose_post itself does
		// not write KV state; it orchestrates the downstream writes.
		if commonResponse := validateComposeRequest(request); commonResponse != nil {
			return commonResponse
		}

		// Stage 1: materialize the post once, then launch all downstream writes.
		post := Post{
			PostID:    deterministicPostID(request),
			Timestamp: deterministicTimestamp(ndTimestamp),
			Text:      commonPayloadString(request, "text"),
			CreatorID: commonPayloadString(request, "creator_id"),
		}
		if !e.SetRequestContextValue(requestID, composePayloadContextKey, post) {
			return map[string]any{
				"request_id": requestID,
				"status":     "error",
				"error":      "failed to initialize compose context",
			}
		}
		if !e.SetRequestContextValue(requestID, composeStageContextKey, composeStageAwaitNested) {
			return map[string]any{
				"request_id": requestID,
				"status":     "error",
				"error":      "failed to set compose stage",
			}
		}

		dispatchComposeNestedRequests(e, request, post, ndTimestamp)
		return blockedForNestedResponse(requestID)

	case composeStageAwaitNested:
		// compose_post: wait for all three downstream services to finish, then
		// return the final post_id to the client.
		// Stage 2: wait until post_storage, user_timeline, and home_timeline have
		// all acknowledged the compose fanout.
		nestedResponses, ok := e.GetNestedResponses(requestID)
		if !ok {
			return blockedForNestedResponse(requestID)
		}
		if !composeNestedResponsesReady(nestedResponses) {
			return blockedForNestedResponse(requestID)
		}

		e.ClearRequestContext(requestID)
		return map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"post_id":    deterministicPostID(request),
		}

	default:
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "unknown compose stage: " + stage,
		}
	}
}

func validateComposeRequest(request map[string]any) map[string]any {
	requestID := request["request_id"]
	if commonPayloadString(request, "creator_id") == "" {
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "missing creator_id",
		}
	}
	if commonPayloadString(request, "text") == "" {
		return map[string]any{
			"request_id": requestID,
			"status":     "error",
			"error":      "missing text",
		}
	}
	return nil
}

func dispatchComposeNestedRequests(e workflowRuntime, request map[string]any, post Post, ndTimestamp float64) {
	parentRequestID := request["request_id"]
	postIDs := []string{post.PostID}
	postStorageTargets := socialServiceTargets(e, "post_storage", composeNestedTargets["post_storage"])
	userTimelineTargets := socialServiceTargets(e, "user_timeline", composeNestedTargets["user_timeline"])
	homeTimelineTargets := socialServiceTargets(e, "home_timeline", composeNestedTargets["home_timeline"])

	// Each child gets its own request_id plus the shared parent_request_id so the
	// parent compose workflow can correlate the three completions.
	outgoingByTarget := map[string]map[string]any{
		postStorageTargets[0]: {
			"type":              "request",
			"request_id":        nestedRequestID(parentRequestID, "post_storage"),
			"parent_request_id": parentRequestID,
			"timestamp":         ndTimestamp,
			"op":                "store_post",
			"op_payload": map[string]any{
				"post_id":    post.PostID,
				"timestamp":  post.Timestamp,
				"creator_id": post.CreatorID,
				"text":       post.Text,
			},
		},
		userTimelineTargets[0]: {
			"type":              "request",
			"request_id":        nestedRequestID(parentRequestID, "user_timeline"),
			"parent_request_id": parentRequestID,
			"timestamp":         ndTimestamp,
			"op":                "write_user_timeline",
			"op_payload": map[string]any{
				"user_id":  post.CreatorID,
				"post_ids": postIDs,
			},
		},
		homeTimelineTargets[0]: {
			"type":              "request",
			"request_id":        nestedRequestID(parentRequestID, "home_timeline"),
			"parent_request_id": parentRequestID,
			"timestamp":         ndTimestamp,
			"op":                "write_home_timeline",
			"op_payload": map[string]any{
				"user_id":  post.CreatorID,
				"post_ids": postIDs,
			},
		},
	}

	targetsByService := map[string][]string{
		"post_storage":  postStorageTargets,
		"user_timeline": userTimelineTargets,
		"home_timeline": homeTimelineTargets,
	}
	for _, serviceName := range []string{"post_storage", "user_timeline", "home_timeline"} {
		targets := targetsByService[serviceName]
		socialDispatchNestedRequest(e, request, targets, outgoingByTarget[targets[0]])
	}
}

func composeNestedResponsesReady(nestedResponses []map[string]any) bool {
	return hasNestedChildResponse(nestedResponses, "post_storage") &&
		hasNestedChildResponse(nestedResponses, "user_timeline") &&
		hasNestedChildResponse(nestedResponses, "home_timeline")
}

func hasNestedChildResponse(nestedResponses []map[string]any, serviceName string) bool {
	expectedSuffix := "/" + serviceName
	for _, nested := range nestedResponses {
		if shimAggregated, _ := nested["shim_quorum_aggregated"].(bool); !shimAggregated {
			continue
		}
		requestID, _ := nested["request_id"].(string)
		if len(requestID) >= len(expectedSuffix) && requestID[len(requestID)-len(expectedSuffix):] == expectedSuffix {
			return true
		}
	}
	return false
}

func blockedForNestedResponse(requestID any) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "blocked_for_nested_response",
	}
}

func commonPayloadString(request map[string]any, key string) string {
	opPayload, _ := request["op_payload"].(map[string]any)
	if opPayload == nil {
		return ""
	}
	value, _ := opPayload[key].(string)
	return value
}

func commonPayloadStringSlice(request map[string]any, key string) []string {
	opPayload, _ := request["op_payload"].(map[string]any)
	if opPayload == nil {
		return nil
	}
	raw, ok := opPayload[key].([]any)
	if !ok {
		if typed, ok := opPayload[key].([]string); ok {
			return append([]string{}, typed...)
		}
		return nil
	}
	values := make([]string, 0, len(raw))
	for _, value := range raw {
		if text, ok := value.(string); ok {
			values = append(values, text)
		}
	}
	return values
}

func errorResponse(requestID any, message string) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"status":     "error",
		"error":      message,
	}
}
