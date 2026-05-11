package socialworkflow

import (
	"aegean/common"
	"encoding/json"
	"fmt"
	"sort"
)

func postKey(postID string) string {
	return "post:" + postID
}

func userTimelineKey(userID string) string {
	return "user_timeline:" + userID
}

func homeTimelineKey(userID string) string {
	return "home_timeline:" + userID
}

func socialGraphKey(userID string) string {
	return "social_graph:" + userID
}

func deterministicPostID(request map[string]any) string {
	return fmt.Sprintf("post-%v", request["request_id"])
}

func nestedRequestID(parentRequestID any, serviceName string) string {
	return fmt.Sprintf("%v/%s", parentRequestID, serviceName)
}

func nestedRequestTargets(runConfig map[string]any, replicas []string) []string {
	// social_nested_send_all_replicas controls whether nested social RPCs are
	// sent only to the primary replica or broadcast to every replica.
	if len(replicas) == 0 {
		return nil
	}
	if !common.BoolOrDefault(runConfig, "social_nested_send_all_replicas", false) {
		return []string{replicas[0]}
	}
	return append([]string{}, replicas...)
}

func socialServiceTargets(e workflowRuntime, serviceName string, fallback []string) []string {
	return common.ServiceNodesOrDefault(e.GetRunConfig(), serviceName, fallback)
}

func socialDispatchNestedRequest(e workflowRuntime, sourceRequest map[string]any, targets []string, outgoing map[string]any) {
	selectedTargets := nestedRequestTargets(e.GetRunConfig(), targets)
	if common.BoolOrDefault(e.GetRunConfig(), "social_nested_use_eo", false) {
		e.DispatchNestedRequestEO(sourceRequest, selectedTargets, outgoing)
		return
	}
	e.DispatchNestedRequestDirect(sourceRequest, selectedTargets, outgoing)
}

func deterministicTimestamp(ndTimestamp float64) int64 {
	// TODO: This currently uses the batch-level ndTimestamp rather than the original
	// client request time. That keeps timestamp assignment deterministic inside
	// exec, but it is not an accurate per-request event timestamp because every
	// request in the same batch shares the same value.
	if ndTimestamp == 0 {
		return 0
	}
	return int64(ndTimestamp * 1_000_000)
}

func decodePost(raw string) (Post, bool) {
	var post Post
	if raw == "" {
		return post, false
	}
	if err := json.Unmarshal([]byte(raw), &post); err != nil {
		return Post{}, false
	}
	return post, true
}

func encodePost(post Post) string {
	bytes, err := json.Marshal(post)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func decodeStringSlice(raw string) []string {
	if raw == "" {
		return []string{}
	}
	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return []string{}
	}
	return values
}

func encodeStringSlice(values []string) string {
	bytes, err := json.Marshal(values)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func decodeSocialGraphEntry(raw string) (SocialGraphEntry, bool) {
	var entry SocialGraphEntry
	if raw == "" {
		return entry, false
	}
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return SocialGraphEntry{}, false
	}
	return entry, true
}

func encodeSocialGraphEntry(entry SocialGraphEntry) string {
	bytes, err := json.Marshal(entry)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func appendTimelineEntries(existing []string, newPostIDs []string, maxEntries int) []string {
	if len(newPostIDs) == 0 {
		return existing
	}
	combined := append(append([]string{}, existing...), newPostIDs...)
	if len(combined) <= maxEntries {
		return combined
	}
	return combined[len(combined)-maxEntries:]
}

func uniqueSortedStrings(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		set[value] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func extractStringSlice(value any) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string{}, typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, entry := range typed {
			if text, ok := entry.(string); ok {
				out = append(out, text)
			}
		}
		return out
	default:
		return []string{}
	}
}

func nestedOkResponse(request map[string]any) map[string]any {
	return attachParentRequestID(request, map[string]any{
		"request_id": request["request_id"],
		"status":     "ok",
	})
}

func nestedOkResponseWithPostID(request map[string]any, postID string) map[string]any {
	response := nestedOkResponse(request)
	response["post_id"] = postID
	return response
}

func attachParentRequestID(request map[string]any, response map[string]any) map[string]any {
	if parentRequestID, ok := request["parent_request_id"]; ok && parentRequestID != nil {
		response["parent_request_id"] = parentRequestID
	}
	return response
}
