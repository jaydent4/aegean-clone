package socialworkflow

import (
	"reflect"
	"testing"
)

type socialGraphTestRuntime struct {
	runConfig map[string]any
	kv        map[string]string
}

func (r *socialGraphTestRuntime) GetRunConfig() map[string]any { return r.runConfig }
func (r *socialGraphTestRuntime) ReadKV(key string) string     { return r.kv[key] }
func (r *socialGraphTestRuntime) WriteKV(key, value string)    { r.kv[key] = value }
func (r *socialGraphTestRuntime) SetRequestContextValue(requestID any, key string, value any) bool {
	return false
}
func (r *socialGraphTestRuntime) GetRequestContextValue(requestID any, key string) (any, bool) {
	return nil, false
}
func (r *socialGraphTestRuntime) DeleteRequestContextValue(requestID any, key string) {}
func (r *socialGraphTestRuntime) ClearRequestContext(requestID any)                   {}
func (r *socialGraphTestRuntime) GetNestedResponses(requestID any) ([]map[string]any, bool) {
	return nil, false
}
func (r *socialGraphTestRuntime) DispatchNestedRequestDirect(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
}
func (r *socialGraphTestRuntime) DispatchNestedRequestEO(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
}

func TestSocialGraphStableFollowersByDefault(t *testing.T) {
	runtime := newSocialGraphTestRuntime(false)
	request := socialGraphFollowersRequest("request-1", "user-3")

	first := ExecuteRequestSocialGraph(runtime, request, 0, 0)
	second := ExecuteRequestSocialGraph(runtime, request, 0, 0)

	if _, ok := first["social_graph_nonidem_version"]; ok {
		t.Fatalf("stable response unexpectedly included nonidem version: %#v", first)
	}
	if !reflect.DeepEqual(first["followers"], second["followers"]) {
		t.Fatalf("stable followers changed: first=%#v second=%#v", first["followers"], second["followers"])
	}
}

func TestSocialGraphNonidemFollowersChangeAcrossCalls(t *testing.T) {
	runtime := newSocialGraphTestRuntime(true)
	request := socialGraphFollowersRequest("request-1", "user-3")

	first := ExecuteRequestSocialGraph(runtime, request, 0, 0)
	second := ExecuteRequestSocialGraph(runtime, request, 0, 0)

	if !reflect.DeepEqual(first["social_graph_nonidem_version"], uint64(1)) {
		t.Fatalf("first nonidem version = %#v, want 1", first["social_graph_nonidem_version"])
	}
	if !reflect.DeepEqual(second["social_graph_nonidem_version"], uint64(2)) {
		t.Fatalf("second nonidem version = %#v, want 2", second["social_graph_nonidem_version"])
	}
	if reflect.DeepEqual(first["followers"], second["followers"]) {
		t.Fatalf("nonidem followers did not change: %#v", first["followers"])
	}
}

func newSocialGraphTestRuntime(nonidem bool) *socialGraphTestRuntime {
	runtime := &socialGraphTestRuntime{
		runConfig: map[string]any{
			"service_name":                    "social_graph",
			"social_user_count":               8,
			"social_followers_per_user":       3,
			"social_graph_nonidem":            nonidem,
			"social_nested_send_all_replicas": false,
		},
		kv: map[string]string{},
	}
	runtime.kv = InitState(runtime)
	return runtime
}

func socialGraphFollowersRequest(requestID string, userID string) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"op":         "get_followers",
		"op_payload": map[string]any{
			"user_id": userID,
		},
	}
}
