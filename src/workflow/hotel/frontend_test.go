package hotelworkflow

import (
	"reflect"
	"testing"
)

func TestFrontendRecommendationRaceDispatchesAllServices(t *testing.T) {
	runtime := &hotelTestRuntime{
		runConfig: map[string]any{
			"hotel_recommendation_race_services": []any{"recommendation_1", "recommendation_2", "recommendation_3"},
			"service_nodes": map[string]any{
				"recommendation_1": []any{"node7", "node8", "node9"},
				"recommendation_2": []any{"node10", "node11", "node12"},
				"recommendation_3": []any{"node13", "node14", "node15"},
			},
		},
		kv: map[string]string{},
	}
	request := map[string]any{
		"request_id": "r1",
		"op":         "recommend_hotels",
		"op_payload": map[string]any{
			"require": "rate",
			"lat":     37.78,
			"lon":     -122.4,
			"locale":  "en",
		},
	}

	response := ExecuteRequestFrontend(runtime, request, 0, 1.25)
	if response["status"] != "blocked_for_nested_response" {
		t.Fatalf("status = %v, want blocked_for_nested_response", response["status"])
	}
	if len(runtime.directDispatches) != 3 {
		t.Fatalf("direct dispatch count = %d, want 3", len(runtime.directDispatches))
	}

	gotRequestIDs := []string{}
	for _, dispatch := range runtime.directDispatches {
		gotRequestIDs = append(gotRequestIDs, dispatch.outgoing["request_id"].(string))
	}
	wantRequestIDs := []string{
		"r1/recommendation_1",
		"r1/recommendation_2",
		"r1/recommendation_3",
	}
	if !reflect.DeepEqual(gotRequestIDs, wantRequestIDs) {
		t.Fatalf("dispatched request IDs = %#v, want %#v", gotRequestIDs, wantRequestIDs)
	}
}

func TestFrontendRecommendationRaceUsesFirstReadyResponse(t *testing.T) {
	runtime := &hotelTestRuntime{
		runConfig: map[string]any{
			"hotel_recommendation_race_services": []string{"recommendation_1", "recommendation_2", "recommendation_3"},
			"service_nodes": map[string]any{
				"recommendation_1": []any{"node7", "node8", "node9"},
				"recommendation_2": []any{"node10", "node11", "node12"},
				"recommendation_3": []any{"node13", "node14", "node15"},
				"profile":          []any{"node4", "node5", "node6"},
			},
		},
		kv: map[string]string{},
	}
	request := map[string]any{
		"request_id": "r1",
		"op":         "recommend_hotels",
		"op_payload": map[string]any{
			"require": "rate",
			"lat":     37.78,
			"lon":     -122.4,
			"locale":  "en",
		},
	}
	_ = ExecuteRequestFrontend(runtime, request, 0, 1.25)
	runtime.directDispatches = nil
	runtime.nestedResponses = map[any][]map[string]any{
		"r1": {
			{
				"request_id":             "r1/recommendation_2",
				"shim_quorum_aggregated": true,
				"response": map[string]any{
					"hotel_ids": []any{"2"},
				},
			},
			{
				"request_id":             "r1/recommendation_1",
				"shim_quorum_aggregated": true,
				"response": map[string]any{
					"hotel_ids": []any{"1"},
				},
			},
		},
	}

	response := ExecuteRequestFrontend(runtime, request, 0, 1.50)
	if response["status"] != "blocked_for_nested_response" {
		t.Fatalf("status = %v, want blocked_for_nested_response", response["status"])
	}
	if len(runtime.directDispatches) != 1 {
		t.Fatalf("profile dispatch count = %d, want 1", len(runtime.directDispatches))
	}
	profileRequest := runtime.directDispatches[0].outgoing
	if profileRequest["request_id"] != "r1/profile_recommendation_2" {
		t.Fatalf("profile request_id = %v, want r1/profile_recommendation_2", profileRequest["request_id"])
	}
	payload := profileRequest["op_payload"].(map[string]any)
	if got := payload["hotel_ids"]; !reflect.DeepEqual(got, []string{"2"}) {
		t.Fatalf("profile hotel_ids = %#v, want %#v", got, []string{"2"})
	}
}
