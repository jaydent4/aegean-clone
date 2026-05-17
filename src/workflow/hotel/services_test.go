package hotelworkflow

import (
	"reflect"
	"testing"
)

func TestExecuteRequestRecommendationUsesConfiguredPool(t *testing.T) {
	runtime := &hotelTestRuntime{
		runConfig: map[string]any{
			"hotel_hotel_count":         2,
			"hotel_recommendation_pool": "pool_2",
		},
		kv: map[string]string{
			hotelRecommendationKey("1"):                  encodeJSON(HotelRecommendation{HotelID: "1", Lat: 0, Lon: 0, Rate: 99, Price: 1}),
			hotelRecommendationKeyForPool("pool_2", "1"): encodeJSON(HotelRecommendation{HotelID: "1", Lat: 0, Lon: 0, Rate: 10, Price: 30}),
			hotelRecommendationKeyForPool("pool_2", "2"): encodeJSON(HotelRecommendation{HotelID: "2", Lat: 0, Lon: 1, Rate: 20, Price: 20}),
		},
	}

	response := ExecuteRequestRecommendation(runtime, map[string]any{
		"request_id": "r1",
		"op":         "get_recommendations",
		"op_payload": map[string]any{
			"require": "rate",
			"lat":     0,
			"lon":     0,
		},
	}, 0, 0)

	if response["status"] != "ok" {
		t.Fatalf("status = %v, want ok", response["status"])
	}
	if got := response["hotel_ids"]; !reflect.DeepEqual(got, []string{"2"}) {
		t.Fatalf("hotel_ids = %#v, want %#v", got, []string{"2"})
	}
}

func TestInitStateSeedsRecommendationRaceServices(t *testing.T) {
	t.Setenv("HOTEL_REDIS_ENABLE", "0")
	runtime := &hotelTestRuntime{
		runConfig: map[string]any{
			"service_name":              "recommendation_2",
			"hotel_hotel_count":         1,
			"hotel_recommendation_pool": "pool_2",
		},
		kv: map[string]string{},
	}

	state := InitState(runtime)
	if _, ok := state[hotelRecommendationKeyForPool("pool_2", "1")]; !ok {
		t.Fatalf("expected recommendation_2 init state to include %q", hotelRecommendationKeyForPool("pool_2", "1"))
	}
}

func TestHotelSeedRecommendationPoolsDiffer(t *testing.T) {
	base := hotelSeedRecommendationsForPool(4, "pool_1")
	pool2 := hotelSeedRecommendationsForPool(4, "pool_2")
	pool3 := hotelSeedRecommendationsForPool(4, "pool_3")

	if reflect.DeepEqual(base, pool2) {
		t.Fatalf("pool_1 and pool_2 recommendation data should differ")
	}
	if reflect.DeepEqual(pool2, pool3) {
		t.Fatalf("pool_2 and pool_3 recommendation data should differ")
	}
}
