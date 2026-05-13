package keys

import "testing"

func TestAddHotelWorkflowKeysForMakeReservation(t *testing.T) {
	readKeys := map[string]struct{}{}
	writeKeys := map[string]struct{}{}

	AddHotelWorkflowKeys(
		map[string]any{"op": "make_reservation", "request_id": "42/reservation"},
		map[string]any{
			"hotel_id":    "7",
			"in_date":     "2015-04-09",
			"out_date":    "2015-04-11",
			"room_number": 1,
		},
		readKeys,
		writeKeys,
	)

	expectedReadKeys := []string{
		"hotel:reservation:capacity:7",
		"hotel:reservation:count:7:2015-04-09",
		"hotel:reservation:count:7:2015-04-10",
	}
	for _, key := range expectedReadKeys {
		if _, ok := readKeys[key]; !ok {
			t.Fatalf("missing read key %q", key)
		}
	}

	expectedWriteKeys := []string{
		"hotel:reservation:count:7:2015-04-09",
		"hotel:reservation:count:7:2015-04-10",
		"hotel:reservation:record:42/reservation",
	}
	for _, key := range expectedWriteKeys {
		if _, ok := writeKeys[key]; !ok {
			t.Fatalf("missing write key %q", key)
		}
	}
}

func TestAddHotelWorkflowKeysForNearbyAndRecommendation(t *testing.T) {
	readKeys := map[string]struct{}{}
	writeKeys := map[string]struct{}{}

	AddHotelWorkflowKeys(
		map[string]any{"op": "nearby"},
		map[string]any{},
		readKeys,
		writeKeys,
	)
	AddHotelWorkflowKeys(
		map[string]any{"op": "get_recommendations"},
		map[string]any{},
		readKeys,
		writeKeys,
	)

	for _, key := range []string{"hotel:geo:index", "hotel:recommendation:index"} {
		if _, ok := readKeys[key]; !ok {
			t.Fatalf("missing read key %q", key)
		}
	}
	if len(writeKeys) != 0 {
		t.Fatalf("unexpected write keys: %#v", writeKeys)
	}
}
