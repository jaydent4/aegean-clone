package hotelworkflow

import (
	"reflect"
	"testing"
)

func TestHotelMustDateRange(t *testing.T) {
	got, ok := hotelMustDateRange("2015-04-09", "2015-04-12")
	if !ok {
		t.Fatalf("hotelMustDateRange returned !ok")
	}

	want := []string{"2015-04-09", "2015-04-10", "2015-04-11"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("hotelMustDateRange = %#v, want %#v", got, want)
	}
}

func TestHotelRequestIDStringPreservesNestedIDs(t *testing.T) {
	got := hotelRequestIDString("123/reservation")
	if got != "123/reservation" {
		t.Fatalf("hotelRequestIDString = %q, want %q", got, "123/reservation")
	}
}

func TestHotelUpdateSearchLedgerPersistsNormalizedQuery(t *testing.T) {
	runtime := &hotelTestRuntime{
		runConfig: map[string]any{},
		kv:        map[string]string{},
	}
	payload := map[string]any{
		"lat":      37.7749295,
		"lon":      -122.4194155,
		"in_date":  "2015-04-09",
		"out_date": "2015-04-10",
	}

	normalized := normalizeHotelSearchPayload(payload)
	first := hotelUpdateSearchLedger(runtime, normalized, []string{"2", "4"}, 1.25)
	if first.Count != 1 {
		t.Fatalf("first.Count = %d, want 1", first.Count)
	}
	if first.LastSeenMicros != 1_250_000 {
		t.Fatalf("first.LastSeenMicros = %d, want 1250000", first.LastSeenMicros)
	}
	if !reflect.DeepEqual(first.LastHotelIDs, []string{"2", "4"}) {
		t.Fatalf("first.LastHotelIDs = %#v, want %#v", first.LastHotelIDs, []string{"2", "4"})
	}

	key := hotelSearchLedgerKey(normalized)
	if key != "hotel:search:37.774929:-122.419415:2015-04-09:2015-04-10:1" {
		t.Fatalf("hotelSearchLedgerKey = %q", key)
	}

	second := hotelUpdateSearchLedger(runtime, normalized, []string{"9"}, 2.5)
	if second.Count != 2 {
		t.Fatalf("second.Count = %d, want 2", second.Count)
	}
	if second.LastSeenMicros != 2_500_000 {
		t.Fatalf("second.LastSeenMicros = %d, want 2500000", second.LastSeenMicros)
	}
	if !reflect.DeepEqual(second.LastHotelIDs, []string{"9"}) {
		t.Fatalf("second.LastHotelIDs = %#v, want %#v", second.LastHotelIDs, []string{"9"})
	}

	persisted, ok := decodeHotelSearchLedger(runtime.kv[key])
	if !ok {
		t.Fatalf("persisted search ledger did not decode: %q", runtime.kv[key])
	}
	if !reflect.DeepEqual(persisted, second) {
		t.Fatalf("persisted ledger = %#v, want %#v", persisted, second)
	}
}

type hotelTestRuntime struct {
	runConfig        map[string]any
	kv               map[string]string
	contexts         map[any]map[string]any
	nestedResponses  map[any][]map[string]any
	directDispatches []hotelTestDispatch
	eoDispatches     []hotelTestDispatch
}

type hotelTestDispatch struct {
	source   map[string]any
	targets  []string
	outgoing map[string]any
}

func (r *hotelTestRuntime) GetRunConfig() map[string]any {
	return r.runConfig
}

func (r *hotelTestRuntime) ReadKV(key string) string {
	return r.kv[key]
}

func (r *hotelTestRuntime) WriteKV(key, value string) {
	r.kv[key] = value
}

func (r *hotelTestRuntime) SetRequestContextValue(requestID any, key string, value any) bool {
	if r.contexts == nil {
		r.contexts = map[any]map[string]any{}
	}
	if r.contexts[requestID] == nil {
		r.contexts[requestID] = map[string]any{}
	}
	r.contexts[requestID][key] = value
	return true
}

func (r *hotelTestRuntime) GetRequestContextValue(requestID any, key string) (any, bool) {
	if r.contexts == nil || r.contexts[requestID] == nil {
		return nil, false
	}
	value, ok := r.contexts[requestID][key]
	return value, ok
}

func (r *hotelTestRuntime) DeleteRequestContextValue(requestID any, key string) {
	if r.contexts == nil || r.contexts[requestID] == nil {
		return
	}
	delete(r.contexts[requestID], key)
}

func (r *hotelTestRuntime) ClearRequestContext(requestID any) {
	delete(r.contexts, requestID)
}

func (r *hotelTestRuntime) GetNestedResponses(requestID any) ([]map[string]any, bool) {
	if r.nestedResponses == nil {
		return nil, false
	}
	responses := r.nestedResponses[requestID]
	if len(responses) == 0 {
		return nil, false
	}
	return responses, true
}

func (r *hotelTestRuntime) DispatchNestedRequestDirect(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
	r.directDispatches = append(r.directDispatches, hotelTestDispatch{
		source:   sourceRequest,
		targets:  append([]string{}, targets...),
		outgoing: outgoing,
	})
}

func (r *hotelTestRuntime) DispatchNestedRequestEO(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
	r.eoDispatches = append(r.eoDispatches, hotelTestDispatch{
		source:   sourceRequest,
		targets:  append([]string{}, targets...),
		outgoing: outgoing,
	})
}
