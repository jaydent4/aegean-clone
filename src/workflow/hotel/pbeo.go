package hotelworkflow

import (
	"encoding/json"

	"aegean/pbeo"
)

type pbeoInitRuntime struct {
	runConfig map[string]any
	kv        map[string]string
}

func (r *pbeoInitRuntime) GetRunConfig() map[string]any { return r.runConfig }
func (r *pbeoInitRuntime) ReadKV(key string) string     { return r.kv[key] }
func (r *pbeoInitRuntime) WriteKV(key, value string)    { r.kv[key] = value }
func (r *pbeoInitRuntime) SetRequestContextValue(requestID any, key string, value any) bool {
	return false
}
func (r *pbeoInitRuntime) GetRequestContextValue(requestID any, key string) (any, bool) {
	return nil, false
}
func (r *pbeoInitRuntime) DeleteRequestContextValue(requestID any, key string) {}
func (r *pbeoInitRuntime) ClearRequestContext(requestID any)                   {}
func (r *pbeoInitRuntime) GetNestedResponses(requestID any) ([]map[string]any, bool) {
	return nil, false
}
func (r *pbeoInitRuntime) DispatchNestedRequestDirect(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
}
func (r *pbeoInitRuntime) DispatchNestedRequestEO(sourceRequest map[string]any, targets []string, outgoing map[string]any) {
}

func InitStatePBEO(runConfig map[string]any) map[string]string {
	runtime := &pbeoInitRuntime{
		runConfig: runConfig,
		kv:        map[string]string{},
	}
	return InitState(runtime)
}

func ExecuteRequestFrontendPBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	return ExecuteRequestFrontend(tx, request, 0, hotelPBEORequestTimestamp(request))
}

func ExecuteRequestSearchPBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	return ExecuteRequestSearch(tx, request, 0, hotelPBEORequestTimestamp(request))
}

func ExecuteRequestGeoPBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	return ExecuteRequestGeo(tx, request, 0, hotelPBEORequestTimestamp(request))
}

func ExecuteRequestRatePBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	return ExecuteRequestRate(tx, request, 0, hotelPBEORequestTimestamp(request))
}

func ExecuteRequestProfilePBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	return ExecuteRequestProfile(tx, request, 0, hotelPBEORequestTimestamp(request))
}

func ExecuteRequestRecommendationPBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	return ExecuteRequestRecommendation(tx, request, 0, hotelPBEORequestTimestamp(request))
}

func ExecuteRequestUserPBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	return ExecuteRequestUser(tx, request, 0, hotelPBEORequestTimestamp(request))
}

func ExecuteRequestReservationPBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	return ExecuteRequestReservation(tx, request, 0, hotelPBEORequestTimestamp(request))
}

func hotelPBEORequestTimestamp(request map[string]any) float64 {
	switch value := request["timestamp"].(type) {
	case float64:
		return value
	case float32:
		return float64(value)
	case int:
		return float64(value)
	case int64:
		return float64(value)
	case json.Number:
		parsed, _ := value.Float64()
		return parsed
	default:
		return 0
	}
}
