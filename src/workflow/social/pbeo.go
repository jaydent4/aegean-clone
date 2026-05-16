package socialworkflow

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

func ExecuteRequestComposePostPBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	return ExecuteRequestComposePost(tx, request, 0, socialPBEORequestTimestamp(request))
}

func ExecuteRequestPostStoragePBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	return ExecuteRequestPostStorage(tx, request, 0, socialPBEORequestTimestamp(request))
}

func ExecuteRequestUserTimelinePBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	return ExecuteRequestUserTimeline(tx, request, 0, socialPBEORequestTimestamp(request))
}

func ExecuteRequestHomeTimelinePBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	return ExecuteRequestHomeTimeline(tx, request, 0, socialPBEORequestTimestamp(request))
}

func ExecuteRequestSocialGraphPBEO(tx *pbeo.Txn, request map[string]any) map[string]any {
	return ExecuteRequestSocialGraph(tx, request, 0, socialPBEORequestTimestamp(request))
}

func socialPBEORequestTimestamp(request map[string]any) float64 {
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
