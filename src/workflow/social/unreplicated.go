package socialworkflow

import "aegean/aegean/unreplicated"

func InitStateDirect(e *unreplicated.Engine) map[string]string {
	return InitState(e)
}

func ExecuteRequestComposePostDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestComposePost(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestPostStorageDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestPostStorage(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestUserTimelineDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestUserTimeline(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestHomeTimelineDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestHomeTimeline(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestSocialGraphDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestSocialGraph(e, request, ndSeed, ndTimestamp)
}
