package hotelworkflow

import "aegean/aegean/unreplicated"

func InitStateDirect(e *unreplicated.Engine) map[string]string {
	return InitState(e)
}

func ExecuteRequestFrontendDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestFrontend(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestSearchDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestSearch(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestGeoDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestGeo(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestRateDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestRate(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestProfileDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestProfile(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestRecommendationDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestRecommendation(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestUserDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestUser(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestReservationDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestReservation(e, request, ndSeed, ndTimestamp)
}
