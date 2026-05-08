package mediaworkflow

import "aegean/aegean/unreplicated"

func InitStateDirect(e *unreplicated.Engine) map[string]string {
	return InitState(e)
}

func ExecuteRequestReviewComposeAPIDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestReviewComposeAPI(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestUserDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestUser(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestMovieIDDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestMovieID(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestTextDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestText(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestUniqueIDDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestUniqueID(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestRatingDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestRating(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestComposeReviewDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestComposeReview(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestReviewStorageDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestReviewStorage(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestUserReviewDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestUserReview(e, request, ndSeed, ndTimestamp)
}

func ExecuteRequestMovieReviewDirect(e *unreplicated.Engine, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	return ExecuteRequestMovieReview(e, request, ndSeed, ndTimestamp)
}
