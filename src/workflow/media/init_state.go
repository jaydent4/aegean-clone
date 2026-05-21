package mediaworkflow

import (
	"aegean/common"
	"strconv"
)

func InitState(e workflowRuntime) map[string]string {
	serviceName := common.MustString(e.GetRunConfig(), "service_name")
	var state map[string]string
	switch serviceName {
	case "user":
		state = initMediaUserState(e)
	case "movie_id":
		state = initMediaMovieIDState(e)
	case "rating":
		state = initMediaRatingState(e)
	default:
		state = map[string]string{}
	}
	return state
}

func initMediaUserState(e workflowRuntime) map[string]string {
	userCount := common.MustInt(e.GetRunConfig(), "media_user_count")
	if userCount <= 0 {
		return map[string]string{}
	}
	state := make(map[string]string, userCount)
	for userIdx := 0; userIdx < userCount; userIdx++ {
		username := mediaSeedUsername(userIdx)
		userID := int64(100000 + userIdx)
		state[mediaUserLookupKey(username)] = mediaInt64String(userID)
	}
	return state
}

func initMediaMovieIDState(e workflowRuntime) map[string]string {
	movieCount := common.MustInt(e.GetRunConfig(), "media_movie_count")
	if movieCount <= 0 {
		return map[string]string{}
	}
	state := make(map[string]string, movieCount)
	for movieIdx := 0; movieIdx < movieCount; movieIdx++ {
		state[mediaMovieLookupKey(mediaSeedMovieTitle(movieIdx))] = mediaSeedMovieID(movieIdx)
	}
	return state
}

func initMediaRatingState(e workflowRuntime) map[string]string {
	movieCount := common.MustInt(e.GetRunConfig(), "media_movie_count")
	if movieCount <= 0 {
		return map[string]string{}
	}
	state := make(map[string]string, movieCount*2)
	for movieIdx := 0; movieIdx < movieCount; movieIdx++ {
		movieID := mediaSeedMovieID(movieIdx)
		state[mediaRatingSumKey(movieID)] = strconv.Itoa(0)
		state[mediaRatingCountKey(movieID)] = strconv.Itoa(0)
	}
	return state
}
