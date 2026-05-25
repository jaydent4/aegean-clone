package socialworkflow

import (
	"aegean/common"
	"sync/atomic"
)

const socialGraphNonidemConfigKey = "social_graph_nonidem"

var socialGraphNonidemCounter atomic.Uint64

func resetSocialGraphNonidemState() {
	socialGraphNonidemCounter.Store(0)
}

func socialGraphFollowersForResponse(e workflowRuntime, userID string, stableFollowers []string) ([]string, uint64) {
	return socialGraphFollowersForRunConfig(e.GetRunConfig(), userID, stableFollowers)
}

func socialGraphFollowersForRunConfig(runConfig map[string]any, userID string, stableFollowers []string) ([]string, uint64) {
	if !common.BoolOrDefault(runConfig, socialGraphNonidemConfigKey, false) {
		return append([]string{}, stableFollowers...), 0
	}

	version := socialGraphNonidemCounter.Add(1)
	userCount := common.MustInt(runConfig, "social_user_count")
	if userCount <= 1 {
		return []string{}, version
	}

	followerCount := common.IntOrDefault(
		runConfig,
		"social_graph_nonidem_followers_per_response",
		common.IntOrDefault(runConfig, "social_followers_per_user", len(stableFollowers)),
	)
	if followerCount < 0 {
		followerCount = 0
	}
	if followerCount >= userCount {
		followerCount = userCount - 1
	}

	followers := make([]string, 0, followerCount)
	start := int(version % uint64(userCount))
	for offset := 0; len(followers) < followerCount && offset < userCount*2; offset++ {
		candidate := socialUserID((start + offset) % userCount)
		if candidate == userID {
			continue
		}
		followers = append(followers, candidate)
	}
	return uniqueSortedStrings(followers), version
}
