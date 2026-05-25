package socialworkflow

import (
	"aegean/common"
	"fmt"
)

// InitState seeds the subset services independently so each benchmark can start
// from a realistic social-network dataset without replaying writes first.
func InitState(e workflowRuntime) map[string]string {
	serviceName := common.MustString(e.GetRunConfig(), "service_name")
	var state map[string]string
	switch serviceName {
	case "post_storage":
		state = initPostStorageState(e)
	case "user_timeline":
		state = initUserTimelineState(e)
	case "home_timeline":
		state = initHomeTimelineState(e)
	case "social_graph":
		resetSocialGraphNonidemState()
		state = initSocialGraphState(e)
	default:
		state = map[string]string{}
	}
	return state
}

func initPostStorageState(e workflowRuntime) map[string]string {
	userCount := common.MustInt(e.GetRunConfig(), "social_user_count")
	postsPerUser := common.IntOrDefault(e.GetRunConfig(), "social_seed_posts_per_user", 0)
	if userCount <= 0 || postsPerUser <= 0 {
		return map[string]string{}
	}

	// Seed concrete post bodies so read workloads can expand timeline indexes.
	state := make(map[string]string, userCount*postsPerUser)
	for userIdx := 0; userIdx < userCount; userIdx++ {
		userID := socialUserID(userIdx)
		for postIdx := 0; postIdx < postsPerUser; postIdx++ {
			post := seededPost(userID, postIdx)
			state[postKey(post.PostID)] = encodePost(post)
		}
	}
	return state
}

func initUserTimelineState(e workflowRuntime) map[string]string {
	userCount := common.MustInt(e.GetRunConfig(), "social_user_count")
	postsPerUser := common.IntOrDefault(e.GetRunConfig(), "social_seed_posts_per_user", 0)
	if userCount <= 0 || postsPerUser <= 0 {
		return map[string]string{}
	}

	// Seed each user's own authored-post index.
	state := make(map[string]string, userCount)
	for userIdx := 0; userIdx < userCount; userIdx++ {
		userID := socialUserID(userIdx)
		postIDs := make([]string, 0, postsPerUser)
		for postIdx := 0; postIdx < postsPerUser; postIdx++ {
			postIDs = append(postIDs, seededPost(userID, postIdx).PostID)
		}
		state[userTimelineKey(userID)] = encodeStringSlice(postIDs)
	}
	return state
}

func initHomeTimelineState(e workflowRuntime) map[string]string {
	userCount := common.MustInt(e.GetRunConfig(), "social_user_count")
	postsPerUser := common.IntOrDefault(e.GetRunConfig(), "social_seed_posts_per_user", 0)
	followersPerUser := common.IntOrDefault(e.GetRunConfig(), "social_followers_per_user", 3)
	if userCount <= 0 || postsPerUser <= 0 {
		return map[string]string{}
	}
	if followersPerUser < 0 {
		followersPerUser = 0
	}
	if followersPerUser >= userCount {
		followersPerUser = userCount - 1
	}

	// Seed each user's home feed from followee posts so read_home_timeline can run
	// without first executing compose traffic.
	state := make(map[string]string, userCount)
	for userIdx := 0; userIdx < userCount; userIdx++ {
		userID := socialUserID(userIdx)
		postIDs := make([]string, 0, followersPerUser*postsPerUser)
		for offset := 1; offset <= followersPerUser; offset++ {
			followeeID := socialUserID((userIdx + offset) % userCount)
			for postIdx := 0; postIdx < postsPerUser; postIdx++ {
				postIDs = append(postIDs, seededPost(followeeID, postIdx).PostID)
			}
		}
		state[homeTimelineKey(userID)] = encodeStringSlice(appendTimelineEntries([]string{}, postIDs, 10))
	}
	return state
}

func initSocialGraphState(e workflowRuntime) map[string]string {
	userCount := common.MustInt(e.GetRunConfig(), "social_user_count")
	followersPerUser := common.IntOrDefault(e.GetRunConfig(), "social_followers_per_user", 3)
	if userCount <= 0 {
		return map[string]string{}
	}
	if followersPerUser < 0 {
		followersPerUser = 0
	}
	if followersPerUser >= userCount {
		followersPerUser = userCount - 1
	}

	// Seed a deterministic ring-like follower graph used by compose fanout.
	state := make(map[string]string, userCount)
	for userIdx := 0; userIdx < userCount; userIdx++ {
		userID := socialUserID(userIdx)
		followers := make([]string, 0, followersPerUser)
		followees := make([]string, 0, followersPerUser)
		for offset := 1; offset <= followersPerUser; offset++ {
			followerIdx := (userIdx - offset + userCount) % userCount
			followeeIdx := (userIdx + offset) % userCount
			followers = append(followers, socialUserID(followerIdx))
			followees = append(followees, socialUserID(followeeIdx))
		}
		entry := SocialGraphEntry{
			UserID:    userID,
			Followers: uniqueSortedStrings(followers),
			Followees: uniqueSortedStrings(followees),
		}
		state[socialGraphKey(userID)] = encodeSocialGraphEntry(entry)
	}
	return state
}

func socialUserID(userIdx int) string {
	return fmt.Sprintf("user-%d", userIdx)
}

func seededPost(userID string, postIdx int) Post {
	return Post{
		PostID:    fmt.Sprintf("seed-post-%s-%d", userID, postIdx),
		Timestamp: int64(postIdx + 1),
		Text:      fmt.Sprintf("seed post %d for %s", postIdx, userID),
		CreatorID: userID,
	}
}
