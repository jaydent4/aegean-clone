package keys

const (
	SocialMixerModeConservativeHomeFanout = "conservative_home_fanout"
	SocialMixerModeNoHomeFanoutKey        = "no_home_fanout_key"
)

func ResolveSocialMixerMode(runConfig map[string]any) string {
	mode := SocialMixerModeNoHomeFanoutKey
	if configured, ok := runConfig["social_mixer_mode"]; ok {
		raw, ok := configured.(string)
		if !ok {
			panic("run config field \"social_mixer_mode\" must be a string")
		}
		mode = raw
	}
	switch mode {
	case SocialMixerModeConservativeHomeFanout, SocialMixerModeNoHomeFanoutKey:
		return mode
	default:
		panic("run config field \"social_mixer_mode\" must be one of: conservative_home_fanout, no_home_fanout_key")
	}
}

func AddSocialWorkflowKeys(request map[string]any, payload map[string]any, readKeys map[string]struct{}, writeKeys map[string]struct{}, activeSocialMixerMode string) {
	op, _ := request["op"].(string)

	switch op {
	case "write_user_timeline":
		if userID, ok := payload["user_id"].(string); ok && userID != "" {
			writeKeys["user_timeline:"+userID] = struct{}{}
		}
	case "read_user_timeline", "ro_read_user_timeline":
		if userID, ok := payload["user_id"].(string); ok && userID != "" {
			readKeys["user_timeline:"+userID] = struct{}{}
		}
	case "write_home_timeline":
		switch activeSocialMixerMode {
		case SocialMixerModeConservativeHomeFanout:
			// Fanout writes update follower feed keys discovered at runtime via
			// social_graph, so different creators can still touch overlapping
			// home_timeline entries. Use one shared key to force a deterministic
			// order for all write_home_timeline requests within a batch.
			writeKeys["home_timeline_fanout"] = struct{}{}
		case SocialMixerModeNoHomeFanoutKey:
			// Keep social keys for the other ops, but intentionally skip a mixer key
			// for write_home_timeline to maximize overlap on the fanout stage.
		}
	case "read_home_timeline", "ro_read_home_timeline":
		if userID, ok := payload["user_id"].(string); ok && userID != "" {
			readKeys["home_timeline:"+userID] = struct{}{}
		}
	case "store_post":
		if postID, ok := payload["post_id"].(string); ok && postID != "" {
			writeKeys["post:"+postID] = struct{}{}
		}
	case "read_post", "ro_read_post":
		if postID, ok := payload["post_id"].(string); ok && postID != "" {
			readKeys["post:"+postID] = struct{}{}
		}
	case "read_posts", "ro_read_posts":
		switch typed := payload["post_ids"].(type) {
		case []any:
			for _, raw := range typed {
				if postID, ok := raw.(string); ok && postID != "" {
					readKeys["post:"+postID] = struct{}{}
				}
			}
		case []string:
			for _, postID := range typed {
				if postID != "" {
					readKeys["post:"+postID] = struct{}{}
				}
			}
		}
	}
}
