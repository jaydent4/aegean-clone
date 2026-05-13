package keys

func AddMediaWorkflowKeys(request map[string]any, payload map[string]any, readKeys map[string]struct{}, writeKeys map[string]struct{}) {
	op, _ := request["op"].(string)
	reviewRequestID, _ := payload["review_request_id"].(string)

	switch op {
	case "upload_user_with_username":
		if username, ok := payload["username"].(string); ok && username != "" {
			readKeys["media:user:username:"+username] = struct{}{}
		}
	case "upload_movie_id":
		if title, ok := payload["title"].(string); ok && title != "" {
			readKeys["media:movie:title:"+title] = struct{}{}
		}
		if reviewRequestID != "" {
			writeKeys["media:compose:"+reviewRequestID] = struct{}{}
		}
	case "upload_text", "upload_unique_id", "upload_user_id":
		if reviewRequestID != "" {
			writeKeys["media:compose:"+reviewRequestID] = struct{}{}
		}
	case "upload_rating":
		if movieID, ok := payload["movie_id"].(string); ok && movieID != "" {
			writeKeys["media:rating:"+movieID] = struct{}{}
		}
		if reviewRequestID != "" {
			writeKeys["media:compose:"+reviewRequestID] = struct{}{}
		}
	case "store_review":
		if review, ok := payload["review"].(map[string]any); ok {
			addMediaReviewIDKey(review, writeKeys)
		}
	case "upload_user_review":
		addMediaNumericPayloadKey(payload, "user_id", "media:user_review:", writeKeys)
	case "upload_movie_review":
		if movieID, ok := payload["movie_id"].(string); ok && movieID != "" {
			writeKeys["media:movie_review:"+movieID] = struct{}{}
		}
	}
}

func addMediaReviewIDKey(payload map[string]any, writeKeys map[string]struct{}) {
	addMediaNumericPayloadKey(payload, "review_id", "media:review:", writeKeys)
}

func addMediaNumericPayloadKey(payload map[string]any, key string, prefix string, writeKeys map[string]struct{}) {
	switch typed := payload[key].(type) {
	case int:
		writeKeys[prefix+itoaMediaKey(int64(typed))] = struct{}{}
	case int64:
		writeKeys[prefix+itoaMediaKey(typed)] = struct{}{}
	case float64:
		writeKeys[prefix+itoaMediaKey(int64(typed))] = struct{}{}
	case string:
		if typed != "" {
			writeKeys[prefix+typed] = struct{}{}
		}
	}
}

func itoaMediaKey(value int64) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
