package socialworkflow

// PostStorage is the leaf service for post bodies. It either persists a post or
// expands post IDs into full post payloads for timeline readers.
func ExecuteRequestPostStorage(e workflowRuntime, request map[string]any, ndSeed int64, ndTimestamp float64) map[string]any {
	_ = ndSeed
	_ = ndTimestamp

	requestID := request["request_id"]
	op, _ := request["op"].(string)
	opPayload, _ := request["op_payload"].(map[string]any)

	switch op {
	case "store_post":
		// store_post: persist one post body under post:<post_id>. This is a leaf
		// write and does not call any other service.
		// Compose writes land here first so later timeline reads can fetch the post.
		postID, _ := opPayload["post_id"].(string)
		creatorID, _ := opPayload["creator_id"].(string)
		text, _ := opPayload["text"].(string)
		timestamp := int64(0)
		switch typed := opPayload["timestamp"].(type) {
		case int64:
			timestamp = typed
		case int:
			timestamp = int64(typed)
		case float64:
			timestamp = int64(typed)
		}
		if postID == "" || creatorID == "" {
			return errorResponse(requestID, "missing post payload")
		}
		post := Post{
			PostID:    postID,
			Timestamp: timestamp,
			Text:      text,
			CreatorID: creatorID,
		}
		e.WriteKV(postKey(postID), encodePost(post))
		return nestedOkResponseWithPostID(request, postID)
	case "read_post", "ro_read_post":
		// read_post: read one stored post body locally and return it directly.
		postID, _ := opPayload["post_id"].(string)
		post, ok := decodePost(e.ReadKV(postKey(postID)))
		if !ok {
			return errorResponse(requestID, "post not found")
		}
		return attachParentRequestID(request, map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"post": map[string]any{
				"post_id":    post.PostID,
				"timestamp":  post.Timestamp,
				"text":       post.Text,
				"creator_id": post.CreatorID,
			},
		})
	case "read_posts", "ro_read_posts":
		// read_posts: read multiple stored post bodies locally and return the
		// materialized list. Timeline services call this bulk leaf op.
		// Timeline readers use this bulk path to turn stored post IDs into a
		// fully materialized feed response.
		postIDs := commonPayloadStringSlice(request, "post_ids")
		posts := make([]map[string]any, 0, len(postIDs))
		for _, postID := range postIDs {
			post, ok := decodePost(e.ReadKV(postKey(postID)))
			if !ok {
				continue
			}
			posts = append(posts, map[string]any{
				"post_id":    post.PostID,
				"timestamp":  post.Timestamp,
				"text":       post.Text,
				"creator_id": post.CreatorID,
			})
		}
		return attachParentRequestID(request, map[string]any{
			"request_id": requestID,
			"status":     "ok",
			"posts":      posts,
		})
	default:
		return errorResponse(requestID, "unsupported op: "+op)
	}
}
