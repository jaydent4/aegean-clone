package keys

func AddGenericWorkflowKeys(request map[string]any, payload map[string]any, readKeys map[string]struct{}, writeKeys map[string]struct{}) {
	op, _ := request["op"].(string)

	switch op {
	case "read":
		if key, ok := payload["key"].(string); ok {
			readKeys[key] = struct{}{}
		}
	case "write":
		if key, ok := payload["key"].(string); ok {
			writeKeys[key] = struct{}{}
		}
	case "read_write":
		if key, ok := payload["read_key"].(string); ok {
			readKeys[key] = struct{}{}
		}
		if key, ok := payload["write_key"].(string); ok {
			writeKeys[key] = struct{}{}
		}
	case "spin_write_read":
		if key, ok := payload["read_key"].(string); ok {
			readKeys[key] = struct{}{}
		}
		if key, ok := payload["write_key"].(string); ok {
			writeKeys[key] = struct{}{}
		}
	}
}
