package exec

const maxLoggedValueChars = 20

func truncateLogString(value string) string {
	runes := []rune(value)
	if len(runes) <= maxLoggedValueChars {
		return value
	}
	return string(runes[:maxLoggedValueChars])
}

func truncateLogStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	truncated := make(map[string]string, len(values))
	for key, value := range values {
		truncated[key] = truncateLogString(value)
	}
	return truncated
}

func truncateLogValue(value any) any {
	switch typed := value.(type) {
	case string:
		return truncateLogString(typed)
	case map[string]string:
		return truncateLogStringMap(typed)
	case map[string]any:
		truncated := make(map[string]any, len(typed))
		for key, nested := range typed {
			truncated[key] = truncateLogValue(nested)
		}
		return truncated
	case []map[string]any:
		truncated := make([]map[string]any, len(typed))
		for idx, item := range typed {
			truncated[idx] = truncateLogValue(item).(map[string]any)
		}
		return truncated
	case []string:
		truncated := make([]string, len(typed))
		for idx, item := range typed {
			truncated[idx] = truncateLogString(item)
		}
		return truncated
	case []any:
		truncated := make([]any, len(typed))
		for idx, item := range typed {
			truncated[idx] = truncateLogValue(item)
		}
		return truncated
	default:
		return value
	}
}
