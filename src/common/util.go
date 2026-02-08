package common

// CopyStringMap copies a map[string]string defensively
func CopyStringMap(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

// GetFloat extracts a float64 from a map with numeric types
func GetFloat(m map[string]any, key string) float64 {
	if value, ok := m[key]; ok {
		switch v := value.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return 0
}

// GetInt extracts an int from a map with numeric types
func GetInt(m map[string]any, key string) int {
	if value, ok := m[key]; ok {
		switch v := value.(type) {
		case float64:
			return int(v)
		case int:
			return v
		case int64:
			return int(v)
		}
	}
	return 0
}

// GetInt64 extracts an int64 from a map with numeric types
func GetInt64(m map[string]any, key string) int64 {
	if value, ok := m[key]; ok {
		switch v := value.(type) {
		case float64:
			return int64(v)
		case int:
			return int64(v)
		case int64:
			return v
		}
	}
	return 0
}

// GetString extracts a string from a map.
func GetString(m map[string]any, key string) string {
	if value, ok := m[key]; ok {
		if s, ok := value.(string); ok {
			return s
		}
	}
	return ""
}

// MaxInt returns the larger of a and b
func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// TruncateToken returns token with a max length of 16 chars
func TruncateToken(token string) string {
	if len(token) <= 16 {
		return token
	}
	return token[:16]
}
