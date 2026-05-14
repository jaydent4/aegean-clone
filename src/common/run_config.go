package common

import (
	"fmt"
	"math"
)

func MustInt(config map[string]any, key string) int {
	value, ok := config[key]
	if !ok {
		panic(fmt.Sprintf("run config missing required int field %q", key))
	}

	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		if math.Trunc(typed) != typed {
			panic(fmt.Sprintf("run config field %q must be an integer", key))
		}
		return int(typed)
	default:
		panic(fmt.Sprintf("run config field %q must be an integer", key))
	}
}

func MustFloat64(config map[string]any, key string) float64 {
	value, ok := config[key]
	if !ok {
		panic(fmt.Sprintf("run config missing required float field %q", key))
	}

	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	default:
		panic(fmt.Sprintf("run config field %q must be a float", key))
	}
}

func MustString(config map[string]any, key string) string {
	value, ok := config[key]
	if !ok {
		panic(fmt.Sprintf("run config missing required string field %q", key))
	}
	typed, ok := value.(string)
	if !ok {
		panic(fmt.Sprintf("run config field %q must be a string", key))
	}
	return typed
}

func StringOrDefault(config map[string]any, key string, defaultValue string) string {
	value, ok := config[key]
	if !ok {
		return defaultValue
	}
	typed, ok := value.(string)
	if !ok {
		panic(fmt.Sprintf("run config field %q must be a string", key))
	}
	return typed
}

func IntOrDefault(config map[string]any, key string, defaultValue int) int {
	value, ok := config[key]
	if !ok {
		return defaultValue
	}

	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		if math.Trunc(typed) != typed {
			panic(fmt.Sprintf("run config field %q must be an integer", key))
		}
		return int(typed)
	default:
		panic(fmt.Sprintf("run config field %q must be an integer", key))
	}
}

func K6PreAllocatedVUs(config map[string]any, rate int) int {
	preAllocatedVUs := MustInt(config, "k6_pre_allocated_vus")
	if preAllocatedVUs > 0 {
		return preAllocatedVUs
	}
	if rate > 0 {
		return rate
	}
	return 1
}

func BoolOrDefault(config map[string]any, key string, defaultValue bool) bool {
	value, ok := config[key]
	if !ok {
		return defaultValue
	}
	typed, ok := value.(bool)
	if !ok {
		panic(fmt.Sprintf("run config field %q must be a bool", key))
	}
	return typed
}

func ServiceNodesOrDefault(config map[string]any, serviceName string, fallback []string) []string {
	raw, ok := config["service_nodes"]
	if !ok {
		return append([]string{}, fallback...)
	}

	var rawNodes any
	switch typed := raw.(type) {
	case map[string]any:
		rawNodes = typed[serviceName]
	case map[string][]string:
		if nodes, ok := typed[serviceName]; ok {
			return append([]string{}, nodes...)
		}
	default:
		return append([]string{}, fallback...)
	}

	switch typed := rawNodes.(type) {
	case []string:
		return append([]string{}, typed...)
	case []any:
		nodes := make([]string, 0, len(typed))
		for _, rawNode := range typed {
			nodeName, ok := rawNode.(string)
			if ok && nodeName != "" {
				nodes = append(nodes, nodeName)
			}
		}
		if len(nodes) > 0 {
			return nodes
		}
	}
	return append([]string{}, fallback...)
}
