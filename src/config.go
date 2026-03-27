package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	Type                     string         `json:"type"`
	Service                  string         `json:"service"`
	Next                     []string       `json:"next"`
	Clients                  []string       `json:"clients"`
	Nodes                    []string       `json:"nodes"`
	Shim                     string         `json:"shim"`
	IsPrimaryBatcher         bool           `json:"is_primary_batcher"`
	ExecWorkflow             string         `json:"exec_workflow"`
	InitStateWorkflow        string         `json:"init_state_workflow"`
	ClientWorkflow           string         `json:"client_workflow"`
	ExternalServiceWorkflow  string         `json:"external_service_workflow"`
	ExternalServiceInitState string         `json:"external_service_init_state"`
	ShimQuorumSize           int            `json:"shimQuorumSize"`
	VerifyResponseQuorumSize int            `json:"verifyResponseQuorumSize"`
	ExecVerifyQuorumSize     int            `json:"execVerifyQuorumSize"`
	PhaseQuorumSize          int            `json:"phaseQuorumSize"`
	ExpectedExecVotes        int            `json:"expectedExecVotes"`
	BatcherConfig           map[string]any `json:"batcher_config"`
}

type RunConfig struct {
	Architecture string         `json:"architecture"`
	Params       map[string]any `json:"-"`
}

func loadConfig(path string) (map[string]NodeConfig, error) {
	rawRoot, err := loadObjectFile(path)
	if err != nil {
		return nil, err
	}

	layered := struct {
		Services map[string]any
		Nodes    map[string]any
	}{}
	if err := decodeViaJSON(rawRoot, &layered); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if len(layered.Services) == 0 {
		return nil, fmt.Errorf("config %s missing required top-level \"services\"", path)
	}
	if len(layered.Nodes) == 0 {
		return nil, fmt.Errorf("config %s missing required top-level \"nodes\"", path)
	}

	cfg := make(map[string]NodeConfig, len(layered.Nodes))
	for nodeName, rawNode := range layered.Nodes {
		nodeMap, err := asObject(rawNode)
		if err != nil {
			return nil, fmt.Errorf("parse node %q: %w", nodeName, err)
		}

		serviceName, _ := nodeMap["service"].(string)
		if serviceName == "" {
			return nil, fmt.Errorf("node %q missing required field \"service\"", nodeName)
		}

		serviceRaw, ok := layered.Services[serviceName]
		if !ok {
			return nil, fmt.Errorf("node %q references unknown service %q", nodeName, serviceName)
		}

		serviceMap, err := asObject(serviceRaw)
		if err != nil {
			return nil, fmt.Errorf("parse service %q: %w", serviceName, err)
		}
		if _, hasBatcherConfig := serviceMap["batcher_config"]; !hasBatcherConfig {
			if legacy, hasLegacy := serviceMap["run_config_overrides"]; hasLegacy {
				serviceMap["batcher_config"] = legacy
			}
		}

		merged := make(map[string]any, len(serviceMap)+len(nodeMap))
		for k, v := range serviceMap {
			merged[k] = v
		}
		for k, v := range nodeMap {
			merged[k] = v
		}

		var nodeCfg NodeConfig
		if err := decodeViaJSON(merged, &nodeCfg); err != nil {
			return nil, fmt.Errorf("decode merged config for node %q: %w", nodeName, err)
		}

		cfg[nodeName] = nodeCfg
	}

	return cfg, nil
}

func loadRunConfig(path string) (RunConfig, error) {
	raw, err := loadObjectFile(path)
	if err != nil {
		return RunConfig{}, err
	}
	if raw == nil {
		return RunConfig{}, fmt.Errorf("run config %s must be an object", path)
	}

	architecture, _ := raw["architecture"].(string)
	if architecture == "" {
		return RunConfig{}, fmt.Errorf("run config %s missing required field \"architecture\"", path)
	}
	if err := requirePositiveIntegerField(raw, "run_timeout_seconds"); err != nil {
		return RunConfig{}, fmt.Errorf("run config %s %w", path, err)
	}
	if !filepath.IsAbs(architecture) {
		architecture, err = resolveArchitecturePath(path, architecture)
		if err != nil {
			return RunConfig{}, err
		}
	}
	delete(raw, "architecture")

	return RunConfig{
		Architecture: architecture,
		Params:       raw,
	}, nil
}

func resolveArchitecturePath(runConfigPath, architecture string) (string, error) {
	currentDir := filepath.Dir(filepath.Clean(runConfigPath))
	for {
		if filepath.Base(currentDir) == "runs" {
			architectureDir := filepath.Join(filepath.Dir(currentDir), "architecture")
			info, err := os.Stat(architectureDir)
			if err == nil && info.IsDir() {
				return filepath.Clean(filepath.Join(architectureDir, architecture)), nil
			}
		}

		parentDir := filepath.Dir(currentDir)
		if parentDir == currentDir {
			break
		}
		currentDir = parentDir
	}

	return "", fmt.Errorf(
		"run config %s: could not resolve architecture directory for %q",
		runConfigPath,
		architecture,
	)
}

func loadObjectFile(path string) (map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var obj map[string]any
	switch ext := filepath.Ext(path); ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &obj); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	default:
		if err := json.Unmarshal(data, &obj); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	if obj == nil {
		return nil, fmt.Errorf("config %s must be an object", path)
	}
	return obj, nil
}

func asObject(raw any) (map[string]any, error) {
	obj, ok := raw.(map[string]any)
	if ok && obj != nil {
		return obj, nil
	}

	b, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}

	obj = map[string]any{}
	if err := json.Unmarshal(b, &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func requirePositiveIntegerField(raw map[string]any, field string) error {
	value, ok := raw[field]
	if !ok {
		return fmt.Errorf("missing required field %q", field)
	}

	if _, ok := asPositiveInt(value); !ok {
		return fmt.Errorf("field %q must be a positive integer", field)
	}

	return nil
}

func decodeViaJSON(input any, out any) error {
	b, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

func asPositiveInt(value any) (int64, bool) {
	switch n := value.(type) {
	case int:
		if n > 0 {
			return int64(n), true
		}
	case int8:
		if n > 0 {
			return int64(n), true
		}
	case int16:
		if n > 0 {
			return int64(n), true
		}
	case int32:
		if n > 0 {
			return int64(n), true
		}
	case int64:
		if n > 0 {
			return n, true
		}
	case uint:
		if n > 0 {
			return int64(n), true
		}
	case uint8:
		if n > 0 {
			return int64(n), true
		}
	case uint16:
		if n > 0 {
			return int64(n), true
		}
	case uint32:
		if n > 0 {
			return int64(n), true
		}
	case uint64:
		if n > 0 && n <= uint64(^uint64(0)>>1) {
			return int64(n), true
		}
	case float64:
		if n > 0 && n == float64(int64(n)) {
			return int64(n), true
		}
	}
	return 0, false
}
