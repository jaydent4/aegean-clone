package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type NodeConfig struct {
	Type                     string   `json:"type"`
	Service                  string   `json:"service"`
	Next                     []string `json:"next"`
	Clients                  []string `json:"clients"`
	Nodes                    []string `json:"nodes"`
	Shim                     string   `json:"shim"`
	IsPrimaryBatcher         bool     `json:"is_primary_batcher"`
	ExecWorkflow             string   `json:"exec_workflow"`
	InitStateWorkflow        string   `json:"init_state_workflow"`
	ClientWorkflow           string   `json:"client_workflow"`
	ShimQuorumSize           int      `json:"shimQuorumSize"`
	VerifyResponseQuorumSize int      `json:"verifyResponseQuorumSize"`
	ExecVerifyQuorumSize     int      `json:"execVerifyQuorumSize"`
	PhaseQuorumSize          int      `json:"phaseQuorumSize"`
	ExpectedExecVotes        int      `json:"expectedExecVotes"`
}

func loadConfig(path string) (map[string]NodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	type layeredConfig struct {
		Services map[string]json.RawMessage `json:"services"`
		Nodes    map[string]json.RawMessage `json:"nodes"`
	}

	var layered layeredConfig
	if err := json.Unmarshal(data, &layered); err != nil {
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
		nodeMap, err := decodeObject(rawNode)
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

		serviceMap, err := decodeObject(serviceRaw)
		if err != nil {
			return nil, fmt.Errorf("parse service %q: %w", serviceName, err)
		}

		merged := make(map[string]any, len(serviceMap)+len(nodeMap))
		for k, v := range serviceMap {
			merged[k] = v
		}
		for k, v := range nodeMap {
			merged[k] = v
		}

		b, err := json.Marshal(merged)
		if err != nil {
			return nil, fmt.Errorf("marshal merged config for node %q: %w", nodeName, err)
		}

		var nodeCfg NodeConfig
		if err := json.Unmarshal(b, &nodeCfg); err != nil {
			return nil, fmt.Errorf("decode merged config for node %q: %w", nodeName, err)
		}

		cfg[nodeName] = nodeCfg
	}

	return cfg, nil
}

func decodeObject(raw json.RawMessage) (map[string]any, error) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, fmt.Errorf("expected object")
	}
	return obj, nil
}
