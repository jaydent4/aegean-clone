package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type NodeConfig struct {
	Type             string   `json:"type"`
	Next             []string `json:"next"`
	Clients          []string `json:"clients"`
	Shim             string   `json:"shim"`
	Verifiers        []string `json:"verifiers"`
	Peers            []string `json:"peers"`
	Execs            []string `json:"execs"`
	IsPrimaryBatcher bool     `json:"is_primary_batcher"`
	ExecWorkflow     string   `json:"exec_workflow"`
	ClientWorkflow   string   `json:"client_workflow"`
	ResponseWorkflow string   `json:"response_workflow"`
}

const configPath = "../experiment/architecture/aegean.json"

func loadConfig(path string) (map[string]NodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var cfg map[string]NodeConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	return cfg, nil
}
