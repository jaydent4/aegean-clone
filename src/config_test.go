package main

import (
	"os"
	"path/filepath"
	"testing"

	"aegean/common"
)

func TestLoadRunConfigResolvesArchitectureSiblingToRunsDir(t *testing.T) {
	root := t.TempDir()
	architectureDir := filepath.Join(root, "experiment", "architecture")
	runDir := filepath.Join(root, "experiment", "runs", "basic_oha_large_req")

	if err := os.MkdirAll(architectureDir, 0o755); err != nil {
		t.Fatalf("mkdir architecture dir: %v", err)
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}

	architecturePath := filepath.Join(architectureDir, "basic_oha.yaml")
	if err := os.WriteFile(architecturePath, []byte("services:\n  svc:\n    type: client\nnodes:\n  node0:\n    service: svc\n"), 0o644); err != nil {
		t.Fatalf("write architecture config: %v", err)
	}

	runConfigPath := filepath.Join(runDir, "worker_4.yaml")
	if err := os.WriteFile(runConfigPath, []byte("architecture: basic_oha.yaml\nworker_count: 4\nrun_timeout_seconds: 30\n"), 0o644); err != nil {
		t.Fatalf("write run config: %v", err)
	}

	cfg, err := loadRunConfig(runConfigPath)
	if err != nil {
		t.Fatalf("load run config: %v", err)
	}

	if cfg.Architecture != architecturePath {
		t.Fatalf("architecture path = %q, want %q", cfg.Architecture, architecturePath)
	}
	if got := cfg.Params["worker_count"]; got != 4 {
		t.Fatalf("worker_count = %#v, want 4", got)
	}
	if got := cfg.Params["run_timeout_seconds"]; got != 30 {
		t.Fatalf("run_timeout_seconds = %#v, want 30", got)
	}
}

func TestLoadConfigIncludesServiceRunConfigOverrides(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "architecture.yaml")
	configBody := `
services:
  backend:
    type: server
    clients: [node0]
    nodes: [node4]
    exec_workflow: aegean_backend
    init_state_workflow: aegean_default
    shimQuorumSize: 1
    verifyResponseQuorumSize: 1
    execVerifyQuorumSize: 1
    phaseQuorumSize: 1
    expectedExecVotes: 1
    batcher_config:
      batch_timeout_ms: 11
      batch_size: 17
nodes:
  node4:
    service: backend
    is_primary_batcher: true
`
	if err := os.WriteFile(configPath, []byte(configBody), 0o644); err != nil {
		t.Fatalf("write architecture config: %v", err)
	}

	cfgs, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	nodeCfg := cfgs["node4"]
	if got := common.MustInt(nodeCfg.BatcherConfig, "batch_timeout_ms"); got != 11 {
		t.Fatalf("batch_timeout_ms = %d, want 11", got)
	}
	if got := common.MustInt(nodeCfg.BatcherConfig, "batch_size"); got != 17 {
		t.Fatalf("batch_size = %d, want 17", got)
	}
}

func TestBuildNodeRunConfigAppliesServiceOverrides(t *testing.T) {
	runParams := map[string]any{
		"batch_size":       40,
		"batch_timeout_ms": 20,
		"service_overrides": map[string]any{
			"backend": map[string]any{
				"batch_timeout_ms": 10,
				"batch_size":       20,
			},
		},
	}
	cfg := NodeConfig{
		Service: "backend",
		BatcherConfig: map[string]any{
			"batch_timeout_ms": 15,
		},
	}

	merged := buildNodeRunConfig(runParams, cfg, "node4")

	if got := merged["batch_size"]; got != 20 {
		t.Fatalf("batch_size = %#v, want 20", got)
	}
	if got := merged["batch_timeout_ms"]; got != 10 {
		t.Fatalf("batch_timeout_ms = %#v, want 10", got)
	}
	if got := merged["service_name"]; got != "backend" {
		t.Fatalf("service_name = %#v, want backend", got)
	}
	if _, exists := merged["service_overrides"]; exists {
		t.Fatalf("service_overrides should not be copied into node run config")
	}
}
