package main

import (
	"context"
	"flag"
	"fmt"
	"sort"

	"aegean/nodes"
	"aegean/telemetry"
	workflow "aegean/workflow"
	"go.opentelemetry.io/otel/attribute"
)

func main() {
	stopProfiles := startProfilingFromEnv()
	installSignalCleanup(stopProfiles)
	defer stopProfiles()

	name := flag.String("name", "", "node name")
	host := flag.String("host", "", "host to bind")
	port := flag.Int("port", 0, "port to bind")
	config := flag.String("config", "", "path to run config file")
	flag.Parse()

	if *name == "" || *host == "" || *port == 0 || *config == "" {
		panic("missing required flags: --name, --host, --port, --config")
	}

	runConfig, err := loadRunConfig(*config)
	if err != nil {
		panic(err)
	}

	configs, err := loadConfig(runConfig.Architecture)
	if err != nil {
		panic(err)
	}

	cfg, ok := configs[*name]
	if !ok {
		panic(fmt.Sprintf("unknown node name: %s", *name))
	}
	nodeRunConfig := buildNodeRunConfig(runConfig.Params, cfg, *name)
	readyNodes := allNodeNamesExcept(configs, *name)
	telemetryShutdown := telemetry.Init(
		context.Background(),
		"aegean-"+cfg.Type,
		attribute.String("node.name", *name),
		attribute.String("node.type", cfg.Type),
	)
	defer telemetryShutdown(context.Background())

	var node starter
	switch cfg.Type {
	case "client":
		clientWorkflow := cfg.ClientWorkflow
		if clientWorkflow == "" {
			clientWorkflow = "default"
		}
		clientFn := workflow.ClientWorkflows[clientWorkflow]
		if clientFn == nil {
			panic(fmt.Sprintf("unknown client workflow %q for node %s", clientWorkflow, *name))
		}
		node = nodes.NewClient(*name, *host, *port, cfg.Next, readyNodes, nodeRunConfig, clientWorkflow, clientFn)
	case "server":
		execWorkflow := cfg.ExecWorkflow
		if execWorkflow == "" {
			execWorkflow = "default"
		}
		execFn := workflow.ExecWorkflows[execWorkflow]
		if execFn == nil {
			panic(fmt.Sprintf("unknown exec workflow %q for node %s", execWorkflow, *name))
		}
		initStateWorkflow := cfg.InitStateWorkflow
		if initStateWorkflow == "" {
			initStateWorkflow = "default"
		}
		initFn := workflow.InitStateWorkflows[initStateWorkflow]
		if initFn == nil {
			panic(fmt.Sprintf("unknown init state workflow %q for node %s", initStateWorkflow, *name))
		}
		node = nodes.NewServer(*name, *host, *port, cfg.Clients, cfg.Nodes, cfg.IsPrimaryBatcher, cfg.ShimQuorumSize, cfg.VerifyResponseQuorumSize, cfg.ExecVerifyQuorumSize, cfg.PhaseQuorumSize, cfg.ExpectedExecVotes, execFn, initFn, nodeRunConfig)
	case "external_service":
		serviceInitWorkflow := cfg.ExternalServiceInitState
		if serviceInitWorkflow == "" {
			serviceInitWorkflow = "default"
		}
		initFn := workflow.ExternalServiceInitWorkflows[serviceInitWorkflow]
		if initFn == nil {
			panic(fmt.Sprintf("unknown external service init workflow %q for node %s", serviceInitWorkflow, *name))
		}

		serviceWorkflow := cfg.ExternalServiceWorkflow
		if serviceWorkflow == "" {
			serviceWorkflow = "default"
		}
		serviceFn := workflow.ExternalServiceWorkflows[serviceWorkflow]
		if serviceFn == nil {
			panic(fmt.Sprintf("unknown external service workflow %q for node %s", serviceWorkflow, *name))
		}
		node = nodes.NewExternalService(*name, *host, *port, nodeRunConfig, initFn, serviceFn)
	default:
		panic(fmt.Sprintf("unrecognized node type: %s", cfg.Type))
	}

	node.Start()
}

func buildNodeRunConfig(runParams map[string]any, cfg NodeConfig, nodeName string) map[string]any {
	nodeRunConfig := make(map[string]any, len(runParams)+len(cfg.BatcherConfig)+2)
	for key, value := range runParams {
		if key == "service_overrides" {
			continue
		}
		nodeRunConfig[key] = value
	}
	for key, value := range cfg.BatcherConfig {
		nodeRunConfig[key] = value
	}
	if rawOverrides, ok := runParams["service_overrides"]; ok {
		serviceOverrides, err := asObject(rawOverrides)
		if err != nil {
			panic(fmt.Sprintf("parse service_overrides: %v", err))
		}
		if rawServiceOverrides, ok := serviceOverrides[cfg.Service]; ok {
			serviceConfig, err := asObject(rawServiceOverrides)
			if err != nil {
				panic(fmt.Sprintf("parse service_overrides.%s: %v", cfg.Service, err))
			}
			for key, value := range serviceConfig {
				nodeRunConfig[key] = value
			}
		}
	}
	nodeRunConfig["node_name"] = nodeName
	nodeRunConfig["service_name"] = cfg.Service
	return nodeRunConfig
}

type starter interface {
	Start()
}

func allNodeNamesExcept(configs map[string]NodeConfig, excludedName string) []string {
	names := make([]string, 0, len(configs))
	for name := range configs {
		if name == excludedName {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
