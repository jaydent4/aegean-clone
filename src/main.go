package main

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"aegean/nodes"
	"aegean/telemetry"
	workflow "aegean/workflow"
	"go.opentelemetry.io/otel/attribute"
)

func main() {
	configureLoggingFromEnv()

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
	nodeRunConfig := buildNodeRunConfig(runConfig.Params, cfg, *name, configs)
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
	case "pbeo_server":
		execWorkflow := cfg.ExecWorkflow
		if execWorkflow == "" {
			execWorkflow = "default"
		}
		execFn := workflow.PBEOExecWorkflows[execWorkflow]
		if execFn == nil {
			panic(fmt.Sprintf("unknown pbeo exec workflow %q for node %s", execWorkflow, *name))
		}
		initStateWorkflow := cfg.InitStateWorkflow
		if initStateWorkflow == "" {
			initStateWorkflow = "default"
		}
		initFn := workflow.PBEOInitStateWorkflows[initStateWorkflow]
		if initFn == nil {
			panic(fmt.Sprintf("unknown pbeo init state workflow %q for node %s", initStateWorkflow, *name))
		}
		node = nodes.NewPBEOServer(*name, *host, *port, cfg.Clients, cfg.Nodes, execFn, initFn, nodeRunConfig)
	case "unreplicated_server":
		execWorkflow := cfg.ExecWorkflow
		if execWorkflow == "" {
			execWorkflow = "default"
		}
		execFn := workflow.UnreplicatedWorkflows[execWorkflow]
		if execFn == nil {
			panic(fmt.Sprintf("unknown exec workflow %q for node %s", execWorkflow, *name))
		}
		initStateWorkflow := cfg.InitStateWorkflow
		if initStateWorkflow == "" {
			initStateWorkflow = "default"
		}
		initFn := workflow.UnreplicatedInitStateWorkflows[initStateWorkflow]
		if initFn == nil {
			panic(fmt.Sprintf("unknown init state workflow %q for node %s", initStateWorkflow, *name))
		}
		node = nodes.NewUnreplicatedServer(*name, *host, *port, cfg.Clients, execFn, initFn, nodeRunConfig)
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

func buildNodeRunConfig(runParams map[string]any, cfg NodeConfig, nodeName string, configs map[string]NodeConfig) map[string]any {
	nodeRunConfig := make(map[string]any, len(cfg.RunConfig)+len(runParams)+len(cfg.BatcherConfig)+3)
	for key, value := range cfg.RunConfig {
		nodeRunConfig[key] = value
	}
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
	nodeRunConfig["service_nodes"] = buildServiceNodeMap(configs)
	return nodeRunConfig
}

func buildServiceNodeMap(configs map[string]NodeConfig) map[string]any {
	serviceNodes := map[string][]string{}
	for nodeName, cfg := range configs {
		if cfg.Service == "" {
			continue
		}
		if len(cfg.Nodes) > 0 {
			serviceNodes[cfg.Service] = append([]string{}, cfg.Nodes...)
			continue
		}
		serviceNodes[cfg.Service] = append(serviceNodes[cfg.Service], nodeName)
	}

	out := make(map[string]any, len(serviceNodes))
	for serviceName, nodes := range serviceNodes {
		if len(nodes) > 1 {
			sortNodeNames(nodes)
		}
		out[serviceName] = append([]string{}, nodes...)
	}
	return out
}

func sortNodeNames(nodes []string) {
	sort.Slice(nodes, func(i, j int) bool {
		left, leftOK := nodeNumber(nodes[i])
		right, rightOK := nodeNumber(nodes[j])
		if leftOK && rightOK {
			return left < right
		}
		return nodes[i] < nodes[j]
	})
}

func nodeNumber(nodeName string) (int, bool) {
	if !strings.HasPrefix(nodeName, "node") {
		return 0, false
	}
	value, err := strconv.Atoi(strings.TrimPrefix(nodeName, "node"))
	return value, err == nil
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
