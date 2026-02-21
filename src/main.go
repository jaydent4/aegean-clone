package main

import (
	"flag"
	"fmt"

	"aegean/nodes"
	workflow "aegean/workflow"
)

func main() {
	name := flag.String("name", "", "node name")
	host := flag.String("host", "", "host to bind")
	port := flag.Int("port", 0, "port to bind")
	config := flag.String("config", "", "path to architecture config file")
	flag.Parse()

	if *name == "" || *host == "" || *port == 0 || *config == "" {
		panic("missing required flags: --name, --host, --port, --config")
	}

	configs, err := loadConfig(*config)
	if err != nil {
		panic(err)
	}

	cfg, ok := configs[*name]
	if !ok {
		panic(fmt.Sprintf("unknown node name: %s", *name))
	}

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
		node = nodes.NewClient(*name, *host, *port, cfg.Next, clientFn)
	case "oha_client":
		clientWorkflow := cfg.ClientWorkflow
		if clientWorkflow == "" {
			clientWorkflow = "default"
		}
		clientFn := workflow.ClientWorkflows[clientWorkflow]
		if clientFn == nil {
			panic(fmt.Sprintf("unknown client workflow %q for node %s", clientWorkflow, *name))
		}
		node = nodes.NewOHAClient(*name, *host, *port, cfg.Next, clientFn)
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
		node = nodes.NewServer(*name, *host, *port, cfg.Clients, cfg.Nodes, cfg.IsPrimaryBatcher, cfg.ShimQuorumSize, cfg.VerifyResponseQuorumSize, cfg.ExecVerifyQuorumSize, cfg.PhaseQuorumSize, cfg.ExpectedExecVotes, execFn, initFn)
	default:
		panic(fmt.Sprintf("unrecognized node type: %s", cfg.Type))
	}

	node.Start()
}

type starter interface {
	Start()
}
