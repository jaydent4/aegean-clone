package main

import (
	"flag"
	"log"
	"os"

	"aegean/nodes"
	workflow "aegean/workflow"
)

func main() {
	log.SetFlags(log.LstdFlags)
	// Force log writes to be flushed immediately, even when the process is killed
	log.SetOutput(syncWriter{w: os.Stderr})

	name := flag.String("name", "", "node name")
	host := flag.String("host", "", "host to bind")
	port := flag.Int("port", 0, "port to bind")
	config := flag.String("config", "", "path to architecture config file")
	flag.Parse()

	if *name == "" || *host == "" || *port == 0 || *config == "" {
		log.Fatal("missing required flags: --name, --host, --port, --config")
	}

	configs, err := loadConfig(*config)
	if err != nil {
		log.Fatal(err)
	}

	cfg, ok := configs[*name]
	if !ok {
		log.Fatalf("unknown node name: %s", *name)
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
			log.Fatalf("unknown client workflow %q for node %s", clientWorkflow, *name)
		}
		node = nodes.NewClient(*name, *host, *port, cfg.Next, clientFn)
	case "server":
		execWorkflow := cfg.ExecWorkflow
		if execWorkflow == "" {
			execWorkflow = "default"
		}
		execFn := workflow.ExecWorkflows[execWorkflow]
		if execFn == nil {
			log.Fatalf("unknown exec workflow %q for node %s", execWorkflow, *name)
		}
		node = nodes.NewServer(*name, *host, *port, cfg.Clients, cfg.Nodes, cfg.IsPrimaryBatcher, cfg.ShimQuorumSize, cfg.VerifyResponseQuorumSize, cfg.ExecVerifyQuorumSize, cfg.PhaseQuorumSize, cfg.ExpectedExecVotes, execFn)
	default:
		log.Fatalf("unrecognized node type: %s", cfg.Type)
	}

	node.Start()
}

type starter interface {
	Start()
}

type syncWriter struct {
	w *os.File
}

func (s syncWriter) Write(p []byte) (int, error) {
	n, err := s.w.Write(p)
	_ = s.w.Sync()
	return n, err
}
