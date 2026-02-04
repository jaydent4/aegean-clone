// Package main is the entry point for the aegean application.
// Translates: src_py/start.py
package main

import (
	"flag"
	"log"
	"os"

	"aegean/nodes"
)

func main() {
	log.SetFlags(log.LstdFlags)
	// Force log writes to be flushed immediately, even when the process is killed.
	log.SetOutput(syncWriter{w: os.Stderr})

	name := flag.String("name", "", "node name")
	host := flag.String("host", "", "host to bind")
	port := flag.Int("port", 0, "port to bind")
	flag.Parse()

	if *name == "" || *host == "" || *port == 0 {
		log.Fatal("missing required flags: --name, --host, --port")
	}

	cfg, ok := config[*name]
	if !ok {
		log.Fatalf("unknown node name: %s", *name)
	}

	var node starter
	switch cfg.Type {
	case "client":
		node = nodes.NewClient(*name, *host, *port, cfg.Next)
	case "shim":
		node = nodes.NewShim(*name, *host, *port, first(cfg.Next), cfg.Clients)
	case "mixer":
		node = nodes.NewMixer(*name, *host, *port, first(cfg.Next), cfg.Shim)
	case "exec":
		node = nodes.NewExec(*name, *host, *port, cfg.Verifiers, cfg.Shim, cfg.Peers)
	case "verifier":
		node = nodes.NewVerifier(*name, *host, *port, cfg.Execs)
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

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
