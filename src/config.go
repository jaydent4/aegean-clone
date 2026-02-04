package main

type NodeConfig struct {
	Type      string
	Next      []string
	Clients   []string
	Shim      string
	Verifiers []string
	Peers     []string
	Execs     []string
}

var config = map[string]NodeConfig{
	"node1": {Type: "client", Next: []string{"node4", "node5", "node6"}},
	"node2": {Type: "client", Next: []string{"node4", "node5", "node6"}},
	"node3": {Type: "client", Next: []string{"node4", "node5", "node6"}},
	"node4": {Type: "shim", Next: []string{"node7"}, Clients: []string{"node1", "node2", "node3"}},
	"node5": {Type: "shim", Next: []string{"node8"}, Clients: []string{"node1", "node2", "node3"}},
	"node6": {Type: "shim", Next: []string{"node9"}, Clients: []string{"node1", "node2", "node3"}},
	"node7": {Type: "mixer", Next: []string{"node10"}, Shim: "node4"},
	"node8": {Type: "mixer", Next: []string{"node11"}, Shim: "node5"},
	"node9": {Type: "mixer", Next: []string{"node12"}, Shim: "node6"},
	"node10": {
		Type:      "exec",
		Verifiers: []string{"node13", "node14", "node15"},
		Shim:      "node4",
		Peers:     []string{"node11", "node12"},
	},
	"node11": {
		Type:      "exec",
		Verifiers: []string{"node13", "node14", "node15"},
		Shim:      "node5",
		Peers:     []string{"node10", "node12"},
	},
	"node12": {
		Type:      "exec",
		Verifiers: []string{"node13", "node14", "node15"},
		Shim:      "node6",
		Peers:     []string{"node10", "node11"},
	},
	"node13": {Type: "verifier", Execs: []string{"node10", "node11", "node12"}},
	"node14": {Type: "verifier", Execs: []string{"node10", "node11", "node12"}},
	"node15": {Type: "verifier", Execs: []string{"node10", "node11", "node12"}},
}
