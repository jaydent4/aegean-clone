package nodes

import (
	"aegean/common"
	"aegean/components/eo"
	"aegean/components/pbeo"
	netx "aegean/net"
	"runtime"
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

// PBEOServer runs a passive-replication service. Only the current Raft leader
// executes application requests; all replicas apply committed update records.
type PBEOServer struct {
	*Node
	PBEO *pbeo.PBEO
}

func NewPBEOServer(
	name, host string,
	port int,
	clients []string,
	nodes []string,
	executeRequest pbeo.ExecuteRequestFunc,
	initStateFn pbeo.InitStateFunc,
	runConfig map[string]any,
) *PBEOServer {
	runtime.GOMAXPROCS(common.MustInt(runConfig, "gomaxprocs"))

	server := &PBEOServer{
		Node: NewNode(name, host, port),
	}

	component, err := pbeo.NewPBEO(pbeo.Config{
		Name:      name,
		Peers:     nodes,
		Clients:   clients,
		Execute:   executeRequest,
		InitState: initStateFn,
		RunConfig: runConfig,
		Send: func(peer string, payload map[string]any) error {
			_, err := netx.SendMessage(peer, 8000, payload)
			return err
		},
		SendRaft: func(peer string, message raftpb.Message) error {
			payload, err := eo.EncodeRaftMessage(message)
			if err != nil {
				return err
			}
			_, err = netx.SendMessage(peer, 8000, payload)
			return err
		},
		TickInterval:  time.Duration(common.IntOrDefault(runConfig, "pbeo_tick_interval_ms", 10)) * time.Millisecond,
		ElectionTick:  common.IntOrDefault(runConfig, "pbeo_election_tick", 10),
		HeartbeatTick: common.IntOrDefault(runConfig, "pbeo_heartbeat_tick", 1),
	})
	if err != nil {
		panic(err)
	}
	server.PBEO = component
	server.Node.HandleMessage = server.HandleMessage
	server.Node.HandleReady = server.HandleReady
	return server
}

func (s *PBEOServer) Start() {
	s.Node.Start()
}

func (s *PBEOServer) HandleMessage(payload map[string]any) map[string]any {
	return s.PBEO.HandleMessage(payload)
}

func (s *PBEOServer) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{
		"ready": s.PBEO.Ready(),
	}
}
