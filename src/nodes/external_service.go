package nodes

import "aegean/common"

type ExternalService struct {
	*Node
	InitState    func(es *ExternalService)
	ServiceLogic func(es *ExternalService, payload map[string]any) map[string]any
	RunConfig    map[string]any
}

func NewExternalService(
	name, host string,
	port int,
	runConfig map[string]any,
	initState func(es *ExternalService),
	serviceLogic func(es *ExternalService, payload map[string]any) map[string]any,
) *ExternalService {

	service := &ExternalService{
		Node:         NewNode(name, host, port),
		InitState:    initState,
		ServiceLogic: serviceLogic,
		RunConfig:    runConfig,
	}
	service.Node.EnablePprof = common.BoolOrDefault(runConfig, "pprof_enabled", true)
	service.Node.BlockProfileRate = common.IntOrDefault(runConfig, "pprof_block_profile_rate", 1)
	service.Node.MutexProfileFraction = common.IntOrDefault(runConfig, "pprof_mutex_profile_fraction", 1)
	service.InitState(service)

	service.Node.HandleMessage = service.HandleMessage
	service.Node.HandleReady = service.HandleReady
	return service
}

func (s *ExternalService) Start() {
	s.Node.Start()
}

func (s *ExternalService) HandleMessage(payload map[string]any) map[string]any {
	return s.ServiceLogic(s, payload)
}

func (s *ExternalService) HandleReady(payload map[string]any) map[string]any {
	return map[string]any{
		"ready": true,
	}
}
