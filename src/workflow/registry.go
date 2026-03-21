package workflow

import (
	"aegean/components/exec"
	"aegean/nodes"
	aegeanworkflow "aegean/workflow/aegean"
	externalsrvworkflow "aegean/workflow/external_srv"
	reqraceworkflow "aegean/workflow/req_race"
	supersimpleworkflow "aegean/workflow/supersimple"
)

var ClientWorkflows = map[string]func(c *nodes.Client){
	"aegean_client":           aegeanworkflow.ClientRequestLogic,
	"aegean_k6_client":        aegeanworkflow.K6ClientRequestLogic,
	"aegean_oha_client":       aegeanworkflow.OhaClientRequestLogic,
	"aegean_pipelined":        aegeanworkflow.ClientRequestLogicPipelined,
	"external_srv_oha_client": externalsrvworkflow.OhaClientRequestLogic,
	"req_race_client":         reqraceworkflow.ClientRequestLogic,
	"req_race_oha_client":     reqraceworkflow.OhaClientRequestLogic,
	"supersimple_oha_client":  supersimpleworkflow.OhaClientRequestLogic,
}

var ExecWorkflows = map[string]exec.ExecuteRequestFunc{
	"aegean_backend":           aegeanworkflow.ExecuteRequestBackend,
	"aegean_backend_diverge_1": aegeanworkflow.ExecuteRequestBackendDivergeOneNode,
	"aegean_backend_diverge_2": aegeanworkflow.ExecuteRequestBackendDivergeTwoNode,
	"aegean_backend_diverge_3": aegeanworkflow.ExecuteRequestBackendDivergeThreeNode,
	"aegean_middle":            aegeanworkflow.ExecuteRequestMiddle,
	"aegean_middle_diverge_1":  aegeanworkflow.ExecuteRequestMiddleDivergeOneNode,
	"aegean_middle_diverge_2":  aegeanworkflow.ExecuteRequestMiddleDivergeTwoNode,
	"aegean_middle_diverge_3":  aegeanworkflow.ExecuteRequestMiddleDivergeThreeNode,
	"external_srv_server":      externalsrvworkflow.ExecuteRequestServer,
	"req_race_middle":          reqraceworkflow.ExecuteRequestMiddle,
	"req_race_backend_1":       reqraceworkflow.ExecuteRequestBackend1,
	"req_race_backend_2":       reqraceworkflow.ExecuteRequestBackend2,
	"req_race_backend_3":       reqraceworkflow.ExecuteRequestBackend3,
	"supersimple_middle":       supersimpleworkflow.ExecuteRequestMiddle,
	"supersimple_backend":      supersimpleworkflow.ExecuteRequestServer,
}

var InitStateWorkflows = map[string]exec.InitStateFunc{
	"default":              aegeanworkflow.InitState,
	"aegean_default":       aegeanworkflow.InitState,
	"external_srv_default": externalsrvworkflow.InitState,
	"req_race_default":     reqraceworkflow.InitState,
	"supersimple_default":  supersimpleworkflow.InitState,
}

var ExternalServiceInitWorkflows = map[string]func(es *nodes.ExternalService){
	"default":              externalsrvworkflow.InitExternalService,
	"external_srv_default": externalsrvworkflow.InitExternalService,
}

var ExternalServiceWorkflows = map[string]func(es *nodes.ExternalService, payload map[string]any) map[string]any{
	"external_srv_external_service": externalsrvworkflow.ExternalServiceLogic,
}
