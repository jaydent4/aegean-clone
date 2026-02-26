package workflow

import (
	"aegean/components/exec"
	"aegean/nodes"
	aegeanworkflow "aegean/workflow/aegean"
	noseexternalsrvworkflow "aegean/workflow/no_se_external_srv"
	reqraceworkflow "aegean/workflow/req_race"
)

var ClientWorkflows = map[string]func(c *nodes.Client){
	"aegean_client":                 aegeanworkflow.ClientRequestLogic,
	"aegean_oha_client":             aegeanworkflow.OhaClientRequestLogic,
	"aegean_pipelined":              aegeanworkflow.ClientRequestLogicPipelined,
	"no_se_external_srv_oha_client": noseexternalsrvworkflow.OhaClientRequestLogic,
	"req_race_client":               reqraceworkflow.ClientRequestLogic,
	"req_race_oha_client":           reqraceworkflow.OhaClientRequestLogic,
}

var ExecWorkflows = map[string]exec.ExecuteRequestFunc{
	"aegean_backend":            aegeanworkflow.ExecuteRequestBackend,
	"aegean_backend_diverge_1":  aegeanworkflow.ExecuteRequestBackendDivergeOneNode,
	"aegean_backend_diverge_2":  aegeanworkflow.ExecuteRequestBackendDivergeTwoNode,
	"aegean_backend_diverge_3":  aegeanworkflow.ExecuteRequestBackendDivergeThreeNode,
	"aegean_middle":             aegeanworkflow.ExecuteRequestMiddle,
	"aegean_middle_diverge_1":   aegeanworkflow.ExecuteRequestMiddleDivergeOneNode,
	"aegean_middle_diverge_2":   aegeanworkflow.ExecuteRequestMiddleDivergeTwoNode,
	"aegean_middle_diverge_3":   aegeanworkflow.ExecuteRequestMiddleDivergeThreeNode,
	"no_se_external_srv_server": noseexternalsrvworkflow.ExecuteRequestServer,
	"req_race_middle":           reqraceworkflow.ExecuteRequestMiddle,
	"req_race_backend_1":        reqraceworkflow.ExecuteRequestBackend1,
	"req_race_backend_2":        reqraceworkflow.ExecuteRequestBackend2,
	"req_race_backend_3":        reqraceworkflow.ExecuteRequestBackend3,
}

var InitStateWorkflows = map[string]exec.InitStateFunc{
	"default":                    aegeanworkflow.InitState,
	"aegean_default":             aegeanworkflow.InitState,
	"no_se_external_srv_default": noseexternalsrvworkflow.InitState,
	"req_race_default":           reqraceworkflow.InitState,
}
