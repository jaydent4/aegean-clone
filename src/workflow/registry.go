package workflow

import (
	"aegean/components/exec"
	"aegean/nodes"
	aegeanworkflow "aegean/workflow/aegean"
	reqraceworkflow "aegean/workflow/req_race"
)

var ClientWorkflows = map[string]func(c *nodes.Client){
	"aegean_client":    aegeanworkflow.ClientRequestLogic,
	"aegean_oha_client": aegeanworkflow.OhaClientRequestLogic,
	"aegean_pipelined": aegeanworkflow.ClientRequestLogicPipelined,
	"req_race_client":  reqraceworkflow.ClientRequestLogic,
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
	"req_race_middle":          reqraceworkflow.ExecuteRequestMiddle,
	"req_race_backend_1":       reqraceworkflow.ExecuteRequestBackend1,
	"req_race_backend_2":       reqraceworkflow.ExecuteRequestBackend2,
	"req_race_backend_3":       reqraceworkflow.ExecuteRequestBackend3,
}
