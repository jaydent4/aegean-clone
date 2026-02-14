package workflow

import (
	"aegean/components/exec"
	"aegean/nodes"
	basicrunworkflow "aegean/workflow/basic_run"
	reqraceworkflow "aegean/workflow/req_race"
)

var ClientWorkflows = map[string]func(c *nodes.Client){
	"basic_run_client":    basicrunworkflow.ClientRequestLogic,
	"basic_run_pipelined": basicrunworkflow.ClientRequestLogicPipelined,
	"req_race_client":     reqraceworkflow.ClientRequestLogic,
}

var ExecWorkflows = map[string]exec.ExecuteRequestFunc{
	"basic_run_backend":           basicrunworkflow.ExecuteRequestBackend,
	"basic_run_backend_diverge_1": basicrunworkflow.ExecuteRequestBackendDivergeOneNode,
	"basic_run_backend_diverge_2": basicrunworkflow.ExecuteRequestBackendDivergeTwoNode,
	"basic_run_backend_diverge_3": basicrunworkflow.ExecuteRequestBackendDivergeThreeNode,
	"basic_run_middle":            basicrunworkflow.ExecuteRequestMiddle,
	"basic_run_middle_diverge_1":  basicrunworkflow.ExecuteRequestMiddleDivergeOneNode,
	"basic_run_middle_diverge_2":  basicrunworkflow.ExecuteRequestMiddleDivergeTwoNode,
	"basic_run_middle_diverge_3":  basicrunworkflow.ExecuteRequestMiddleDivergeThreeNode,
	"req_race_middle":             reqraceworkflow.ExecuteRequestMiddle,
	"req_race_backend_1":          reqraceworkflow.ExecuteRequestBackend1,
	"req_race_backend_2":          reqraceworkflow.ExecuteRequestBackend2,
}
