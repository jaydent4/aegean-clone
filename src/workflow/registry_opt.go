package workflow

import (
	"aegean/aegean_opt/exec"
	aegeanoptworkflow "aegean/workflow/aegean_opt"
)

var OptExecWorkflows = map[string]exec.ExecuteRequestFunc{
	"aegean_backend":           aegeanoptworkflow.ExecuteRequestBackend,
	"aegean_backend_diverge_1": aegeanoptworkflow.ExecuteRequestBackendDivergeOneNode,
	"aegean_backend_diverge_2": aegeanoptworkflow.ExecuteRequestBackendDivergeTwoNode,
	"aegean_backend_diverge_3": aegeanoptworkflow.ExecuteRequestBackendDivergeThreeNode,
	"aegean_middle":            aegeanoptworkflow.ExecuteRequestMiddle,
	"aegean_middle_diverge_1":  aegeanoptworkflow.ExecuteRequestMiddleDivergeOneNode,
	"aegean_middle_diverge_2":  aegeanoptworkflow.ExecuteRequestMiddleDivergeTwoNode,
	"aegean_middle_diverge_3":  aegeanoptworkflow.ExecuteRequestMiddleDivergeThreeNode,
}

var OptInitStateWorkflows = map[string]exec.InitStateFunc{
	"aegean_default": aegeanoptworkflow.InitState,
	"default":        aegeanoptworkflow.InitState,
}
