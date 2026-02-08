package aegeanworkflow

import (
	"aegean/components/exec"
	"aegean/nodes"
)

var ClientWorkflows = map[string]func(c *nodes.Client){}
var ExecWorkflows = map[string]exec.ExecuteRequestFunc{}
var ResponseWorkflows = map[string]exec.ExecuteResponseFunc{}

func init() {
	ClientWorkflows["default"] = ClientRequestLogic
	ExecWorkflows["default"] = ExecuteRequest
	ExecWorkflows["fanout"] = ExecuteRequestFanout
	ResponseWorkflows["default"] = ResponseForwardToClients
	ResponseWorkflows["forward_to_clients"] = ResponseForwardToClients
	ResponseWorkflows["noop"] = ResponseNoop
}
