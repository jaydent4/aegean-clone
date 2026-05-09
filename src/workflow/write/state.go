package writeworkflow

import (
	"fmt"
	"strings"

	"aegean/aegean/exec"
	"aegean/common"
)

func InitState(e *exec.Exec) map[string]string {
	_ = e
	return map[string]string{}
}

func writeConfiguredValue(e *exec.Exec, requestID any, service string) {
	valueLength := common.MustInt(e.RunConfig, "write_value_size_bytes")
	value := strings.Repeat("x", valueLength)
	e.WriteKV(fmt.Sprintf("%s:%v", service, requestID), value)
}
