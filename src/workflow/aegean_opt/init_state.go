package aegeanoptworkflow

import (
	"strconv"
	"strings"

	"aegean/aegean_opt/exec"
	"aegean/common"
)

func InitState(e *exec.Exec) map[string]string {
	keyCount := common.MustInt(e.RunConfig, "key_count")
	valueLength := common.MustInt(e.RunConfig, "value_length")

	initial := make(map[string]string, keyCount)
	value := strings.Repeat("x", valueLength)
	for i := 1; i <= keyCount; i++ {
		initial[strconv.Itoa(i)] = value
	}
	return initial
}
