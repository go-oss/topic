package topic

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	redis "gopkg.in/redis.v5"
)

// txError for redis transaction error formatter.
func txError(cmds []redis.Cmder) error {
	debugLogs := make([]string, 0, len(cmds))
	for i, cmd := range cmds {
		debugLogs = append(debugLogs, fmt.Sprintf("error[%d]:%+v", i, cmd.Err()))
	}
	return errors.New(strings.Join(debugLogs, ", "))
}
