package revision

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFieldsIncludesVersion(t *testing.T) {
	kvs := fields("v0.13.0 test")
	require.GreaterOrEqual(t, len(kvs), 2)
	require.Equal(t, "version", kvs[0])
	require.Equal(t, "v0.13.0 test", kvs[1])

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	require.Contains(t, kvs, "go")
	require.Contains(t, kvs, info.GoVersion)
}

type captureLog struct {
	msg string
	kvs []any
}

func (c *captureLog) Infow(msg string, keysAndValues ...any) {
	c.msg = msg
	c.kvs = keysAndValues
}

func TestLogStarting(t *testing.T) {
	var c captureLog
	Log(&c, "v1")
	require.Equal(t, "starting", c.msg)
	require.Equal(t, "version", c.kvs[0])
	require.Equal(t, "v1", c.kvs[1])
}
