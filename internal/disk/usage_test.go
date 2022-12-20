package disk_test

import (
	"testing"

	"github.com/ipni/storetheindex/internal/disk"
	"github.com/stretchr/testify/require"
)

func TestUsage(t *testing.T) {
	tempDir := t.TempDir()
	us, err := disk.Usage(tempDir)
	require.NoError(t, err)

	t.Log("Path:", us.Path)
	require.Equal(t, tempDir, us.Path)

	t.Log("Total:", us.Total)
	require.NotZero(t, us.Total)

	t.Log("Free:", us.Free)
	require.NotZero(t, us.Free)

	t.Log("Used:", us.Used)
	require.NotZero(t, us.Used)

	t.Log("Percent:", us.Percent)
	require.Greater(t, us.Percent, 0.0)
}
