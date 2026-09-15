package disk_test

import (
	"testing"

	"github.com/ipni/storetheindex/fsutil/disk"
	"github.com/stretchr/testify/require"
)

// Smoke-test the public shim. Behavior is covered in internal/fsutil/disk.
func TestShimUsage(t *testing.T) {
	us, err := disk.Usage(t.TempDir())
	require.NoError(t, err)
	require.NotNil(t, us)
}
