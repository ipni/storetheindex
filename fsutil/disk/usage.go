// A wrapper around the internal fsutil/disk package,
// kept for go mod backwards compatibility.
package disk

import "github.com/ipni/storetheindex/internal/fsutil/disk"

type UsageStats = disk.UsageStats

func Usage(path string) (*UsageStats, error) { return disk.Usage(path) }
