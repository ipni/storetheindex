package disk

type UsageStats struct {
	// Path is the filesystem path that disk usage is associated with.
	Path string
	// Percent is the user usage percent compared to the total amount of space
	// the user can use. This number would be higher if compared to root's
	// because the user has less space (around 5%).
	Percent float64
	// Free is the remaining free space usable by the user.
	Free uint64
	// Total is the space only available to root.
	Total uint64
	// Used is the total space being used.
	Used uint64
}

// DiskUsage returns disk usage associated with path.
//
// About 5% of disk space is usually accessible by root but not accessible by
// user. The values of UsageStats.Total and UsageStats.Used are the total and
// used disk space, whereas UsageStats.Free and UsageStats.Percent are user
// view of disk space.
func Usage(path string) (*UsageStats, error) {
	// Call OS-specific function.
	return usage(path)
}

func percentUsed(used, total uint64) float64 {
	if total == 0 {
		return 0.0
	}
	return float64(used) / float64(total)
}
