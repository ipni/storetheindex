//go:build freebsd || linux || darwin || (aix && !cgo)
// +build freebsd linux darwin aix,!cgo

package disk

import (
	"golang.org/x/sys/unix"
)

func usage(path string) (*UsageStats, error) {
	var stat unix.Statfs_t
	err := unix.Statfs(path, &stat)
	if err != nil {
		return nil, err
	}
	blockSize := uint64(stat.Bsize)

	// Total blocks only available to root.
	total := uint64(stat.Blocks)
	// Remaining free blocks usable by root.
	availToRoot := uint64(stat.Bfree)
	// Remaining free blocks usable by user.
	availToUser := uint64(stat.Bavail)
	// Total blocks being used.
	used := total - availToRoot
	// Total blocks available to user.
	totalUser := used + availToUser

	return &UsageStats{
		Path:    path,
		Percent: percentUsed(used, totalUser),
		Free:    availToUser * blockSize,
		Total:   total * blockSize,
		Used:    used * blockSize,
	}, nil
}
