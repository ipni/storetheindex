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
	blockSize := stat.Bsize

	// Total blocks only available to root.
	total := stat.Blocks
	// Remaining free blocks usable by root.
	availToRoot := stat.Bfree
	// Remaining free blocks usable by user.
	availToUser := stat.Bavail
	// Total blocks being used.
	used := total - availToRoot
	// Total blocks available to user.
	totalUser := used + availToUser

	return &UsageStats{
		Path:    path,
		Percent: percentUsed(used, totalUser),
		Free:    availToUser * uint64(blockSize),
		Total:   total * uint64(blockSize),
		Used:    used * uint64(blockSize),
	}, nil
}
