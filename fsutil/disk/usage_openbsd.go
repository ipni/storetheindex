//go:build openbsd

package disk

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func usage(path string) (*UsageStats, error) {
	var stat syscall.Statfs_t
	err := unix.Statfs(path, &stat)
	if err != nil {
		return nil, err
	}
	blockSize := stat.F_bsize

	// Total blocks only available to root.
	total := stat.F_blocks
	// Remaining free blocks usable by root.
	availToRoot := stat.F_bfree
	// Remaining free blocks usable by user.
	availToUser := stat.F_bavail
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
