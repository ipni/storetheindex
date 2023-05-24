//go:build freebsd || linux || darwin || openbsd || (aix && !cgo)

package freeze

import (
	"fmt"
	"os"
	"syscall"
)

func deviceNumber(path string) (int, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("cannot stat %q: %w", path, err)
	}
	sysStatAny := fi.Sys()
	if sysStatAny == nil {
		return -1, nil
	}
	sysStat, ok := sysStatAny.(*syscall.Stat_t)
	if !ok {
		return -1, nil
	}
	return int(sysStat.Dev), nil
}
