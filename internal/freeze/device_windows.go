package freeze

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

var devNum int
var volNums = map[string]int{}

var procGetVolumePathNameW = windows.NewLazySystemDLL("kernel32.dll").NewProc("GetVolumePathNameW")

func deviceNumber(path string) (int, error) {
	_, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("cannot stat %q: %w", path, err)
	}
	buf := make([]uint16, 200)
	r1, _, _ := procGetVolumePathNameW.Call(
		uintptr(unsafe.Pointer(windows.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(len(buf)),
	)
	if r1 == 0 {
		return -1, nil
	}
	vol := syscall.UTF16ToString(buf)
	num, ok := volNums[vol]
	if ok {
		return num, nil
	}
	devNum++
	volNums[vol] = devNum
	return devNum, nil
}
