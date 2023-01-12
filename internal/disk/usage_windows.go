package disk

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

var procGetDiskFreeSpaceExW = windows.NewLazySystemDLL("kernel32.dll").NewProc("GetDiskFreeSpaceExW")

func usage(path string) (*UsageStats, error) {
	var (
		freeBytesAvailable int64
		totalBytes         int64
		totalFreeBytes     int64
	)
	diskret, _, err := procGetDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(windows.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalBytes)),
		uintptr(unsafe.Pointer(&totalFreeBytes)))
	// The returned error is always non-nil. Callers must inspect the primary
	// return value to decide whether an error occurred. See:
	// https://pkg.go.dev/golang.org/x/sys/windows#LazyProc.Call
	if diskret == 0 {
		return nil, err
	}

	used := uint64(totalBytes) - uint64(totalFreeBytes)
	return &UsageStats{
		Path:    path,
		Percent: percentUsed(used, uint64(totalBytes)),
		Free:    uint64(totalFreeBytes),
		Total:   uint64(totalBytes),
		Used:    used,
	}, nil
}
