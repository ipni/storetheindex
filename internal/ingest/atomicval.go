package ingest

import (
	"sync/atomic"
	"unsafe"
)

type atomicVal struct {
	wrapper *unsafe.Pointer
}

func newAtomicVal(val interface{}) *atomicVal {
	ptr := unsafe.Pointer(&val)
	return &atomicVal{
		wrapper: &ptr,
	}
}

// Swap does what atomic.Value.Swap does, but supports go.16.
// TODO delete this when we drop go.16 support.
func (a *atomicVal) swap(new interface{}) interface{} {
	newPtr := unsafe.Pointer(&new)
	oldPtr := atomic.SwapPointer(a.wrapper, newPtr)
	if oldPtr == nil {
		return nil
	}

	return *(*interface{})(oldPtr)
}
