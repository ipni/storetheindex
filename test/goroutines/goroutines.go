// Package goroutines provides assertions about leaked goroutines in tests.
package goroutines

import (
	"bytes"
	"runtime"
	"testing"
	"time"
)

const (
	defaultTimeout = 5 * time.Second
	pollInterval   = 10 * time.Millisecond
	// A goroutine is not always visible in the stack dump immediately after it
	// is created, so a count of zero is only trusted once it holds for this
	// many consecutive samples.
	settleSamples = 5
)

// RequireNone waits for all goroutines whose stack contains fnName to exit, and
// fails t if any are still running. Use the package-qualified function name,
// such as "carstore.readEntries".
func RequireNone(t *testing.T, fnName string) {
	t.Helper()

	var count, zeros int
	for deadline := time.Now().Add(defaultTimeout); ; {
		count = Count(fnName)
		if count == 0 {
			zeros++
			if zeros == settleSamples {
				return
			}
		} else {
			zeros = 0
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("%d goroutine(s) running %s after %s", count, fnName, defaultTimeout)
}

// Count returns the number of goroutine stacks containing fnName.
func Count(fnName string) int {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return bytes.Count(buf[:n], []byte(fnName))
		}
		buf = make([]byte, 2*len(buf))
	}
}
