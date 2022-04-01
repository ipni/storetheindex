// Package pprof is separated out from metrics to isolate the 'init'
// functionality of pprof, so thatit is included when used by binaries, but not
// if other packages get used or integrated into clients that don't expect the
// pprof side effect to have taken effect.
package pprof

import (
	"net/http"
	"runtime"

	pprof "net/http/pprof" // adds default pprof endpoint at /debug/pprof
)

// WithProfile provides an http mux setup to serve pprof endpoints. it should
// be mounted at /debug/pprof.
func WithProfile() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/pprof/gc", func(w http.ResponseWriter, req *http.Request) {
		runtime.GC()
	})

	return mux
}
