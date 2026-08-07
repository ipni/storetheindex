package httpserver

import (
	"fmt"
	"mime"
	"net/http"
	"strings"
)

const (
	MediaTypeNDJson = "application/x-ndjson"
	MediaTypeJson   = "application/json"
	MediaTypeAny    = "*/*"
)

// EnableCors sets Access-Control-Allow-Origin to allow any origin.
func EnableCors(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

// AcceptsMediaType checks whether the request Accept header matches one of the
// given media types and returns the first match. When strict is false and
// Accept is empty, it accepts. On mismatch or invalid Accept, it writes
// StatusNotAcceptable and returns false.
func AcceptsMediaType(w http.ResponseWriter, r *http.Request, strict bool, mts ...string) (string, bool) {
	values := r.Header.Values("Accept")
	if len(values) == 0 {
		if !strict || len(mts) == 0 {
			return "", true
		}
	}
	mtSet := make(map[string]struct{})
	for _, accept := range values {
		amts := strings.SplitSeq(accept, ",")
		for amt := range amts {
			mt, _, err := mime.ParseMediaType(amt)
			if err != nil {
				err = fmt.Errorf("invalid accept header: %s", err)
				http.Error(w, err.Error(), http.StatusNotAcceptable)
				return "", false
			}
			mtSet[mt] = struct{}{}
		}
	}
	for _, mt := range mts {
		if _, ok := mtSet[mt]; ok {
			return mt, true
		}
	}

	err := fmt.Errorf("accept: %s", strings.Join(mts, ", "))
	http.Error(w, err.Error(), http.StatusNotAcceptable)
	return "", false
}
