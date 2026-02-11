package find

import (
	"fmt"
	"mime"
	"net/http"
	"strings"
)

const (
	mediaTypeNDJson = "application/x-ndjson"
	mediaTypeJson   = "application/json"
	mediaTypeAny    = "*/*"
)

func acceptsAnyOf(w http.ResponseWriter, r *http.Request, strict bool, mts ...string) (string, bool) {
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
