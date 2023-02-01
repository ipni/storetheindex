package httpfinderserver

import (
	"mime"
	"net/http"
	"strings"
)

const (
	mediaTypeNDJson = "application/x-ndjson"
	mediaTypeJson   = "application/json"
	mediaTypeAny    = "*/*"
)

func explicitlyAcceptsNDJson(r *http.Request) (bool, error) {
	return acceptsAnyOf(r, mediaTypeNDJson)
}

func acceptsAnyOf(r *http.Request, mts ...string) (bool, error) {
	for _, accept := range r.Header.Values("Accept") {
		amts := strings.Split(accept, ",")
		for _, amt := range amts {
			mt, _, err := mime.ParseMediaType(amt)
			if err != nil {
				return false, err
			}
			for _, wmt := range mts {
				if mt == wmt {
					return true, nil
				}
			}
		}
	}
	return false, nil
}
