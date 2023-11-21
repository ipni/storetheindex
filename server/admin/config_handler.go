package admin

import (
	"fmt"
	"net/http"
	"sort"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/storetheindex/internal/httpserver"
)

// setLogLevel sets the log level for a subsystem matched using regular expression.
// Multiple subsystems and levels may be specified as query parameters where
// the key signifies a rgular expression matching a subsystem, and value
// signifies the desired level. At least on such pair must be specified.
//
// See: registerSetLogLevelHandler.
func setLogLevel(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPost) {
		return
	}

	query := r.URL.Query()
	qSize := len(query)
	if qSize == 0 {
		http.Error(w, "at least one <subsystem>=<level> query parameter must be specified", http.StatusBadRequest)
		return
	}

	for ss := range query {
		l := query.Get(ss)
		if l == "" {
			log.Errorw("Level not set for subsystem", "subsystem", ss)
			http.Error(w,
				fmt.Sprintf("level is not specified for subsystem: %s", ss),
				http.StatusBadRequest)
			return
		}
		if err := logging.SetLogLevelRegex(ss, l); err != nil {
			log.Errorw("Failed to set log level", "subsystem", ss, "level", l, "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Infow("Log level changed", "subsystem", ss, "level", l)
	}
}

// listLogSubSystems prints current logging subsystems one at a line.
func listLogSubSystems(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	subsystems := logging.GetSubsystems()
	sort.Strings(subsystems)
	for _, ss := range subsystems {
		_, _ = fmt.Fprintln(w, ss)
	}
	log.Debugw("Listed logging subsystems", "subsystems", subsystems)
}
