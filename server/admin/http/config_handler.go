package adminserver

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
)

func registerSetLogLevelHandler(r *mux.Router) *mux.Route {
	return r.HandleFunc("/config/log/level", setLogLevel).Methods(http.MethodPost)
}

// setLogLevel sets the log level for a subsystem matched using regular expression.
// Multiple subsystems and levels may be specified as query parameters where
// the key signifies a rgular expression matching a subsystem, and value
// signifies the desired level. At least on such pair must be specified.
//
// See: registerSetLogLevelHandler.
func setLogLevel(w http.ResponseWriter, r *http.Request) {

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
	w.WriteHeader(http.StatusOK)
}

// listLogSubSystems prints current logging subsystems one at a line.
func listLogSubSystems(w http.ResponseWriter, _ *http.Request) {
	subsystems := logging.GetSubsystems()
	sort.Strings(subsystems)
	for _, ss := range subsystems {
		_, _ = fmt.Fprintln(w, ss)
	}
	log.Debugw("Listed logging subsystems", "subsystems", subsystems)
}

func registerListLogSubSystems(r *mux.Router) *mux.Route {
	return r.HandleFunc("/config/log/subsystems", listLogSubSystems).Methods(http.MethodGet)
}
