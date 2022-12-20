package adminserver

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/zap/zapcore"
)

func Test_ListLoggingSubsystems(t *testing.T) {
	router := mux.NewRouter()
	registerListLogSubSystems(router)

	req, err := http.NewRequest(http.MethodGet, "/config/log/subsystems", nil)
	qt.Assert(t, err, qt.IsNil)

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	qt.Check(t, rr.Code, qt.Equals, http.StatusOK)

	scanner := bufio.NewScanner(rr.Body)
	subsystems := logging.GetSubsystems()
	sort.Strings(subsystems)
	for _, ss := range subsystems {
		eof := scanner.Scan()
		qt.Assert(t, eof, qt.IsTrue)
		line := scanner.Text()
		qt.Assert(t, line, qt.Equals, ss)
	}

	eof := scanner.Scan()
	qt.Assert(t, eof, qt.IsFalse)
	qt.Assert(t, scanner.Err(), qt.IsNil)
}

func Test_SetLogLevel_NoQueryParamsIsError(t *testing.T) {
	router := mux.NewRouter()
	registerSetLogLevelHandler(router)

	req, err := http.NewRequest(http.MethodPost, "/config/log/level", nil)
	qt.Assert(t, err, qt.IsNil)

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	qt.Check(t, rr.Code, qt.Equals, http.StatusBadRequest)

	respBody, err := io.ReadAll(rr.Body)
	qt.Assert(t, err, qt.IsNil)
	qt.Check(t, strings.TrimSpace(string(respBody)), qt.Equals, "at least one <subsystem>=<level> query parameter must be specified")
}

func Test_SetLogLevel(t *testing.T) {
	tests := []struct {
		name           string
		givenSubsystem string
		givenLevel     string
		wantStatus     int
		wantMessage    string
		wantAssertions func(t *testing.T)
	}{
		{
			name:           "invalid level is error",
			givenSubsystem: ".*",
			givenLevel:     "fish",
			wantStatus:     http.StatusInternalServerError,
			wantMessage:    `unrecognized level: "fish"`,
		},
		{
			name:           "no level is error",
			givenSubsystem: "indexer/ingest",
			wantStatus:     http.StatusBadRequest,
			wantMessage:    `level is not specified for subsystem: indexer/ingest`,
		},
		{
			name:           "regex all to debug",
			givenSubsystem: ".*",
			givenLevel:     "debug",
			wantStatus:     http.StatusOK,
			wantAssertions: func(t *testing.T) {
				assertLogLevel(t, zapcore.DebugLevel, true)
			},
		},
		{
			name:           "explicit subsystem",
			givenSubsystem: "indexer/admin",
			givenLevel:     "debug",
			wantStatus:     http.StatusOK,
			wantAssertions: func(t *testing.T) {
				assertLogLevelBySubSystem(t, "indexer/admin", zapcore.DebugLevel, true)
			},
		},
		{
			name:           "regex all to error",
			givenSubsystem: ".*",
			givenLevel:     "error",
			wantStatus:     http.StatusOK,
			wantAssertions: func(t *testing.T) {
				assertLogLevel(t, zapcore.ErrorLevel, true)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := mux.NewRouter()
			registerSetLogLevelHandler(router)

			req, err := http.NewRequest(http.MethodPost, "/config/log/level", nil)
			qt.Assert(t, err, qt.IsNil)
			q := url.Values{}
			q.Add(tt.givenSubsystem, tt.givenLevel)
			req.URL.RawQuery = q.Encode()

			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)

			qt.Check(t, rr.Code, qt.Equals, tt.wantStatus)

			respBody, err := io.ReadAll(rr.Body)
			qt.Assert(t, err, qt.IsNil)
			qt.Check(t, strings.TrimSpace(string(respBody)), qt.Equals, tt.wantMessage)
			if tt.wantAssertions != nil {
				tt.wantAssertions(t)
			}
		})
	}
}

func assertLogLevel(t *testing.T, level zapcore.Level, enabled bool) {
	for _, ss := range logging.GetSubsystems() {
		assertLogLevelBySubSystem(t, ss, level, enabled)
	}
}

func assertLogLevelBySubSystem(t *testing.T, ss string, level zapcore.Level, enabled bool) {
	qt.Assert(t, logging.Logger(ss).Desugar().Core().Enabled(level), qt.Equals, enabled)
}
