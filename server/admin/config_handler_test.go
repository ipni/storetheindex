package admin

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func Test_ListLoggingSubsystems(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/config/log/subsystems", listLogSubSystems)

	req, err := http.NewRequest(http.MethodGet, "/config/log/subsystems", nil)
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	scanner := bufio.NewScanner(rr.Body)
	subsystems := logging.GetSubsystems()
	slices.Sort(subsystems)
	for _, ss := range subsystems {
		eof := scanner.Scan()
		require.True(t, eof)
		line := scanner.Text()
		require.Equal(t, ss, line)
	}

	eof := scanner.Scan()
	require.False(t, eof)
	require.NoError(t, scanner.Err())
}

func Test_SetLogLevel_NoQueryParamsIsError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/config/log/level", setLogLevel)

	req, err := http.NewRequest(http.MethodPost, "/config/log/level", nil)
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)

	respBody, err := io.ReadAll(rr.Body)
	require.NoError(t, err)
	require.Equal(t, "at least one <subsystem>=<level> query parameter must be specified", strings.TrimSpace(string(respBody)))
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
			mux := http.NewServeMux()
			mux.HandleFunc("/config/log/level", setLogLevel)

			req, err := http.NewRequest(http.MethodPost, "/config/log/level", nil)
			require.NoError(t, err)
			q := url.Values{}
			q.Add(tt.givenSubsystem, tt.givenLevel)
			req.URL.RawQuery = q.Encode()

			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)

			require.Equal(t, tt.wantStatus, rr.Code)

			respBody, err := io.ReadAll(rr.Body)
			require.NoError(t, err)
			require.Equal(t, tt.wantMessage, strings.TrimSpace(string(respBody)))
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
	require.Equal(t, enabled, logging.Logger(ss).Desugar().Core().Enabled(level))
}
