package metrics_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ipni/storetheindex/assigner/metrics"
	"github.com/stretchr/testify/require"
)

func TestMetricsServer(t *testing.T) {
	s, err := metrics.New("127.0.0.1:0")
	require.NoError(t, err)

	errChan := make(chan error, 1)
	go func() {
		err := s.Start()
		if err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()
	t.Cleanup(func() {
		require.NoError(t, s.Close())
		require.NoError(t, <-errChan)
	})

	metrics.RecordReceived("json", metrics.ResultOK)
	res, err := http.Get(s.URL() + "/metrics")
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.True(t, strings.Contains(string(body), "assigner_announce_received_total"))
}
