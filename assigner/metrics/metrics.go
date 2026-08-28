package metrics

import (
	"net/url"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	ResultOK          = "ok"
	ResultDecodeError = "decode_error"
	ResultInvalid     = "invalid"
	ResultForbidden   = "forbidden"
	ResultError       = "error"
)

var (
	AnnounceReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "assigner_announce_received_total",
			Help: "HTTP announce requests handled by the assigner, by encoding and result.",
		},
		[]string{"encoding", "result"},
	)

	AnnounceAccepted = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "assigner_announce_accepted_total",
			Help: "Announces that passed the in-memory CID cache and were processed (forwarded and/or assigned). Duplicates of a recently seen CID never increment this.",
		},
	)

	AnnounceForwarded = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "assigner_announce_forwarded_total",
			Help: "HTTP announces forwarded to a target ingest URL, by target host and result.",
		},
		[]string{"target", "result"},
	)
)

func RecordReceived(encoding, result string) {
	AnnounceReceived.WithLabelValues(encoding, result).Inc()
}

func RecordAccepted() {
	AnnounceAccepted.Inc()
}

func RecordForwarded(ingestURL, result string) {
	AnnounceForwarded.WithLabelValues(forwardTarget(ingestURL), result).Inc()
}

func forwardTarget(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}
	return u.Host
}
