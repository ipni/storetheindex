package metrics

import (
	"context"
	"sync/atomic"

	logging "github.com/ipfs/go-log/v2"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/unit"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/aggregation"

	"go.opentelemetry.io/otel/exporters/prometheus"
	cmetric "go.opentelemetry.io/otel/metric"
)

// Global Tags
var (
	ErrKind = "errKind"
	Method  = "method"
	Found   = "found"
	Version = "version"
)

type stiMetrics struct {
	FindLatency          instrument.Int64Histogram
	IngestChange         instrument.Int64Counter
	AdIngestLatency      instrument.Int64Histogram
	AdIngestErrorCount   instrument.Int64Counter
	AdIngestQueued       instrument.Int64Counter
	AdIngestBacklog      instrument.Int64Histogram
	AdIngestActive       instrument.Int64Histogram
	AdIngestSuccessCount instrument.Int64Counter
	AdIngestSkippedCount instrument.Int64Counter
	AdLoadError          instrument.Int64Counter
	EntriesSyncLatency   instrument.Int64Histogram
	providerCount        instrument.Int64ObservableGauge
	mhStoreNanoseconds   instrument.Int64ObservableGauge
	indexCount           instrument.Int64ObservableGauge
	percentUsage         instrument.Float64ObservableGauge

	ProviderCountValue      atomic.Int64
	MhStoreNanosecondsValue atomic.Int64
	IndexCountValue         atomic.Int64
	PercentUsageValue       atomic.Value

	meter  cmetric.Meter
	bmeter cmetric.Meter

	reg cmetric.Registration
}

func aggregationSelector(ik metric.InstrumentKind) aggregation.Aggregation {
	if ik == metric.InstrumentKindHistogram {
		return aggregation.ExplicitBucketHistogram{
			Boundaries: []float64{0, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 1000, 2000, 5000},
			NoMinMax:   false,
		}
	}
	return metric.DefaultAggregationSelector(ik)
}

func backlogAggregationSelector(ik metric.InstrumentKind) aggregation.Aggregation {
	if ik == metric.InstrumentKindHistogram {
		return aggregation.ExplicitBucketHistogram{
			Boundaries: []float64{0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
			NoMinMax:   false,
		}
	}
	return metric.DefaultAggregationSelector(ik)
}

func newStiMetrics() (*stiMetrics, error) {
	var m stiMetrics
	var err error
	var exporter *prometheus.Exporter
	if exporter, err = prometheus.New(prometheus.WithoutUnits(),
		prometheus.WithAggregationSelector(aggregationSelector)); err != nil {
		return nil, err
	}

	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	m.meter = provider.Meter("ipni/storetheindex")

	var bexporter *prometheus.Exporter
	if bexporter, err = prometheus.New(prometheus.WithoutUnits(),
		prometheus.WithAggregationSelector(backlogAggregationSelector)); err != nil {
		return nil, err
	}
	bprovider := metric.NewMeterProvider(metric.WithReader(bexporter))
	m.bmeter = bprovider.Meter("ipni/storetheindex/backlogs")

	m.MhStoreNanosecondsValue.Store(int64(0))
	m.IndexCountValue.Store(int64(0))
	m.PercentUsageValue.Store(float64(0))
	m.ProviderCountValue.Store(int64(0))

	if m.FindLatency, err = m.meter.Int64Histogram("find/latency",
		instrument.WithDescription("Time to respond to a find request"),
		instrument.WithUnit(unit.Milliseconds)); err != nil {
		return nil, err
	}

	if m.IngestChange, err = m.meter.Int64Counter("ingest/change",
		instrument.WithDescription("Number of syncAdEntries started"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.AdIngestLatency, err = m.meter.Int64Histogram("ingest/adsynclatency",
		instrument.WithDescription("latency of syncAdEntries completed successfully"),
		instrument.WithUnit(unit.Milliseconds)); err != nil {
		return nil, err
	}

	if m.AdIngestErrorCount, err = m.meter.Int64Counter("ingest/adingestError",
		instrument.WithDescription("Number of errors encountered while processing an ad"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.AdIngestQueued, err = m.meter.Int64Counter("ingest/adingestqueued",
		instrument.WithDescription("Number of queued advertisements"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.AdIngestBacklog, err = m.bmeter.Int64Histogram("ingest/adbacklog",
		instrument.WithDescription("Queued backlog of adverts"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.AdIngestActive, err = m.bmeter.Int64Histogram("ingest/adactive",
		instrument.WithDescription("Active ingest workers"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.AdIngestSuccessCount, err = m.meter.Int64Counter("ingest/adingestSuccess",
		instrument.WithDescription("Number of successful ad ingest"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.AdIngestSkippedCount, err = m.meter.Int64Counter("ingest/adingestSkipped",
		instrument.WithDescription("Number of ads skipped during ingest"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.AdLoadError, err = m.meter.Int64Counter("ingest/adLoadError",
		instrument.WithDescription("Number of times an ad failed to load"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.providerCount, err = m.meter.Int64ObservableGauge("provider/count",
		instrument.WithDescription("Number of known (registered) providers"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.EntriesSyncLatency, err = m.meter.Int64Histogram("ingest/entriessynclatency",
		instrument.WithDescription("How long it took to sync an Ad's entries"),
		instrument.WithUnit(unit.Milliseconds)); err != nil {
		return nil, err
	}

	if m.mhStoreNanoseconds, err = m.meter.Int64ObservableGauge("ingest/mhstorenanoseconds",
		instrument.WithDescription("Average nanoseconds to store one multihash"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.indexCount, err = m.meter.Int64ObservableGauge("provider/indexCount",
		instrument.WithDescription("Number of indexes stored for all providers"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	if m.percentUsage, err = m.meter.Float64ObservableGauge("ingest/percentusage",
		instrument.WithDescription("Percent usage of storage available in value store"),
		instrument.WithUnit(unit.Dimensionless)); err != nil {
		return nil, err
	}

	return &m, nil
}

var log = logging.Logger("indexer/metrics")

func (m *stiMetrics) observe(ctx context.Context, o cmetric.Observer) error {
	o.ObserveInt64(m.providerCount, m.ProviderCountValue.Load())
	o.ObserveInt64(m.mhStoreNanoseconds, m.MhStoreNanosecondsValue.Load())
	o.ObserveInt64(m.indexCount, m.IndexCountValue.Load())
	o.ObserveFloat64(m.percentUsage, m.PercentUsageValue.Load().(float64))
	return nil
}

// Start creates an HTTP router for serving metric info
func (m *stiMetrics) Start() error {
	var err error

	m.reg, err = m.meter.RegisterCallback(
		m.observe,
		m.providerCount,
		m.mhStoreNanoseconds,
		m.indexCount,
		m.percentUsage,
	)

	log.Infow("Storetheindex metrics registered")

	return err
}

func (m *stiMetrics) Shutdwon() error {
	return m.reg.Unregister()
}
