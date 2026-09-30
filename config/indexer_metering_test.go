package config

import (
	"testing"
)

func TestMeteringDefaults(t *testing.T) {
	idx := NewIndexer()
	if idx.Metering == nil {
		t.Fatal("expected Metering defaults")
	}
	if idx.Metering.Enabled {
		t.Fatal("metering is off unless enabled")
	}
	if idx.Metering.ScanBatchSize != 1000000 {
		t.Fatalf("ScanBatchSize: got %d", idx.Metering.ScanBatchSize)
	}
	if idx.Metering.TimeFill != 0.1 {
		t.Fatalf("TimeFill: got %v", idx.Metering.TimeFill)
	}
}

func TestMeteringPopulateUnset(t *testing.T) {
	idx := &Indexer{
		Metering: &Metering{Enabled: true},
	}
	idx.populateUnset()
	if idx.Metering.ScanBatchSize != 1000000 {
		t.Fatalf("ScanBatchSize: got %d", idx.Metering.ScanBatchSize)
	}
	if idx.Metering.TimeFill != 0.1 {
		t.Fatalf("TimeFill: got %v", idx.Metering.TimeFill)
	}
	if !idx.Metering.Enabled {
		t.Fatal("explicit Enabled must stay set")
	}
}

func TestMeteringTimeFillPopulate(t *testing.T) {
	idx := &Indexer{
		Metering: &Metering{TimeFill: 0.5},
	}
	idx.populateUnset()
	if idx.Metering.TimeFill != 0.5 {
		t.Fatalf("TimeFill: got %v", idx.Metering.TimeFill)
	}

	over := &Indexer{Metering: &Metering{TimeFill: 2}}
	over.populateUnset()
	if over.Metering.TimeFill != 1 {
		t.Fatalf("out of range fill: got %v", over.Metering.TimeFill)
	}
}

func TestMeteringNilPopulate(t *testing.T) {
	idx := &Indexer{}
	idx.populateUnset()
	if idx.Metering == nil {
		t.Fatal("expected Metering to be set")
	}
}
