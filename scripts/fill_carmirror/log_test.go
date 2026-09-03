package main

import (
	"testing"

	logging "github.com/ipfs/go-log/v2"
)

func TestResolveLogFormat(t *testing.T) {
	t.Setenv("GOLOG_LOG_FMT", "")
	t.Setenv("IPFS_LOGGING_FMT", "")

	got, err := resolveLogFormat()
	if err != nil {
		t.Fatal(err)
	}
	if got != logging.JSONOutput {
		t.Fatalf("default format: got %v want json", got)
	}

	t.Setenv("GOLOG_LOG_FMT", "color")
	got, err = resolveLogFormat()
	if err != nil {
		t.Fatal(err)
	}
	if got != logging.ColorizedOutput {
		t.Fatalf("GOLOG_LOG_FMT=color: got %v want color", got)
	}
}
