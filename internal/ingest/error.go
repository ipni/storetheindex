package ingest

import (
	"fmt"
)

type adIngestState string

type adIngestError struct {
	state adIngestState
	err   error
}

const (
	adIngestIndexerErr          adIngestState = "indexerErr"
	adIngestDecodingErr         adIngestState = "decodingErr"
	adIngestMalformedErr        adIngestState = "malformedErr"
	adIngestRegisterProviderErr adIngestState = "registerErr"
	adIngestSyncEntriesErr      adIngestState = "syncEntriesErr"
	adIngestContentNotFound     adIngestState = "contentNotFound"
	// Happens if there is an error during ingest of an entry chunk (rather than fetching it).
	adIngestEntryChunkErr adIngestState = "ingestEntryChunkErr"
)

func (e adIngestError) Error() string {
	return fmt.Sprintf("%s: %s", e.state, e.err)
}

func (e adIngestError) Unwrap() error {
	return e.err
}
