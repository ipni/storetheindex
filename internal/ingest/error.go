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
	// adIngestEntryChunkErr is an entries block that was fetched from the
	// publisher and stored, but does not load as an EntryChunk when ingest
	// reads it back. The publisher serves the same block on every retry.
	adIngestEntryChunkErr adIngestState = "ingestEntryChunkErr"
)

func (e adIngestError) Error() string {
	return fmt.Sprintf("%s: %s", e.state, e.err)
}

func (e adIngestError) Unwrap() error {
	return e.err
}
