package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// stripDynamicTimes removes StartTime, EndTime, and Elapsed from current phase
// objects and from each top-level *History entry before exact JSON comparison.
func stripDynamicTimes(t *testing.T, data []byte) []byte {
	t.Helper()
	m := map[string]any{}
	require.NoError(t, json.Unmarshal(data, &m))
	for _, phase := range []string{"Scan", "Processing", "Download"} {
		stripPhaseTimes(t, m[phase])
	}
	for _, histKey := range []string{"ScanHistory", "ProcessingHistory", "DownloadHistory"} {
		if hist, ok := m[histKey].([]any); ok {
			for _, h := range hist {
				stripPhaseTimes(t, h)
			}
		}
	}
	out, err := json.Marshal(m)
	require.NoError(t, err)
	return out
}

func stripPhaseTimes(t *testing.T, v any) {
	t.Helper()
	if v == nil {
		return
	}
	pm, ok := v.(map[string]any)
	require.True(t, ok)
	for _, key := range []string{"StartTime", "EndTime", "Elapsed"} {
		if val, ok := pm[key]; ok {
			require.NotEmpty(t, val)
			delete(pm, key)
		}
	}
}

func assertTrackerJSON(t *testing.T, st *syncTracker, expected string) {
	t.Helper()
	data, err := json.Marshal(st)
	require.NoError(t, err)
	trimmed := stripDynamicTimes(t, data)
	require.JSONEq(t, expected, string(trimmed))
}

func TestSyncTrackerMarshalJSON(t *testing.T) {
	headAd, err := cid.Decode("baguqeeraa5mjufqdwzgafkqxmllc4hwzd4qcjqzj4tnaswgvazawepoqwzqa")
	require.NoError(t, err)
	curAd, err := cid.Decode("baguqeerakzwmt6pkjmymlywrf27uxvdtosv6cvyissv2eqgyvy7g7p35tptq")
	require.NoError(t, err)
	provID := peer.ID("test-provider")

	st := &syncTracker{}
	assertTrackerJSON(t, st, `{}`)

	st.recordAdScanned(provID, headAd)
	st.recordAdScanned(provID, curAd)
	assertTrackerJSON(t, st, fmt.Sprintf(`{
		"Provider": %q,
		"Scan": {
			"Ongoing": true,
			"AdsScanned": 2,
			"HeadAd": %q,
			"CurrentAd": %q
		}
	}`, provID.String(), headAd.String(), curAd.String()))

	st.endScan(nil)
	st.BeginProcessing(5)
	st.SetCurrentAd(curAd, 1)
	assertTrackerJSON(t, st, fmt.Sprintf(`{
		"Provider": %q,
		"ScanHistory": [{
			"AdsScanned": 2,
			"HeadAd": %q,
			"CurrentAd": %q
		}],
		"Processing": {
			"Ongoing": true,
			"AdsProcessed": 1,
			"AdsTotal": 5,
			"AdsLeft": 4,
			"CurrentAd": %q
		}
	}`, provID.String(), headAd.String(), curAd.String(), curAd.String()))

	st.SetDownloading()
	st.AddChunk(100)
	st.AddChunk(50)
	st.AddHamtMultihashes(30)
	st.AddBytes(4096)
	st.IncError()
	assertTrackerJSON(t, st, fmt.Sprintf(`{
		"Provider": %q,
		"ScanHistory": [{
			"AdsScanned": 2,
			"HeadAd": %q,
			"CurrentAd": %q
		}],
		"Processing": {
			"Ongoing": true,
			"AdsProcessed": 1,
			"AdsTotal": 5,
			"AdsLeft": 4,
			"CurrentAd": %q,
			"ErrorCount": 1
		},
		"Download": {
			"Ongoing": true,
			"MultihashCount": 180,
			"ChunkMultihashCount": 150,
			"HamtMultihashCount": 30,
			"EntryChunkCount": 2,
			"BytesDownloaded": 4096
		}
	}`, provID.String(), headAd.String(), curAd.String(), curAd.String()))

	st.EndProcessing(nil)
	assertTrackerJSON(t, st, fmt.Sprintf(`{
		"Provider": %q,
		"ScanHistory": [{
			"AdsScanned": 2,
			"HeadAd": %q,
			"CurrentAd": %q
		}],
		"ProcessingHistory": [{
			"AdsProcessed": 1,
			"AdsTotal": 5,
			"AdsLeft": 4,
			"CurrentAd": %q,
			"ErrorCount": 1
		}],
		"DownloadHistory": [{
			"MultihashCount": 180,
			"ChunkMultihashCount": 150,
			"HamtMultihashCount": 30,
			"EntryChunkCount": 2,
			"BytesDownloaded": 4096
		}]
	}`, provID.String(), headAd.String(), curAd.String(), curAd.String()))

	st.recordAdScanned(provID, headAd)
	assertTrackerJSON(t, st, fmt.Sprintf(`{
		"Provider": %q,
		"Scan": {
			"Ongoing": true,
			"AdsScanned": 1,
			"HeadAd": %q,
			"CurrentAd": %q
		},
		"ScanHistory": [{
			"AdsScanned": 2,
			"HeadAd": %q,
			"CurrentAd": %q
		}],
		"ProcessingHistory": [{
			"AdsProcessed": 1,
			"AdsTotal": 5,
			"AdsLeft": 4,
			"CurrentAd": %q,
			"ErrorCount": 1
		}],
		"DownloadHistory": [{
			"MultihashCount": 180,
			"ChunkMultihashCount": 150,
			"HamtMultihashCount": 30,
			"EntryChunkCount": 2,
			"BytesDownloaded": 4096
		}]
	}`, provID.String(), headAd.String(), headAd.String(), headAd.String(), curAd.String(), curAd.String()))
}

func TestSyncTrackerPhaseError(t *testing.T) {
	headAd, err := cid.Decode("baguqeeraa5mjufqdwzgafkqxmllc4hwzd4qcjqzj4tnaswgvazawepoqwzqa")
	require.NoError(t, err)
	provID := peer.ID("test-provider")
	syncErr := errors.New("sync failed")

	st := &syncTracker{}
	st.recordAdScanned(provID, headAd)
	st.endScan(syncErr)

	data, err := json.Marshal(st)
	require.NoError(t, err)
	m := map[string]any{}
	require.NoError(t, json.Unmarshal(data, &m))
	require.NotContains(t, m, "Scan")
	hist := m["ScanHistory"].([]any)
	require.Len(t, hist, 1)
	scan := hist[0].(map[string]any)
	require.Equal(t, syncErr.Error(), scan["Error"])
	require.NotContains(t, scan, "Ongoing")
}

func TestSyncTrackerMarshalJSONHamtOnly(t *testing.T) {
	st := &syncTracker{}
	st.BeginProcessing(1)
	st.SetDownloading()
	st.AddHamtMultihashes(42)
	assertTrackerJSON(t, st, `{
		"Processing": {
			"Ongoing": true,
			"AdsTotal": 1,
			"AdsLeft": 1
		},
		"Download": {
			"Ongoing": true,
			"MultihashCount": 42,
			"HamtMultihashCount": 42
		}
	}`)
}

func TestRegistrySyncStatus(t *testing.T) {
	r := &Registry{
		syncStatus: make(map[peer.ID]*syncTracker),
	}
	pubID := peer.ID("test-publisher")

	require.Nil(t, r.SyncStatusJSONFor(pubID))

	st := r.SyncStatusFor(pubID)
	require.NotNil(t, st)
	require.Same(t, st, r.SyncStatusFor(pubID))

	st.BeginProcessing(3)
	data := r.SyncStatusJSONFor(pubID)
	m := map[string]any{}
	require.NoError(t, json.Unmarshal(data, &m))
	proc := m["Processing"].(map[string]any)
	require.Equal(t, float64(3), proc["AdsTotal"])
	require.Equal(t, true, proc["Ongoing"])

	st.EndProcessing(nil)
	require.NotNil(t, r.SyncStatusJSONFor(pubID))
	data = r.SyncStatusJSONFor(pubID)
	m = map[string]any{}
	require.NoError(t, json.Unmarshal(data, &m))
	require.NotContains(t, m, "Processing")
	procHist := m["ProcessingHistory"].([]any)
	require.Len(t, procHist, 1)
	proc = procHist[0].(map[string]any)
	require.NotContains(t, proc, "Ongoing")
}

func TestRegistrySyncScanned(t *testing.T) {
	r := &Registry{
		syncStatus: make(map[peer.ID]*syncTracker),
	}
	pubID := peer.ID("test-publisher")
	provID := peer.ID("test-provider")
	ad1, err := cid.Decode("baguqeeraa5mjufqdwzgafkqxmllc4hwzd4qcjqzj4tnaswgvazawepoqwzqa")
	require.NoError(t, err)
	ad2, err := cid.Decode("baguqeerakzwmt6pkjmymlywrf27uxvdtosv6cvyissv2eqgyvy7g7p35tptq")
	require.NoError(t, err)

	require.Equal(t, 1, r.RecordAdScanned(pubID, provID, ad1))
	require.Equal(t, 2, r.RecordAdScanned(pubID, provID, ad2))

	data := stripDynamicTimes(t, mustMarshal(t, r.SyncStatusJSONFor(pubID)))
	require.JSONEq(t, fmt.Sprintf(`{
		"Provider": %q,
		"Scan": {
			"Ongoing": true,
			"AdsScanned": 2,
			"HeadAd": %q,
			"CurrentAd": %q
		}
	}`, provID.String(), ad1.String(), ad2.String()), string(data))
}

func TestRegistryAllSyncStatuses(t *testing.T) {
	r := &Registry{
		syncStatus: make(map[peer.ID]*syncTracker),
	}
	pubID := peer.ID("test-publisher")
	provID := peer.ID("test-provider")
	ad, err := cid.Decode("baguqeeraa5mjufqdwzgafkqxmllc4hwzd4qcjqzj4tnaswgvazawepoqwzqa")
	require.NoError(t, err)

	all := r.AllSyncStatuses()
	require.Empty(t, all)

	r.RecordAdScanned(pubID, provID, ad)

	data := stripDynamicTimes(t, mustMarshal(t, r.SyncStatusJSONFor(pubID)))
	expected := fmt.Sprintf(`{
		"Provider": %q,
		"Scan": {
			"Ongoing": true,
			"AdsScanned": 1,
			"HeadAd": %q,
			"CurrentAd": %q
		}
	}`, provID.String(), ad.String(), ad.String())
	require.JSONEq(t, expected, string(data))

	all = r.AllSyncStatuses()
	require.Len(t, all, 1)
	require.JSONEq(t, expected, string(stripDynamicTimes(t, all[pubID])))
}

func TestRegistryEndScan(t *testing.T) {
	r := &Registry{
		syncStatus: make(map[peer.ID]*syncTracker),
	}
	pubID := peer.ID("test-publisher")
	provID := peer.ID("test-provider")
	ad, err := cid.Decode("baguqeeraa5mjufqdwzgafkqxmllc4hwzd4qcjqzj4tnaswgvazawepoqwzqa")
	require.NoError(t, err)

	r.RecordAdScanned(pubID, provID, ad)
	r.EndScan(pubID, nil)

	data := stripDynamicTimes(t, mustMarshal(t, r.SyncStatusJSONFor(pubID)))
	m := map[string]any{}
	require.NoError(t, json.Unmarshal(data, &m))
	require.NotContains(t, m, "Scan")
	hist := m["ScanHistory"].([]any)
	require.Len(t, hist, 1)
	scan := hist[0].(map[string]any)
	require.NotContains(t, scan, "Ongoing")
}

func mustMarshal(t *testing.T, data json.RawMessage) []byte {
	t.Helper()
	if data == nil {
		return []byte("null")
	}
	return data
}
