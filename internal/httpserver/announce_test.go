package httpserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/stretchr/testify/require"
)

func testAnnounceJSONBody(t *testing.T) []byte {
	t.Helper()
	msg := message.Message{Cid: cid.NewCidV1(cid.Raw, random.Multihashes(1)[0])}
	body, err := json.Marshal(msg)
	require.NoError(t, err)
	return body
}

func TestDecodeAnnounceMessageJSON(t *testing.T) {
	body := testAnnounceJSONBody(t)
	req := httptest.NewRequest(http.MethodPut, "/announce", bytes.NewReader(body))
	req.Header.Set("Content-Type", MediaTypeJson)
	rec := httptest.NewRecorder()

	an, encoding, err := DecodeAnnounceMessage(rec, req)
	require.NoError(t, err)
	require.Equal(t, AnnounceEncodingJSON, encoding)
	require.True(t, an.Cid.Defined())
}

func TestDecodeAnnounceMessageJSONCharset(t *testing.T) {
	body := testAnnounceJSONBody(t)
	req := httptest.NewRequest(http.MethodPut, "/announce", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	rec := httptest.NewRecorder()

	an, encoding, err := DecodeAnnounceMessage(rec, req)
	require.NoError(t, err)
	require.Equal(t, AnnounceEncodingJSON, encoding)
	require.True(t, an.Cid.Defined())
}

func TestDecodeAnnounceMessageCBOR(t *testing.T) {
	msg := message.Message{Cid: cid.NewCidV1(cid.Raw, random.Multihashes(1)[0])}
	var buf bytes.Buffer
	require.NoError(t, msg.MarshalCBOR(&buf))
	req := httptest.NewRequest(http.MethodPut, "/announce", &buf)
	rec := httptest.NewRecorder()

	an, encoding, err := DecodeAnnounceMessage(rec, req)
	require.NoError(t, err)
	require.Equal(t, AnnounceEncodingCBOR, encoding)
	require.Equal(t, msg.Cid, an.Cid)
}
