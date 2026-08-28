package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-libipni/announce/message"
	client "github.com/ipni/go-libipni/ingest/client"
	"github.com/ipni/storetheindex/assigner/config"
	"github.com/ipni/storetheindex/assigner/core"
	server "github.com/ipni/storetheindex/assigner/server"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func setupAllowAllAssigner(t *testing.T) *core.Assigner {
	t.Helper()
	cfg := config.Assignment{
		Policy: config.Policy{
			Allow: true,
		},
		PubSubTopic: "testtopic",
	}
	assigner, err := core.NewAssigner(t.Context(), cfg, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, assigner.Close())
	})
	return assigner
}

func startAssignerServer(t *testing.T, assigner *core.Assigner) *server.Server {
	t.Helper()
	s := setupServer(t, assigner)
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
	return s
}

func testAnnounceMessage(t *testing.T) (message.Message, peer.AddrInfo, cid.Cid) {
	t.Helper()
	peerID, _, err := pubIdent.Decode()
	require.NoError(t, err)
	ai, err := peer.AddrInfoFromString(fmt.Sprintf("/ip4/127.0.0.1/tcp/9999/p2p/%s", peerID))
	require.NoError(t, err)
	ai.ID = peerID
	p2pAddrs, err := peer.AddrInfoToP2pAddrs(ai)
	require.NoError(t, err)
	adCid := cid.NewCidV1(cid.Raw, random.Multihashes(1)[0])
	msg := message.Message{Cid: adCid}
	msg.SetAddrs(p2pAddrs)
	return msg, *ai, adCid
}

func TestAnnounceJSON(t *testing.T) {
	s := startAssignerServer(t, setupAllowAllAssigner(t))
	msg, _, _ := testAnnounceMessage(t)
	body, err := json.Marshal(msg)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, s.URL()+"/announce", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusNoContent, res.StatusCode)
}

func TestAnnounceJSONCharset(t *testing.T) {
	s := startAssignerServer(t, setupAllowAllAssigner(t))
	msg, _, _ := testAnnounceMessage(t)
	body, err := json.Marshal(msg)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, s.URL()+"/ingest/announce", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusNoContent, res.StatusCode)
}

func TestAnnounceJSONInvalid(t *testing.T) {
	s := startAssignerServer(t, setupAllowAllAssigner(t))

	req, err := http.NewRequest(http.MethodPut, s.URL()+"/announce", bytes.NewReader([]byte("not json")))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
	_, _ = io.Copy(io.Discard, res.Body)
}

func TestAnnounceCBOR(t *testing.T) {
	s := startAssignerServer(t, setupAllowAllAssigner(t))
	_, ai, adCid := testAnnounceMessage(t)
	cl, err := client.New(s.URL())
	require.NoError(t, err)

	err = cl.Announce(t.Context(), &ai, adCid)
	require.NoError(t, err)
}
