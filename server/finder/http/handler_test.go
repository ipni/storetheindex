package httpfinderserver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/server/finder/test"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestServer_CORSWithExpectedContentType(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	mhs := util.RandomMultihashes(10, rng)
	findBatchRequest, err := model.MarshalFindRequest(&model.FindRequest{Multihashes: mhs})
	require.NoError(t, err)
	c := cid.NewCidV1(cid.Raw, mhs[0])

	ind := test.InitIndex(t, false)
	reg := test.InitRegistry(t)
	s, err := New("127.0.0.1:0", ind, reg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ind.Close())
		require.NoError(t, reg.Close())
	})

	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	require.NoError(t, err)
	err = ind.Put(indexer.Value{
		ProviderID:    p,
		ContextID:     []byte("fish"),
		MetadataBytes: []byte("lobster"),
	}, mhs...)
	require.NoError(t, err)
	require.NoError(t, ind.Flush())
	v, found, err := ind.Get(mhs[0])
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, v)

	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	info := &registry.ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    p,
			Addrs: []multiaddr.Multiaddr{a},
		},
	}
	err = reg.Register(context.TODO(), info)
	require.NoError(t, err)

	tests := []struct {
		reqMethod       string
		reqUrl          string
		reqBody         io.Reader
		wantContentType string
	}{
		{
			reqMethod:       http.MethodGet,
			reqUrl:          "/stats",
			wantContentType: "application/json",
		},
		{
			reqMethod:       http.MethodGet,
			reqUrl:          "/health",
			wantContentType: "application/json",
		},
		{
			reqMethod:       http.MethodGet,
			reqUrl:          "/providers",
			wantContentType: "application/json",
		},
		{
			reqMethod:       http.MethodGet,
			reqUrl:          "/providers/" + p.String(),
			wantContentType: "application/json",
		},
		{
			reqMethod:       http.MethodGet,
			reqUrl:          "/multihash/" + mhs[0].B58String(),
			wantContentType: "application/json",
		},
		{
			reqMethod:       http.MethodPost,
			reqUrl:          "/multihash",
			reqBody:         bytes.NewBuffer(findBatchRequest),
			wantContentType: "application/json",
		},
		{
			reqMethod:       http.MethodGet,
			reqUrl:          "/cid/" + c.String(),
			wantContentType: "application/json",
		},
		{
			reqMethod:       http.MethodGet,
			reqUrl:          "/",
			wantContentType: "text/html",
		},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("%s %s", tt.reqMethod, tt.reqUrl)
		t.Run(name, func(t *testing.T) {

			rr := httptest.NewRecorder()

			req, err := http.NewRequest(tt.reqMethod, tt.reqUrl, tt.reqBody)
			require.NoError(t, err)
			// Set necessary headers for CORS.
			req.Header.Set("Origin", "ghoti")
			req.Header.Set("Access-Control-Request-Method", tt.reqMethod)

			s.server.Handler.ServeHTTP(rr, req)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			require.Equal(t, "*", rr.Header().Get("Access-Control-Allow-Origin"))

			gotContentType := rr.Header().Get("Content-Type")
			require.True(t, strings.HasPrefix(gotContentType, tt.wantContentType), rr.Body.String())

			// Assert the endpoint supports OPTIONS as required by CORS.
			optReq, err := http.NewRequest(http.MethodOptions, tt.reqUrl, nil)
			require.NoError(t, err)
			s.server.Handler.ServeHTTP(rr, optReq)
			require.Equal(t, http.StatusOK, rr.Code)
		})
	}
}
