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

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/ipni/storetheindex/server/finder/test"
	"github.com/ipni/storetheindex/test/util"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestServer_CORSWithExpectedContentType(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	mhs := util.RandomMultihashes(10, rng)
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	require.NoError(t, err)
	findBatchRequest, err := model.MarshalFindRequest(&model.FindRequest{Multihashes: mhs})
	require.NoError(t, err)
	c := cid.NewCidV1(cid.Raw, mhs[0])

	subject := setupTestServerHander(t, indexer.Value{
		ProviderID:    p,
		ContextID:     []byte("fish"),
		MetadataBytes: []byte("lobster"),
	}, mhs)

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

			subject(rr, req)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			require.Equal(t, "*", rr.Header().Get("Access-Control-Allow-Origin"))

			gotContentType := rr.Header().Get("Content-Type")
			require.True(t, strings.HasPrefix(gotContentType, tt.wantContentType), rr.Body.String())

			// Assert the endpoint supports OPTIONS as required by CORS.
			optReq, err := http.NewRequest(http.MethodOptions, tt.reqUrl, nil)
			require.NoError(t, err)
			subject(rr, optReq)
			require.Equal(t, http.StatusOK, rr.Code)
		})
	}
}

func TestServer_StreamingResponse(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	mhs := util.RandomMultihashes(10, rng)
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	require.NoError(t, err)

	subject := setupTestServerHander(t, indexer.Value{
		ProviderID:    p,
		ContextID:     []byte("fish"),
		MetadataBytes: []byte("lobster"),
	}, mhs)

	tests := []struct {
		name             string
		reqURI           string
		reqAccept        string
		wantContentType  string
		wantResponseBody string
	}{
		{
			name:             "mutlihash json",
			reqURI:           "/multihash/" + mhs[3].B58String(),
			reqAccept:        "ext/html,  application/json",
			wantContentType:  "application/json; charset=utf-8",
			wantResponseBody: `{"MultihashResults":[{"Multihash":"EiCtVzjVYlU7UrB20GmR2mqk59dl7fk+Ann4CmsLfQfT+g==","ProviderResults":[{"ContextID":"ZmlzaA==","Metadata":"bG9ic3Rlcg==","Provider":{"ID":"12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA","Addrs":["/ip4/127.0.0.1/tcp/9999"]}}]}],"EncryptedMultihashResults":null}`,
		},
		{
			name:            "mutlihash ndjson",
			reqURI:          "/multihash/" + mhs[3].B58String(),
			reqAccept:       "application/x-ndjson,application/xhtml+xml,application/xml;q=0.9",
			wantContentType: "application/x-ndjson",
			wantResponseBody: `{"ContextID":"ZmlzaA==","Metadata":"bG9ic3Rlcg==","Provider":{"ID":"12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA","Addrs":["/ip4/127.0.0.1/tcp/9999"]}}

`,
		},
		{
			name:             "cid json",
			reqURI:           "/cid/" + cid.NewCidV1(cid.Raw, mhs[0]).String(),
			reqAccept:        "application/json,ext/html,  application/xhtml+xml,application/xml;q=0.9",
			wantContentType:  "application/json; charset=utf-8",
			wantResponseBody: `{"MultihashResults":[{"Multihash":"EiC44Rthii367t9Nb5PD6C0XT49Ub14+f0iF7gA4Xgr/6A==","ProviderResults":[{"ContextID":"ZmlzaA==","Metadata":"bG9ic3Rlcg==","Provider":{"ID":"12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA","Addrs":["/ip4/127.0.0.1/tcp/9999"]}}]}],"EncryptedMultihashResults":null}`,
		},
		{
			name:            "cid ndjson",
			reqURI:          "/cid/" + cid.NewCidV1(cid.Raw, mhs[5]).String(),
			reqAccept:       "ext/html,application/xhtml+xml,application/xml;q=0.9,application/x-ndjson",
			wantContentType: "application/x-ndjson",
			wantResponseBody: `{"ContextID":"ZmlzaA==","Metadata":"bG9ic3Rlcg==","Provider":{"ID":"12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA","Addrs":["/ip4/127.0.0.1/tcp/9999"]}}

`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := httptest.NewRecorder()

			req, err := http.NewRequest(http.MethodGet, tt.reqURI, nil)
			require.NoError(t, err)
			req.Header.Set("Accept", tt.reqAccept)
			subject(rr, req)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

			gotContentType := rr.Header().Get("Content-Type")
			require.Equal(t, tt.wantContentType, gotContentType)
			require.Equal(t, tt.wantResponseBody, rr.Body.String())
		})
	}
}

func TestServer_Landing(t *testing.T) {
	ind := test.InitIndex(t, false)
	reg := test.InitRegistry(t)
	s, err := New("127.0.0.1:0", ind, reg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ind.Close())
		reg.Close()
	})

	rr := httptest.NewRecorder()

	req, err := http.NewRequest(http.MethodGet, "/", nil)
	require.NoError(t, err)

	s.server.Handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	gotBody := rr.Body.String()
	require.NotEmpty(t, gotBody)
	require.True(t, strings.Contains(gotBody, "https://web-ipni.cid.contact/"))
}

func setupTestServerHander(t *testing.T, iv indexer.Value, mhs []multihash.Multihash) http.HandlerFunc {
	ind := test.InitIndex(t, false)
	reg := test.InitRegistry(t)
	s, err := New("127.0.0.1:0", ind, reg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ind.Close())
		reg.Close()
	})

	err = ind.Put(iv, mhs...)
	require.NoError(t, err)
	require.NoError(t, ind.Flush())
	v, found, err := ind.Get(mhs[0])
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, v)

	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	provider := peer.AddrInfo{
		ID:    iv.ProviderID,
		Addrs: []multiaddr.Multiaddr{a},
	}

	err = reg.Update(context.Background(), provider, peer.AddrInfo{}, cid.Undef, nil, 0)
	require.NoError(t, err)
	return s.server.Handler.ServeHTTP
}
