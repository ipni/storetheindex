package find_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/test"
	"github.com/ipni/storetheindex/server/find"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const landingRendered = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Network Indexer</title>
    <style type="text/css">
*, ::after, ::before {
  box-sizing: border-box;
  border: 0 solid;
}
body {
  font-family: inherit;
  line-height: inherit;
  margin: 0;
}
iframe {
  position:fixed;
  top:0; left:0;
  bottom:0;
  right:0;
  width:100vw;
  height:100vh;
  border:none;
  margin:0;
  padding:0;
  overflow:hidden;
  z-index:999999;
}
    </style>
</head>
<body>
  <iframe src="https://web-ipni.cid.contact/" frameborder="0"></iframe>
</body>
</html>`

func TestServer_CORSWithExpectedContentType(t *testing.T) {
	mhs := test.RandomMultihashes(10)
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	require.NoError(t, err)
	findBatchRequest, err := model.MarshalFindRequest(&model.FindRequest{Multihashes: mhs})
	require.NoError(t, err)
	c := cid.NewCidV1(cid.Raw, mhs[0])

	s := setupTestServer(t, indexer.Value{
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
			wantContentType: "text/plain",
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

	cl := http.DefaultClient

	for _, tt := range tests {
		name := fmt.Sprintf("%s %s", tt.reqMethod, tt.reqUrl)
		t.Run(name, func(t *testing.T) {
			reqURL := s.URL() + tt.reqUrl
			req, err := http.NewRequest(tt.reqMethod, reqURL, tt.reqBody)
			require.NoError(t, err)
			// Set necessary headers for CORS.
			req.Header.Set("Origin", "ghoti")
			req.Header.Set("Access-Control-Request-Method", tt.reqMethod)

			res, err := cl.Do(req)
			require.NoError(t, err)
			res.Body.Close()
			require.Equal(t, http.StatusOK, res.StatusCode)

			require.Equal(t, "*", res.Header.Get("Access-Control-Allow-Origin"))

			gotContentType := res.Header.Get("Content-Type")
			require.True(t, strings.HasPrefix(gotContentType, tt.wantContentType), "expected "+tt.wantContentType)

			// Assert the endpoint supports OPTIONS as required by CORS.
			optReq, err := http.NewRequest(http.MethodOptions, reqURL, nil)
			require.NoError(t, err)
			res, err = cl.Do(optReq)
			require.NoError(t, err)
			res.Body.Close()
			require.Equal(t, http.StatusOK, res.StatusCode)
		})
	}
}

func TestServer_StreamingResponse(t *testing.T) {
	landing := landingRendered
	if runtime.GOOS == "windows" {
		// Replace newlines with whatever new line is in the current runtime environment to keep
		// windows tests happy; cause they render template with `\r\n`.
		landing = strings.ReplaceAll(landingRendered, "\n", "\r\n")
	}

	mhs := test.RandomMultihashes(10)
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	require.NoError(t, err)

	s := setupTestServer(t, indexer.Value{
		ProviderID:    p,
		ContextID:     []byte("fish"),
		MetadataBytes: []byte("lobster"),
	}, mhs)

	jsonmhs0, _ := json.Marshal(mhs[0])
	jsonmhs3, _ := json.Marshal(mhs[3])
	tests := []struct {
		name               string
		reqURI             string
		reqAccept          string
		wantContentType    string
		wantResponseStatus int
		wantResponseBody   string
	}{
		{
			name:               "mutlihash json",
			reqURI:             "/multihash/" + mhs[3].B58String(),
			reqAccept:          "ext/html,  application/json",
			wantContentType:    "application/json; charset=utf-8",
			wantResponseBody:   `{"MultihashResults":[{"Multihash":` + string(jsonmhs3) + `,"ProviderResults":[{"ContextID":"ZmlzaA==","Metadata":"bG9ic3Rlcg==","Provider":{"ID":"12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA","Addrs":["/ip4/127.0.0.1/tcp/9999"]}}]}]}`,
			wantResponseStatus: http.StatusOK,
		},
		{
			name:            "mutlihash ndjson",
			reqURI:          "/multihash/" + mhs[3].B58String(),
			reqAccept:       "application/x-ndjson,application/xhtml+xml,application/xml;q=0.9",
			wantContentType: "application/x-ndjson",
			wantResponseBody: `{"ContextID":"ZmlzaA==","Metadata":"bG9ic3Rlcg==","Provider":{"ID":"12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA","Addrs":["/ip4/127.0.0.1/tcp/9999"]}}
`,
			wantResponseStatus: http.StatusOK,
		},
		{
			name:               "cid json",
			reqURI:             "/cid/" + cid.NewCidV1(cid.Raw, mhs[0]).String(),
			reqAccept:          "application/json,ext/html,  application/xhtml+xml,application/xml;q=0.9",
			wantContentType:    "application/json; charset=utf-8",
			wantResponseBody:   `{"MultihashResults":[{"Multihash":` + string(jsonmhs0) + `,"ProviderResults":[{"ContextID":"ZmlzaA==","Metadata":"bG9ic3Rlcg==","Provider":{"ID":"12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA","Addrs":["/ip4/127.0.0.1/tcp/9999"]}}]}]}`,
			wantResponseStatus: http.StatusOK,
		},
		{
			name:            "cid ndjson",
			reqURI:          "/cid/" + cid.NewCidV1(cid.Raw, mhs[5]).String(),
			reqAccept:       "ext/html,application/xhtml+xml,application/xml;q=0.9,application/x-ndjson",
			wantContentType: "application/x-ndjson",
			wantResponseBody: `{"ContextID":"ZmlzaA==","Metadata":"bG9ic3Rlcg==","Provider":{"ID":"12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA","Addrs":["/ip4/127.0.0.1/tcp/9999"]}}
`,
			wantResponseStatus: http.StatusOK,
		},
		{
			name:               "landing",
			reqURI:             "/",
			wantContentType:    "text/html; charset=utf-8",
			wantResponseBody:   landing,
			wantResponseStatus: http.StatusOK,
		},
		{
			name:               "index.html",
			reqURI:             "/index.html",
			wantContentType:    "text/html; charset=utf-8",
			wantResponseBody:   landing,
			wantResponseStatus: http.StatusOK,
		},
		{
			name:               "unknown metadata",
			reqURI:             "/metadata/fish",
			wantContentType:    "text/plain; charset=utf-8",
			wantResponseBody:   "Not Found\n",
			wantResponseStatus: http.StatusNotFound,
		},
		{
			name:               "unknwon any",
			reqURI:             "/lobster",
			wantContentType:    "text/plain; charset=utf-8",
			wantResponseBody:   "Not Found\n",
			wantResponseStatus: http.StatusNotFound,
		},
	}

	cl := http.DefaultClient

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqURL := s.URL() + tt.reqURI
			req, err := http.NewRequest(http.MethodGet, reqURL, nil)
			require.NoError(t, err)
			req.Header.Set("Accept", tt.reqAccept)

			res, err := cl.Do(req)
			require.NoError(t, err)
			body, err := io.ReadAll(res.Body)
			res.Body.Close()
			require.NoError(t, err)
			require.Equal(t, tt.wantResponseStatus, res.StatusCode)

			gotContentType := res.Header.Get("Content-Type")
			require.Equal(t, tt.wantContentType, gotContentType)
			require.Equal(t, tt.wantResponseBody, string(body))
		})
	}
}

func TestServer_Landing(t *testing.T) {
	ind := initIndex(t, false)
	reg := initRegistry(t)
	s := setupServer(ind, reg, t)
	go func() {
		err := s.Start()
		require.ErrorIs(t, err, http.ErrServerClosed)
	}()
	t.Cleanup(func() {
		require.NoError(t, s.Close())
		require.NoError(t, ind.Close())
		reg.Close()
	})

	res, err := http.Get(s.URL() + "/")
	require.NoError(t, err)
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.NoError(t, err)
	require.Contains(t, string(body), "https://web-ipni.cid.contact/")
}

func setupTestServer(t *testing.T, iv indexer.Value, mhs []multihash.Multihash) *find.Server {
	ind := initIndex(t, false)
	reg := initRegistry(t)
	s := setupServer(ind, reg, t)
	go func() {
		err := s.Start()
		require.ErrorIs(t, err, http.ErrServerClosed)
	}()
	t.Cleanup(func() {
		require.NoError(t, s.Close())
		require.NoError(t, ind.Close())
		reg.Close()
	})

	err := ind.Put(iv, mhs...)
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

	return s
}
