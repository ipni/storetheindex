package httpsync_test

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"testing"

	"github.com/filecoin-project/storetheindex/dagsync/httpsync"
	"github.com/filecoin-project/storetheindex/dagsync/httpsync/maconv"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

const (
	// Sample data extracted directly from http://ipfs-advertisement.s3.us-west-2.amazonaws.com

	sampleNFTStorageCid  = "baguqeeranpqrweyey2zsab2mmt33ixc3jkg27p5ri3abr5di2pbbsbaig74q"
	sampleNFTStorageHead = `{"head":{"/":"` + sampleNFTStorageCid + `"},"pubkey":{"/":{"bytes":"CAASpgIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDYi9qW5J1UIE4CUaRxxoROyqHkKZ2nhGRGWDurhNhPiNQ+n0sCy8rREEF9lertFt2n81c1Ik4W/8HZKxvk8PYKStrlGWjur6UoFyt+WuS/1hkRVyqEXjzBF7cLvfYQ75UaATIbLhXWpXqys1DVdh2snD0jnDugF4J72ZboIz6gwZC+BEd5axeVaibB9gJcg+5P48ihq9SAbr4dQUS47OgISMNb3f6nHfK7FQFF/KYx80byJYMJ9Oxsw8CB6C8pmDTdqvzYBT9kCUdY+loN/IcBqEeNw/UF7l3ay/ZJ2Yq437k6kn5BoxaZfxlbZHItoBjiLSJ9FSD7gpnUO+lJAh9bAgMBAAE="}},"sig":{"/":{"bytes":"NYkxmG812wa4DOCsZwH7NGLDERRkwtVwLNykf61RFug5VWNB1mKQjp0M3g0EBhVlf4dWqeh7ZCeVIC1qhAIONKw9VBAq4ITi4DTOlpx4yFphcNcCPGWNSfV0Qlosct6r64VmA4KtnRlYhwf6EsZ0gxcnZySbsv7KENHttmXLmO0ZBNQzG8dBNrp6thwiJbK4A1mw6+J6Ut4VzVwFUzJjONQzc0RpvUV7MwD1l6ZP83ucuOUT4Vw1F2yjVnFDgEa14N2tJfhxw2ZY2mHEiPH1pJL1dVxcjXRkiILV3V/qy1W5/cw+HhQHM3BIAgSBeWwSb7gbculHuBnOPDhHVKBmuQ=="}}}`
	sampleNFTStorageAd   = `{"Addresses":["/dns4/elastic.dag.house/tcp/443/wss"],"ContextID":{"/":{"bytes":"YmFndXFlZXJhdW1qbGM3MjRhenFucmRiNXh3dDJ3bWdxMmZ4N2lrd2N6MmxtNHhlNWR0dWx4NHIzemQycQ=="}},"Entries":{"/":"baguqeeraumjlc724azqnrdb5xwt2wmgq2fx7ikwcz2lm4xe5dtulx4r3zd2q"},"IsRm":false,"Metadata":{"/":{"bytes":"gBI"}},"PreviousID":{"/":"baguqeeramj6uf7ie5brhk5ivzi7e4mccndzke6fizc4qaie6t73xrvspkxrq"},"Provider":"bafzbeibhqavlasjc7dvbiopygwncnrtvjd2xmryk5laib7zyjor6kf3avm","Signature":{"/":{"bytes":"CqsCCAASpgIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDYi9qW5J1UIE4CUaRxxoROyqHkKZ2nhGRGWDurhNhPiNQ+n0sCy8rREEF9lertFt2n81c1Ik4W/8HZKxvk8PYKStrlGWjur6UoFyt+WuS/1hkRVyqEXjzBF7cLvfYQ75UaATIbLhXWpXqys1DVdh2snD0jnDugF4J72ZboIz6gwZC+BEd5axeVaibB9gJcg+5P48ihq9SAbr4dQUS47OgISMNb3f6nHfK7FQFF/KYx80byJYMJ9Oxsw8CB6C8pmDTdqvzYBT9kCUdY+loN/IcBqEeNw/UF7l3ay/ZJ2Yq437k6kn5BoxaZfxlbZHItoBjiLSJ9FSD7gpnUO+lJAh9bAgMBAAESGy9pbmRleGVyL2luZ2VzdC9hZFNpZ25hdHVyZRoiEiAz1niaKM3G2J40Bz/3wQbElyuBh1+2Q1E9SBj9wNsE8iqAAgOO1BKwq1RRy7AkZksWRrDlClhXU5IHAiy9pHuYtI/ePbVANiMAisjIEkd7jtJx7uct+/q2BTTTVcmZS7iE4OMTUymVbPQJ21qrzB6l5hulKD5ieedkJngAPCpizXmI1Z32Ib1zkuEFMraRcFaQ0YWqBKoIBJjjO4POGIdB2SgrCO0aFSd94k+2lyudMeWK+OisGLI7r6+ovd8g1VmcspEgl6pfdlHvThM3TdYGa46LO3kSCZmTzbI/XPnbMKaITvbuS3p8gm6elxNagx7Jxw4oP7hVyINSJ9chRu/w0RiBO986WRwhkGz1jW5jUF6VG/cQODzwjhuACteHf9TWp18"}}}`
)

func TestHttpSync_NFTStorage_DigestCheck(t *testing.T) {
	pubid, err := peer.Decode("QmQzqxhK82kAmKvARFZSkUVS6fo9sySaiogAnx5EnZ6ZmC")
	require.NoError(t, err)
	tests := []struct {
		name, headCid, head, headAd, wantErr string
	}{
		{
			name:    "mismatching hash is not synced",
			headCid: sampleNFTStorageCid,
			head:    sampleNFTStorageHead,
			headAd:  "fish",
			wantErr: "hash digest mismatch",
		},
		{
			name:    "technically invalid but matching digest is synced",
			headCid: sampleNFTStorageCid,
			head:    sampleNFTStorageHead,
			headAd:  sampleNFTStorageAd,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			pub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch path.Base(r.URL.Path) {
				case "head":
					_, err := w.Write([]byte(test.head))
					require.NoError(t, err)
				case test.headCid:
					_, err := w.Write([]byte(test.headAd))
					require.NoError(t, err)
				default:
					http.NotFound(w, r)
				}
			}))
			defer pub.Close()

			ls := cidlink.DefaultLinkSystem()
			store := &memstore.Store{}
			ls.SetWriteStorage(store)
			ls.SetReadStorage(store)

			puburl, err := url.Parse(pub.URL)
			require.NoError(t, err)
			pubmaddr, err := maconv.ToMultiaddr(puburl)
			require.NoError(t, err)

			sync := httpsync.NewSync(ls, http.DefaultClient, nil)
			syncer, err := sync.NewSyncer(pubid, pubmaddr, nil)
			require.NoError(t, err)

			head, err := syncer.GetHead(ctx)
			require.NoError(t, err)

			err = syncer.Sync(ctx, head, selectorparse.CommonSelector_MatchPoint)

			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				_, exists := store.Bag[head.KeyString()]
				require.False(t, exists)
			} else {
				require.NoError(t, err)
				_, exists := store.Bag[head.KeyString()]
				require.True(t, exists)

				// Assert that, even though the CID does not match the computed link
				// the original CID can be loaded from the linksystem.
				wantLink := cidlink.Link{Cid: head}
				node, err := ls.Load(ipld.LinkContext{Ctx: ctx}, wantLink, basicnode.Prototype.Any)
				require.NoError(t, err)

				gotLink, err := ls.ComputeLink(wantLink.Prototype(), node)
				require.NoError(t, err)
				require.NotEqual(t, gotLink, wantLink)
			}
		})
	}
}

func TestHttpsync_AcceptsSpecCompliantDagJson(t *testing.T) {
	ctx := context.Background()

	pubPrK, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	require.NoError(t, err)
	pubID, err := peer.IDFromPrivateKey(pubPrK)
	require.NoError(t, err)
	var pubAddr multiaddr.Multiaddr

	// Instantiate a dagsync publisher.
	{
		publs := cidlink.DefaultLinkSystem()
		pubstore := &memstore.Store{}
		publs.SetWriteStorage(pubstore)
		publs.SetReadStorage(pubstore)

		pub, err := httpsync.NewPublisher("0.0.0.0:0", publs, pubID, pubPrK)
		require.NoError(t, err)
		pubAddr = pub.Addrs()[0]
		t.Cleanup(func() { require.NoError(t, pub.Close()) })

		link, err := publs.Store(
			ipld.LinkContext{Ctx: ctx},
			cidlink.LinkPrototype{
				Prefix: cid.Prefix{
					Version:  1,
					Codec:    uint64(multicodec.DagJson),
					MhType:   uint64(multicodec.Sha2_256),
					MhLength: -1,
				},
			},
			fluent.MustBuildMap(basicnode.Prototype.Map, 4, func(na fluent.MapAssembler) {
				na.AssembleEntry("fish").AssignString("lobster")
				na.AssembleEntry("fish1").AssignString("lobster1")
				na.AssembleEntry("fish2").AssignString("lobster2")
				na.AssembleEntry("fish0").AssignString("lobster0")
			}))
		require.NoError(t, err)
		require.NoError(t, pub.SetRoot(ctx, link.(cidlink.Link).Cid))
	}

	ls := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	ls.SetWriteStorage(store)
	ls.SetReadStorage(store)

	sync := httpsync.NewSync(ls, http.DefaultClient, nil)
	syncer, err := sync.NewSyncer(pubID, pubAddr, nil)
	require.NoError(t, err)

	head, err := syncer.GetHead(ctx)
	require.NoError(t, err)

	err = syncer.Sync(ctx, head, selectorparse.CommonSelector_MatchPoint)
	require.NoError(t, err)

	// Assert that data is loadable from the link system.
	wantLink := cidlink.Link{Cid: head}
	node, err := ls.Load(ipld.LinkContext{Ctx: ctx}, wantLink, basicnode.Prototype.Any)
	require.NoError(t, err)

	// Assert synced node link matches the computed link, i.e. is spec-compliant.
	gotLink, err := ls.ComputeLink(wantLink.Prototype(), node)
	require.NoError(t, err)
	require.Equal(t, gotLink, wantLink, "computed %s but got %s", gotLink.String(), wantLink.String())
}
