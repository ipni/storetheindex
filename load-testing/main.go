package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	mathrand "math/rand"

	"github.com/dustin/randbo"
	"github.com/filecoin-project/go-legs"
	legsDT "github.com/filecoin-project/go-legs/dtsync"
	legsHttp "github.com/filecoin-project/go-legs/httpsync"
	httpfinderclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/http"
	ingesthttpclient "github.com/filecoin-project/storetheindex/api/v0/ingest/client/http"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/load-testing/dsadapter"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	libp2pconfig "github.com/libp2p/go-libp2p/config"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
)

const loadTestProtocolID = 0x300001

func verify(indexerHost string, nodeSeed uint64, upTo uint, randomQuery bool) error {
	client, err := httpfinderclient.New(indexerHost)
	if err != nil {
		return err
	}
	var allMhs []multihash.Multihash
	allMhsSet := map[string]bool{}

	numberOfMhsToQuery := upTo
	if randomQuery {
		numberOfMhsToQuery = 1000
	}
	for i := uint(0); i < numberOfMhsToQuery; i++ {
		multihashIndex := uint64(i)
		if randomQuery {
			multihashIndex = uint64(mathrand.Int63n(int64(upTo)))
		}
		mh, err := generateMH(nodeSeed, multihashIndex)
		if err != nil {
			return err
		}
		allMhs = append(allMhs, mh)
		allMhsSet[mh.B58String()] = true
	}
	start := time.Now()
	resp, err := client.Find(context.Background(), allMhs[0])
	if err != nil {
		return err
	}
	for _, result := range resp.MultihashResults {
		delete(allMhsSet, result.Multihash.B58String())
	}

	if len(allMhsSet) != 0 {
		limitToShow := 10
		for mh := range allMhsSet {
			fmt.Println("Missing:", mh)
			limitToShow--
			if limitToShow <= 0 {
				break
			}
		}
	}

	fmt.Printf("Found %d out of %d (%02d%%)", len(resp.MultihashResults), len(allMhs), int(float64(len(resp.MultihashResults))/float64(len(allMhs))*100))
	fmt.Println()
	fmt.Println("Find took", time.Since(start))
	return nil
}

func ipMappingFunc(ipKVs string) map[string]string {
	out := map[string]string{}
	if ipKVs == "" {
		return out
	}
	kvs := strings.Split(ipKVs, ",")
	for _, kv := range kvs {
		parts := strings.Split(kv, "=")
		k := parts[0]
		v := parts[1]
		out[k] = v
	}
	return out
}

func NewAddrsFactory(ipMapping map[string]string) libp2pconfig.AddrsFactory {
	return func(ms []multiaddr.Multiaddr) []multiaddr.Multiaddr {
		var out []multiaddr.Multiaddr
		for _, ma := range ms {
			v, err := ma.ValueForProtocol(multiaddr.P_IP4)
			if err != nil || ipMapping[v] == "" {
				out = append(out, ma)
				continue
			}

			var mappedComponents []multiaddr.Multiaddr
			multiaddr.ForEach(ma, func(c multiaddr.Component) bool {
				if c.Protocol().Code == multiaddr.P_IP4 && ipMapping[c.Value()] != "" {
					nextComponent, err := multiaddr.NewComponent(c.Protocol().Name, ipMapping[c.Value()])
					if err != nil {
						panic("Failed to map multiaddr")
					}
					mappedComponents = append(mappedComponents, nextComponent)

				} else {
					mappedComponents = append(mappedComponents, &c)
				}
				return true
			})
			out = append(out, multiaddr.Join(mappedComponents...))
		}
		return out
	}
}

// type AddrsFactory func([]ma.Multiaddr) []ma.Multiaddr

func main() {
	configFile := flag.String("config", "", "Config file to use")
	doVerify := flag.Bool("verify", false, "Verify ads were ingested")
	doVerifyRandom := flag.Bool("verifyRandom", false, "Verify ads were ingested by randomly querying address space")
	announceOnly := flag.Bool("announceOnly", false, "Announce to the indexer that this node should be synced")
	peerIDOnly := flag.Bool("peerIDOnly", false, "Print the peer id and quit")
	externalAddressMapping := flag.String("external-address-mapping", "", "Comma separated list of k=v pairs of local address mapping to external address. Useful when you are behind a nat and know your external address. e.g. 127.0.0.1=1.1.4.2")
	indexerAddr := flag.String("indexerAddr", "http://localhost:3001", "HTTP Address of the indexer endpoint e.g. http://localhost:3001")
	indexerFindAddr := flag.String("indexerFindAddr", "http://localhost:3000", "HTTP Address of the indexer find endpoint e.g. http://localhost:3001")
	providerSeed := flag.Uint64("providerSeed", 1, "For verification. Seed of the provider that published these entries.")
	upto := flag.Uint("upto", 1, "When verifying, the upper limit of the entries space.")
	providerCount := flag.Uint("providerCount", 1, "How many concurrent providers")
	flag.Parse()
	if *doVerify || *doVerifyRandom {
		err := verify(*indexerFindAddr, *providerSeed, *upto, *doVerifyRandom)
		if err != nil {
			panic("verify failed: " + err.Error())
		}
		return
	}

	config := DefaultConfig()
	if *configFile != "" {
		var err error
		config, err = LoadConfigFromFile(*configFile)
		if err != nil {
			panic("Failed to load config file: " + err.Error())
		}
	}

	if *peerIDOnly {
		end := *providerSeed + 1
		if *upto != 1 {
			end = uint64(*upto)
		}
		for i := *providerSeed; i < end; i++ {
			configCopy := config
			configCopy.Seed = i
			p := newProviderLoadGen(configCopy, *indexerAddr, ipMappingFunc(*externalAddressMapping))
			fmt.Println(p.h.ID())
		}
		os.Exit(0)
	}

	if *announceOnly {
		configCopy := config
		configCopy.Seed = uint64(*providerSeed)
		var err error
		configCopy.ListenMultiaddr, err = incrementListenMultiaddrPortBy(config.ListenMultiaddr, uint(*providerSeed))
		if err != nil {
			panic("Failed to increment listen multiaddr: " + err.Error())
		}
		configCopy.HttpListenAddr, err = incrementHttpListenPortBy(config.HttpListenAddr, uint(*providerSeed))
		if err != nil {
			panic("Failed to increment http listen multiaddr: " + err.Error())
		}
		p := newProviderLoadGen(configCopy, *indexerAddr, ipMappingFunc(*externalAddressMapping))
		fmt.Println("Announcing to indexer for host", p.h.ID().String())
		fmt.Println("With multiaddr", configCopy.ListenMultiaddr)
		fmt.Println("HTTP multiaddr", configCopy.HttpListenAddr)
		err = p.announce()
		if err != nil {
			panic("Failed to announce: " + err.Error())
		}
		os.Exit(0)
	}

	stopPublishingFns := make([]func(), 0, *providerCount)
	closeFns := make([]func(), 0, *providerCount)

	for i := uint(0); i < *providerCount; i++ {
		configCopy := config
		configCopy.Seed = config.Seed + uint64(i)
		var err error
		configCopy.ListenMultiaddr, err = incrementListenMultiaddrPortBy(config.ListenMultiaddr, i)
		if err != nil {
			panic("Failed to increment listen multiaddr: " + err.Error())
		}
		configCopy.HttpListenAddr, err = incrementHttpListenPortBy(config.HttpListenAddr, i)
		if err != nil {
			panic("Failed to increment http listen multiaddr: " + err.Error())
		}
		stopPublishing, close, err := startProviderLoadGen(configCopy, *indexerAddr, ipMappingFunc(*externalAddressMapping))
		if err != nil {
			panic("Failed to start provider: " + err.Error())
		}
		stopPublishingFns = append(stopPublishingFns, stopPublishing)
		closeFns = append(closeFns, close)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	for _, fn := range stopPublishingFns {
		fn()
	}
	fmt.Println("New publishing stopped. Hit ctrl-c again to exit.")
	go signal.Notify(ch, os.Interrupt)
	<-ch

	for _, fn := range closeFns {
		fn()
	}
}

func startProviderLoadGen(config Config, indexerHttpAddr string, addressMapping map[string]string) (stopGenAds func(), close func(), err error) {
	p := newProviderLoadGen(config, indexerHttpAddr, addressMapping)

	stopGenAds = p.runUpdater(func() {
		err := p.announce()
		if err != nil {
			panic("Failed to announce: " + err.Error())
		}
	})

	close = func() {}
	if config.IsHttp {
		close = p.announceInBackground()
	}
	fmt.Println("Started provider load generator")
	fmt.Println("Peer ID:", p.h.ID().Pretty())
	fmt.Println("Addrs:", p.h.Addrs())
	fmt.Println()

	return stopGenAds, close, nil
}

func newProviderLoadGen(c Config, indexerHttpAddr string, addressMapping map[string]string) *proivderLoadGen {
	lsys := cidlink.DefaultLinkSystem()
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	store := &dsadapter.Adapter{
		Wrapped: ds,
	}
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)

	pseudoRandReader := randbo.NewFrom(mathrand.NewSource(int64(c.Seed)))
	signingKey, _, err := crypto.GenerateEd25519Key(pseudoRandReader)
	if err != nil {
		panic("Failed to generate signing key")
	}

	host, err := libp2p.New(libp2p.ListenAddrStrings(c.ListenMultiaddr), libp2p.Identity(signingKey), libp2p.AddrsFactory(NewAddrsFactory(addressMapping)), libp2p.ResourceManager(network.NullResourceManager))
	if err != nil {
		panic("Failed to start host" + err.Error())
	}

	var pub legs.Publisher
	if c.IsHttp {
		pub, err = legsHttp.NewPublisher(c.HttpListenAddr, lsys, host.ID(), signingKey)
	} else {
		pub, err = legsDT.NewPublisher(host, ds, lsys, c.GossipSubTopic)

	}
	if err != nil {
		panic("Failed to start legs publisher: " + err.Error())
	}
	p := &proivderLoadGen{
		indexerHttpAddr: indexerHttpAddr,
		config:          c,
		signingKey:      signingKey,
		h:               host,
		lsys:            lsys,
		pub:             pub,
	}

	return p
}

type proivderLoadGen struct {
	indexerHttpAddr string
	config          Config
	signingKey      crypto.PrivKey
	h               host.Host
	lsys            ipld.LinkSystem
	pub             legs.Publisher
	// Keep track of the total number of entries we've created. There are a couple uses for this:
	//   * At the end we can tell the user how many entries we've created.
	//   * We can generate multihashes as a function of this number. E.g. the multihash is just an encoded version of the entry number.
	entriesGenerated uint
	adsGenerated     uint
	currentHead      schema.Link_Advertisement
	recordKeepingMu  sync.Mutex
}

func (p *proivderLoadGen) announce() error {
	client, err := ingesthttpclient.New(p.indexerHttpAddr)
	if err != nil {
		return err
	}

	addrs := p.h.Addrs()[:1]
	if p.config.IsHttp {
		parts := strings.Split(p.config.HttpListenAddr, ":")
		httpMultiaddr := `/ip4/` + parts[0] + `/tcp/` + parts[1] + `/http`
		ma, err := multiaddr.NewMultiaddr(httpMultiaddr)
		if err != nil {
			return err
		}
		addrs = []multiaddr.Multiaddr{ma}
	}

	if p.currentHead == nil || p.currentHead.IsAbsent() {
		return client.Announce(context.Background(), &peer.AddrInfo{ID: p.h.ID(), Addrs: addrs}, cid.Undef)
	}
	return client.Announce(context.Background(), &peer.AddrInfo{ID: p.h.ID(), Addrs: addrs}, p.currentHead.ToCid())
}

func (p *proivderLoadGen) announceInBackground() func() {
	closer := make(chan struct{})
	t := time.NewTicker(2 * time.Second)
	go func() {
		for {
			select {
			case <-closer:
				return
			case <-t.C:
				p.announce()
			}

		}
	}()

	return func() {
		close(closer)
	}
}

func (p *proivderLoadGen) runUpdater(afterFirstUpdate func()) func() {
	closer := make(chan struct{})
	var callAfterFirstUpdate *sync.Once = &sync.Once{}

	go func() {
		t := time.NewTicker(time.Second / time.Duration(p.config.AdsPerSec))
		for {
			select {
			case <-closer:
				return
			case <-t.C:
				start := time.Now()
				p.recordKeepingMu.Lock()
				// TODO
				isRm := false
				var addrs []string
				for _, a := range p.h.Addrs() {
					addrs = append(addrs, a.String())
				}
				adBuilder := AdBuilder{
					mhGenerator:     p.mhGenerator,
					entryCount:      p.config.EntriesPerAdGenerator(),
					entriesPerChunk: p.config.EntriesPerChunk,
					isRm:            isRm,
					provider:        p.h.ID().String(),
					providerAddrs:   addrs,
				}

				nextAdHead, err := adBuilder.Build(p.lsys, p.signingKey, p.currentHead)
				if err != nil {
					panic(fmt.Sprintf("Failed to build ad: %s", err))
				}

				p.currentHead = nextAdHead
				p.adsGenerated = p.adsGenerated + 1
				p.entriesGenerated = adBuilder.entryCount + p.entriesGenerated
				fmt.Printf("ID=%d .Number of generated entries: %d\n", p.config.Seed, p.entriesGenerated)

				p.recordKeepingMu.Unlock()

				err = p.pub.UpdateRootWithAddrs(context.Background(), nextAdHead.ToCid(), p.h.Addrs())
				if err != nil {
					panic(fmt.Sprintf("Failed to publish ad: %s", err))
				}

				callAfterFirstUpdate.Do(afterFirstUpdate)

				fmt.Println("Published ad in", time.Since(start))

				if p.config.StopAfterNEntries > 0 && p.entriesGenerated > uint(p.config.StopAfterNEntries) {
					fmt.Printf("ID=%d finished\n", p.config.Seed)
					return
				}
			}
		}
	}()

	return func() {
		close(closer)
	}
}

func generateMH(nodeID uint64, entryNumber uint64) (multihash.Multihash, error) {
	nodeIDVarInt := varint.ToUvarint(nodeID)
	nVarInt := varint.ToUvarint(entryNumber)
	b := append(nodeIDVarInt, nVarInt...)
	// Identity hash for debugging
	// return multihash.Sum(b, multihash.IDENTITY, -1)
	return multihash.Sum(b, multihash.SHA2_256, -1)
}

func (p *proivderLoadGen) mhGenerator(entryNumberWithinAd uint) (multihash.Multihash, error) {
	i := p.entriesGenerated + entryNumberWithinAd
	return generateMH(uint64(p.config.Seed), uint64(i))
}

type AdBuilder struct {
	// mhGenerator defines how the multihash for this given entry
	mhGenerator     func(entryNumberWithinAd uint) (multihash.Multihash, error)
	entryCount      uint
	entriesPerChunk uint
	isRm            bool
	contextID       uint
	metadata        []byte
	provider        string
	providerAddrs   []string
}

func (b AdBuilder) Build(lsys ipld.LinkSystem, signingKey crypto.PrivKey, prevAd schema.Link_Advertisement) (schema.Link_Advertisement, error) {
	contextID := []byte(fmt.Sprintf("%d", b.contextID))
	metadata := b.metadata
	var entriesLink datamodel.Link
	if b.entryCount == 0 {
		entriesLink = schema.NoEntries
	} else {
		var allMhs []multihash.Multihash
		for i := uint(0); i < b.entryCount; i++ {
			mh, err := b.mhGenerator(i)
			if err != nil {
				return nil, err
			}
			allMhs = append(allMhs, mh)
		}
		for len(allMhs) > 0 {
			splitIdx := len(allMhs) - int(b.entriesPerChunk)
			if splitIdx < 0 {
				splitIdx = 0
			}
			mhChunk := allMhs[splitIdx:]
			allMhs = allMhs[:splitIdx]
			var err error
			entriesLink, _, err = schema.NewLinkedListOfMhs(lsys, mhChunk, entriesLink)
			if err != nil {
				return nil, err
			}
		}
	}
	_, adLink, err := schema.NewAdvertisementWithLink(lsys, signingKey, prevAd, entriesLink, contextID, metadata, b.isRm, b.provider, b.providerAddrs)

	return adLink, err
}
