package loadgen

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	mathrand "math/rand"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/dsadapter"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/announce/p2psender"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	ingestclient "github.com/ipni/go-libipni/ingest/client"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/libp2p/go-libp2p"
	libp2pconfig "github.com/libp2p/go-libp2p/config"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
)

type LoadGenOpts struct {
	IndexerAddr            string
	ConcurrentProviders    uint
	ExternalAddressMapping map[string]string
	ListenForInterrupt     bool
}

func StartLoadGen(ctx context.Context, loadConfig Config, loadGenOpts LoadGenOpts) {
	stopPublishingFns := make([]func(), 0, loadGenOpts.ConcurrentProviders)
	closeFns := make([]func(), 0, loadGenOpts.ConcurrentProviders)

	fmt.Println("Starting load generator", loadGenOpts)
	for i := uint(0); i < loadGenOpts.ConcurrentProviders; i++ {
		configCopy := loadConfig
		configCopy.Seed = loadConfig.Seed + uint64(i)
		var err error
		configCopy.ListenMultiaddr, err = incrementListenMultiaddrPortBy(loadConfig.ListenMultiaddr, i)
		if err != nil {
			panic("Failed to increment listen multiaddr: " + err.Error())
		}
		configCopy.HttpListenAddr, err = incrementHttpListenPortBy(loadConfig.HttpListenAddr, i)
		if err != nil {
			panic("Failed to increment http listen multiaddr: " + err.Error())
		}
		fmt.Println("Config is ", configCopy)
		stopPublishing, close, err := startProviderLoadGen(configCopy, loadGenOpts.IndexerAddr, loadGenOpts.ExternalAddressMapping)
		if err != nil {
			panic("Failed to start provider: " + err.Error())
		}
		stopPublishingFns = append(stopPublishingFns, stopPublishing)
		closeFns = append(closeFns, close)
	}

	ch := make(chan os.Signal, 1)
	if loadGenOpts.ListenForInterrupt {
		signal.Notify(ch, os.Interrupt)
		<-ch
	} else {
		<-ctx.Done()
	}
	for _, fn := range stopPublishingFns {
		fn()
	}

	fmt.Println("New publishing stopped. Hit ctrl-c again to exit.")
	if loadGenOpts.ListenForInterrupt {
		<-ch
	} else {
		<-ctx.Done()
	}

	for _, fn := range closeFns {
		fn()
	}
}

func startProviderLoadGen(config Config, indexerHttpAddr string, addressMapping map[string]string) (stopGenAds func(), close func(), err error) {
	p := newProviderLoadGen(config, indexerHttpAddr, addressMapping)

	fmt.Printf("Provider seed=%d ID=%v\n", p.config.Seed, p.h.ID())

	var afterEachUpdate func()
	if !config.IsHttp {
		afterEachUpdate = func() {
			err := p.announce()
			if err != nil {
				panic("Failed to announce: " + err.Error())
			}
		}
	}
	stopGenAds = p.runUpdater(afterEachUpdate)

	close = func() {}
	if config.IsHttp {
		close = p.announceInBackground()
	}
	fmt.Println("Started provider load generator")
	fmt.Println("Peer ID:", p.h.ID().String())
	fmt.Println("Addrs:", p.h.Addrs())
	fmt.Println()

	return stopGenAds, close, nil
}

func newProviderLoadGen(c Config, indexerHttpAddr string, addressMapping map[string]string) *providerLoadGen {
	lsys := cidlink.DefaultLinkSystem()
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	store := &dsadapter.Adapter{
		Wrapped: ds,
	}
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)

	pseudoRandReader := newPseudoRandReaderFrom(mathrand.NewSource(int64(c.Seed)))
	signingKey, _, err := crypto.GenerateEd25519Key(pseudoRandReader)
	if err != nil {
		panic("Failed to generate signing key")
	}

	host, err := libp2p.New(libp2p.ListenAddrStrings(c.ListenMultiaddr), libp2p.Identity(signingKey), libp2p.AddrsFactory(newAddrsFactory(addressMapping)), libp2p.ResourceManager(&network.NullResourceManager{}))
	if err != nil {
		panic("Failed to start host" + err.Error())
	}

	var pub dagsync.Publisher
	if c.IsHttp {
		pub, err = ipnisync.NewPublisher(lsys, signingKey, ipnisync.WithHTTPListenAddrs(c.HttpListenAddr))
	} else {
		pub, err = ipnisync.NewPublisher(lsys, signingKey, ipnisync.WithStreamHost(host), ipnisync.WithHeadTopic(c.GossipSubTopic))
	}
	if err != nil {
		panic("Failed to start publisher: " + err.Error())
	}
	sender, err := p2psender.New(host, c.GossipSubTopic)
	if err != nil {
		panic("Failed to start announce sender: " + err.Error())
	}

	p := &providerLoadGen{
		indexerHttpAddr: indexerHttpAddr,
		config:          c,
		signingKey:      signingKey,
		h:               host,
		lsys:            lsys,
		pub:             pub,
		sender:          sender,
	}

	return p
}

type providerLoadGen struct {
	indexerHttpAddr string
	config          Config
	signingKey      crypto.PrivKey
	h               host.Host
	lsys            ipld.LinkSystem
	pub             dagsync.Publisher
	// Keep track of the total number of entries we've created. There are a couple uses for this:
	//   * At the end we can tell the user how many entries we've created.
	//   * We can generate multihashes as a function of this number. E.g. the multihash is just an encoded version of the entry number.
	entriesGenerated uint
	adsGenerated     uint
	currentHead      ipld.Link
	recordKeepingMu  sync.Mutex
	sender           announce.Sender
}

func (p *providerLoadGen) announce() error {
	client, err := ingestclient.New(p.indexerHttpAddr)
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

	if p.currentHead == nil {
		// Nothing to announce
		return nil
	}
	return client.Announce(context.Background(), &peer.AddrInfo{ID: p.h.ID(), Addrs: addrs}, (p.currentHead).(cidlink.Link).Cid)
}

func (p *providerLoadGen) announceInBackground() func() {
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

func (p *providerLoadGen) runUpdater(afterEachUpdate func()) func() {
	closer := make(chan struct{})

	go func() {
		t := time.NewTicker(time.Second / time.Duration(p.config.AdsPerSec))
		for {
			select {
			case <-closer:
				return
			case <-t.C:
				start := time.Now()
				p.recordKeepingMu.Lock()
				var addrs []string
				for _, a := range p.h.Addrs() {
					addrs = append(addrs, a.String())
				}
				adBuilder := adBuilder{
					mhGenerator:     p.mhGenerator,
					entryCount:      p.config.EntriesPerAdGenerator(),
					entriesPerChunk: p.config.EntriesPerChunk,
					isRm:            false,
					provider:        p.h.ID().String(),
					providerAddrs:   addrs,
					metadata:        fmt.Appendf(nil, "providerSeed=%d,entriesGenerated=%d", p.config.Seed, p.entriesGenerated),
				}

				nextAdHead, err := adBuilder.build(p.lsys, p.signingKey, p.currentHead)
				if err != nil {
					panic(fmt.Sprintf("Failed to build ad: %s", err))
				}

				p.currentHead = nextAdHead
				p.adsGenerated = p.adsGenerated + 1
				p.entriesGenerated = adBuilder.entryCount + p.entriesGenerated
				fmt.Printf("ID=%d .Number of generated entries: %d\n", p.config.Seed, p.entriesGenerated)

				p.recordKeepingMu.Unlock()

				p.pub.SetRoot(nextAdHead.(cidlink.Link).Cid)
				err = announce.Send(context.Background(), nextAdHead.(cidlink.Link).Cid, p.h.Addrs(), p.sender)
				if err != nil {
					panic(fmt.Sprintf("Failed to publish ad: %s", err))
				}

				if afterEachUpdate != nil {
					afterEachUpdate()
				}
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

func GenerateMH(nodeID uint64, entryNumber uint64) (multihash.Multihash, error) {
	nodeIDVarInt := varint.ToUvarint(nodeID)
	nVarInt := varint.ToUvarint(entryNumber)
	b := append(nodeIDVarInt, nVarInt...)
	// Identity hash for debugging
	// return multihash.Sum(b, multihash.IDENTITY, -1)
	return multihash.Sum(b, multihash.SHA2_256, -1)
}

func (p *providerLoadGen) mhGenerator(entryNumberWithinAd uint) (multihash.Multihash, error) {
	i := p.entriesGenerated + entryNumberWithinAd
	return GenerateMH(uint64(p.config.Seed), uint64(i))
}

type adBuilder struct {
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

func (b adBuilder) build(lsys ipld.LinkSystem, signingKey crypto.PrivKey, prevAd ipld.Link) (ipld.Link, error) {
	contextID := fmt.Appendf(nil, "%d", b.contextID)
	metadata := b.metadata
	var entriesLink ipld.Link
	if b.entryCount == 0 {
		entriesLink = ipld.Link(schema.NoEntries)
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
			splitIdx := max(len(allMhs)-int(b.entriesPerChunk), 0)
			mhChunk := allMhs[splitIdx:]
			allMhs = allMhs[:splitIdx]
			var err error
			entriesNode, err := schema.EntryChunk{
				Entries: mhChunk,
				Next:    entriesLink,
			}.ToNode()
			if err != nil {
				return nil, err
			}

			entriesLinkV, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, entriesNode)
			if err != nil {
				return nil, err
			}
			if entriesLinkV != nil {
				entriesLink = entriesLinkV
			}
		}
	}
	ad := schema.Advertisement{
		PreviousID: prevAd,
		Provider:   b.provider,
		Addresses:  b.providerAddrs,
		Entries:    entriesLink,
		ContextID:  contextID,
		Metadata:   metadata,
		IsRm:       b.isRm,
	}
	ad.Sign(signingKey)

	adNode, err := ad.ToNode()
	if err != nil {
		return nil, err
	}

	adLink, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, adNode)
	return adLink, err
}

func newAddrsFactory(ipMapping map[string]string) libp2pconfig.AddrsFactory {
	if ipMapping == nil {
		ipMapping = map[string]string{}
	}
	return func(ms []multiaddr.Multiaddr) []multiaddr.Multiaddr {
		var out []multiaddr.Multiaddr
		for _, ma := range ms {
			v, err := ma.ValueForProtocol(multiaddr.P_IP4)
			if err != nil || ipMapping[v] == "" {
				out = append(out, ma)
				continue
			}

			var mappedComponents multiaddr.Multiaddr
			//multiaddr.ForEach(ma, func(c multiaddr.Component) bool {
			for _, c := range ma {
				if c.Protocol().Code == multiaddr.P_IP4 && ipMapping[c.Value()] != "" {
					nextComponent, err := multiaddr.NewComponent(c.Protocol().Name, ipMapping[c.Value()])
					if err != nil {
						panic("Failed to map multiaddr")
					}
					mappedComponents = append(mappedComponents, *nextComponent)
				} else {
					mappedComponents = append(mappedComponents, c)
				}
			}
			out = append(out, mappedComponents)
		}
		return out
	}
}
