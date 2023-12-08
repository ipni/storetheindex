package reaper_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipni/go-indexer-core/store/memory"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/pcache"
	"github.com/ipni/storetheindex/filestore"
	"github.com/ipni/storetheindex/ipni-gc/reaper"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

const testTopic = "/indexer/ingest/test"

var pid1, pid2 peer.ID

func init() {
	var err error
	pid1, err = peer.Decode("12D3KooWNSRG5wTShNu6EXCPTkoH7dWsphKAPrbvQchHa5arfsDC")
	if err != nil {
		panic(err)
	}
	pid2, err = peer.Decode("12D3KooWHf7cahZvAVB36SGaVXc7fiVDoJdRJq42zDRcN2s2512h")
	if err != nil {
		panic(err)
	}
}

type mockSource struct {
	infos []*model.ProviderInfo
}

func TestReaper(t *testing.T) {
	tmpDir := "/tmp/rtest" //t.TempDir()
	dsDir := filepath.Join(tmpDir, "gcdatastore")
	dsTmpDir := filepath.Join(tmpDir, "gctmpdata")

	fileStore, err := filestore.NewLocal(tmpDir)
	require.NoError(t, err)

	idxr := memory.New()

	src := newMockSource(pid1)
	pc, err := pcache.New(pcache.WithSource(src))
	require.NoError(t, err)

	grim, err := reaper.New(idxr, fileStore,
		reaper.WithCarDelete(true),
		reaper.WithCarRead(true),
		reaper.WithCommit(true),
		reaper.WithDatastoreDir(dsDir),
		reaper.WithDatastoreTempDir(dsTmpDir),
		reaper.WithPCache(pc),
		reaper.WithTopicName(testTopic),
	)
	require.NoError(t, err)
	defer grim.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = grim.Reap(ctx, pid1)
	require.NoError(t, err)

	// Check that archive is stored in filestore.
	archiveName, err := grim.DataArchiveName(ctx, pid1)
	require.NoError(t, err)
	fileInfo, err := fileStore.Head(ctx, archiveName)
	require.NoError(t, err)
	require.NotZero(t, fileInfo.Size)
}

func newMockSource(pids ...peer.ID) *mockSource {
	s := &mockSource{}
	for _, pid := range pids {
		s.addInfo(pid)
	}
	return s
}

func (s *mockSource) addInfo(pid peer.ID) {
	maddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/192.168.0.%d/tcp/24001", len(s.infos)+2))
	if err != nil {
		panic(err)
	}
	maddr2, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/192.168.0.%d/tcp/24001", len(s.infos)+3))
	if err != nil {
		panic(err)
	}
	info := &model.ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    pid,
			Addrs: []multiaddr.Multiaddr{maddr},
		},
		Publisher: &peer.AddrInfo{
			ID:    pid2,
			Addrs: []multiaddr.Multiaddr{maddr2},
		},
		LastAdvertisementTime: time.Now().Format(time.RFC3339),
	}
	s.infos = append(s.infos, info)
}

func (s *mockSource) Fetch(ctx context.Context, pid peer.ID) (*model.ProviderInfo, error) {
	for _, info := range s.infos {
		if pid == info.AddrInfo.ID {
			return info, nil
		}
	}
	return nil, nil
}

func (s *mockSource) FetchAll(ctx context.Context) ([]*model.ProviderInfo, error) {
	return s.infos, nil
}

func (s *mockSource) String() string {
	return "mockSource"
}
