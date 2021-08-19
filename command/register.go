package command

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/models"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

var RegisterCmd = &cli.Command{
	Name:   "register",
	Usage:  "Register a trusted provider with an indexer",
	Flags:  RegisterFlags,
	Action: registerCommand,
}

func registerCommand(cctx *cli.Context) error {
	cfg, err := config.Load(cctx.String("config"))
	if err != nil {
		if err == config.ErrNotInitialized {
			err = errors.New("config file not found")
		}
		return fmt.Errorf("cannot load config file: %w", err)
	}

	providerID, err := peer.Decode(cfg.Identity.PeerID)
	if err != nil {
		return fmt.Errorf("could not decode peer id: %s", err)
	}

	privKey, err := cfg.Identity.DecodePrivateKey("")
	if err != nil {
		return fmt.Errorf("could not decode private key: %s", err)
	}

	maddrs := make([]multiaddr.Multiaddr, len(cctx.StringSlice("provider-addr")))
	for i, m := range cctx.StringSlice("provider-addr") {
		maddrs[i], err = multiaddr.NewMultiaddr(m)
		if err != nil {
			return fmt.Errorf("bad provider address: %s", err)
		}
	}

	regReq := &models.RegisterRequest{
		AddrInfo: peer.AddrInfo{
			ID:    providerID,
			Addrs: maddrs,
		},
	}

	if err = regReq.Sign(privKey); err != nil {
		return fmt.Errorf("cannot sign request: %s", err)
	}

	j, err := json.Marshal(regReq)
	if err != nil {
		return err
	}
	reqBody := bytes.NewBuffer(j)

	iaddr := cctx.String("indexer-addr")

	url := path.Join(iaddr, "providers")
	if !strings.HasPrefix(iaddr, "http://") {
		url = fmt.Sprint("http://", url)
	}

	resp, err := http.DefaultClient.Post(url, "application/json", reqBody)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var e v0.Error
		if len(body) != 0 {
			_ = json.Unmarshal(body, &e)
		}
		return fmt.Errorf("failed to register at %s: %s", url, e.Message)
	}

	fmt.Println("Registered provider", cfg.Identity.PeerID, "at indexer", iaddr)
	return nil
}
