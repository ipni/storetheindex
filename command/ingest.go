package command

import (
	"fmt"
	httpclient "github.com/filecoin-project/storetheindex/internal/httpclient"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/urfave/cli/v2"
	"io"
	"net/http"
	"net/url"
)

const (
	adminPort      = 3002
	ingestResource = "ingest"
)

var subscribe = &cli.Command{
	Name:   "subscribe",
	Usage:  "Subscribe indexer with provider",
	Flags:  ingestFlags,
	Action: subscribeCmd,
}

var unsubscribe = &cli.Command{
	Name:   "unsubscribe",
	Usage:  "Unsubscribe indexer from provider",
	Flags:  ingestFlags,
	Action: unsubscribeCmd,
}

var sync = &cli.Command{
	Name:   "sync",
	Usage:  "Sync indexer with provider",
	Flags:  ingestFlags,
	Action: syncCmd,
}

var IngestCmd = &cli.Command{
	Name:  "ingest",
	Usage: "Admin commands to manage ingestion config of indexer",
	Subcommands: []*cli.Command{
		subscribe,
		unsubscribe,
		sync,
	},
}

type ingestClient struct {
	c       *http.Client
	baseurl *url.URL
}

func newIngestClient(baseurl string) (*ingestClient, error) {
	url, c, err := httpclient.NewClient(baseurl, ingestResource, adminPort)
	if err != nil {
		return nil, err
	}
	return &ingestClient{
		c,
		url,
	}, nil
}

func sendRequest(cctx *cli.Context, action string) error {
	cl, err := newIngestClient(cctx.String("indexer"))
	if err != nil {
		return err
	}
	prov := cctx.String("provider")
	p, err := peer.Decode(prov)
	if err != nil {
		return err
	}
	dest := fmt.Sprintf("%s/ingest/%s/%s", cl.baseurl, action, p.String())
	req, err := http.NewRequestWithContext(cctx.Context, "GET", dest, nil)
	if err != nil {
		return err
	}

	resp, err := cl.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return httpclient.ReadError(resp.StatusCode, body)
	}
	return nil
}

func subscribeCmd(cctx *cli.Context) error {
	err := sendRequest(cctx, "subscribe")
	if err != nil {
		return err
	}
	log.Errorf("Successfully subscribed to provider")
	return nil
}

func unsubscribeCmd(cctx *cli.Context) error {
	err := sendRequest(cctx, "unsubscribe")
	if err != nil {
		return err
	}
	log.Errorf("Successfully unsubscribed from provider")
	return nil
}

func syncCmd(cctx *cli.Context) error {
	err := sendRequest(cctx, "sync")
	if err != nil {
		return err
	}
	log.Errorf("Syncing request accepted. Come back later to check if syncing was successful")
	return nil
}
