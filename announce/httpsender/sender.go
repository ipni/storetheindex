package httpsender

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/ipni/storetheindex/announce/message"
	"github.com/libp2p/go-libp2p/core/peer"
)

const DefaultAnnouncePath = "/announce"

type Sender struct {
	announceURLs []string
	client       *http.Client
	peerID       peer.ID
}

// New creates a new Sender that sends announce messages over HTTP. Announce
// messages are sent to the specified URLs. The addresses in announce messages
// are modified to include the specified peerID, which is necessary to
// communicate the publisher ID over HTTP.
func New(announceURLs []*url.URL, peerID peer.ID, options ...Option) (*Sender, error) {
	if len(announceURLs) == 0 {
		return nil, errors.New("no announce urls")
	}
	err := peerID.Validate()
	if err != nil {
		return nil, err
	}

	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	client := opts.client
	if client == nil {
		client = &http.Client{
			Timeout: opts.timeout,
		}
	}

	urls := make([]string, 0, len(announceURLs))
	seen := make(map[string]struct{}, len(announceURLs))
	for _, u := range announceURLs {
		if u.Path == "" {
			u.Path = DefaultAnnouncePath
		}
		ustr := u.String()
		if _, ok := seen[ustr]; ok {
			// Skip duplicate.
			continue
		}
		seen[ustr] = struct{}{}
		urls = append(urls, ustr)
	}

	return &Sender{
		announceURLs: urls,
		client:       client,
		peerID:       peerID,
	}, nil
}

func (s *Sender) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

// Send sends the Message to the announce URLs.
func (s *Sender) Send(ctx context.Context, msg message.Message) error {
	err := s.addIDToAddrs(&msg)
	if err != nil {
		return fmt.Errorf("cannot add p2p id to message addrs: %w", err)
	}
	buf := bytes.NewBuffer(nil)
	if err = msg.MarshalCBOR(buf); err != nil {
		return fmt.Errorf("cannot cbor encode announce message: %w", err)
	}
	return s.sendData(ctx, buf, false)
}

func (s *Sender) SendJson(ctx context.Context, msg message.Message) error {
	err := s.addIDToAddrs(&msg)
	if err != nil {
		return fmt.Errorf("cannot add p2p id to message addrs: %w", err)
	}
	buf := new(bytes.Buffer)
	if err = json.NewEncoder(buf).Encode(msg); err != nil {
		return fmt.Errorf("cannot json encode announce message: %w", err)
	}
	return s.sendData(ctx, buf, true)
}

// addIDToAddrs adds the peerID to each of the multiaddrs in the message. This
// is necessay to communicate the publisher ID when sending an announce over
// HTTP.
func (s *Sender) addIDToAddrs(msg *message.Message) error {
	if len(msg.Addrs) == 0 {
		return nil
	}
	addrs, err := msg.GetAddrs()
	if err != nil {
		return fmt.Errorf("cannot get addrs from message: %s", err)
	}
	ai := peer.AddrInfo{
		ID:    s.peerID,
		Addrs: addrs,
	}
	p2pAddrs, err := peer.AddrInfoToP2pAddrs(&ai)
	if err != nil {
		return err
	}
	msg.SetAddrs(p2pAddrs)
	return nil
}

func (s *Sender) sendData(ctx context.Context, buf *bytes.Buffer, js bool) error {
	if len(s.announceURLs) < 2 {
		u := s.announceURLs[0]
		err := s.sendAnnounce(ctx, u, buf, js)
		if err != nil {
			return fmt.Errorf("failed to send http announce message to %s: %w", u, err)
		}
		return nil
	}

	errChan := make(chan error)
	data := buf.Bytes()
	for _, u := range s.announceURLs {
		// Send HTTP announce to indexers concurrently. If context is canceled,
		// then requests will be canceled.
		go func(announceURL string) {
			err := s.sendAnnounce(ctx, announceURL, bytes.NewBuffer(data), js)
			if err != nil {
				errChan <- fmt.Errorf("failed to send http announce to %s: %w", announceURL, err)
				return
			}
			errChan <- nil
		}(u)
	}
	var errs error
	for i := 0; i < len(s.announceURLs); i++ {
		err := <-errChan
		if err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	return errs
}

func (s *Sender) sendAnnounce(ctx context.Context, announceURL string, buf *bytes.Buffer, js bool) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, announceURL, buf)
	if err != nil {
		return err
	}
	if js {
		req.Header.Set("Content-Type", "application/json")
	} else {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("%d %s: %s", resp.StatusCode, http.StatusText(resp.StatusCode), strings.TrimSpace(string(body)))
	}
	return nil
}
