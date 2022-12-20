package httpsender

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/ipni/storetheindex/announce/message"
)

const DefaultAnnouncePath = "/ingest/announce"

type Sender struct {
	announceURLs []string
	client       *http.Client
}

func New(announceURLs []*url.URL, options ...Option) (*Sender, error) {
	if len(announceURLs) == 0 {
		return nil, errors.New("no announce urls")
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
	}, nil
}

func (s *Sender) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

// Send sends the Message to the announce URLs.
func (s *Sender) Send(ctx context.Context, msg message.Message) error {
	buf := bytes.NewBuffer(nil)
	err := msg.MarshalCBOR(buf)
	if err != nil {
		return err
	}

	if len(s.announceURLs) < 2 {
		u := s.announceURLs[0]
		err = s.sendAnnounce(ctx, u, buf)
		if err != nil {
			return fmt.Errorf("failed to send http announce to %s: %w", u, err)
		}
		return nil
	}

	errChan := make(chan error)
	data := buf.Bytes()
	for _, u := range s.announceURLs {
		// Send HTTP announce to indexers concurrently. If context is canceled,
		// then requests will be canceled.
		go func(announceURL string) {
			err := s.sendAnnounce(ctx, announceURL, bytes.NewBuffer(data))
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

func (s *Sender) sendAnnounce(ctx context.Context, announceURL string, buf *bytes.Buffer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, announceURL, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

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
		return fmt.Errorf("%s: %s", http.StatusText(resp.StatusCode), strings.TrimSpace(string(body)))
	}
	return nil
}
