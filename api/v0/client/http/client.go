package httpclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/filecoin-project/storetheindex/api/v0"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("httpclient")

// newClient creates a base URL and a new http.Client
func newClient(baseURL, resource string, defaultPort int, options ...ClientOption) (*url.URL, *http.Client, error) {
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}

	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, nil, err
	}
	if u.Scheme == "" {
		return nil, nil, errors.New("url missing scheme")
	}
	u.Path = resource
	if u.Port() == "" {
		u.Host += fmt.Sprintf(":%d", defaultPort)
	}

	var cfg clientConfig
	if err := cfg.apply(options...); err != nil {
		return nil, nil, err
	}

	cl := &http.Client{
		Timeout: cfg.timeout,
	}
	return u, cl, nil
}

func readError(statusCode int, body []byte) error {
	if len(body) == 0 {
		return errors.New(http.StatusText(statusCode))
	}

	var e v0.Error
	_ = json.Unmarshal(body, &e)
	return fmt.Errorf("%s: %s", http.StatusText(statusCode), e.Message)
}
