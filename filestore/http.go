package filestore

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	httpType = "http"

	queryList         = "list"
	queryPath         = "path"
	queryRecursive    = "recursive"
	headerFilePath    = "X-Filestore-Path"
	headerStreamError = "X-Filestore-Stream-Error"
	contentTypeNDJSON = "application/x-ndjson"
)

// HTTP is a read-only file store client that accesses files over HTTP.
type HTTP struct {
	baseURL *url.URL
	client  *http.Client
}

// NewHTTP creates an HTTP file store client. prefix is the base URL; file paths
// are joined to it with url.JoinPath (e.g. prefix "http://host/cars/" and path
// "foo.car" becomes "http://host/cars/foo.car").
func NewHTTP(baseURL string, options ...HTTPOption) (*HTTP, error) {
	if baseURL == "" {
		return nil, errors.New("http filestore requires URL prefix")
	}

	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid http filestore prefix: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, errors.New("http filestore prefix must include scheme and host")
	}

	opts, err := getHTTPOpts(options)
	if err != nil {
		return nil, err
	}

	client := opts.client
	if client == nil {
		client = http.DefaultClient
	}

	return &HTTP{
		baseURL: u,
		client:  client,
	}, nil
}

func (h *HTTP) Delete(ctx context.Context, relPath string) error {
	req, err := h.newRequest(ctx, http.MethodDelete, relPath, nil)
	if err != nil {
		return err
	}

	rsp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer rsp.Body.Close()

	if rsp.StatusCode != http.StatusOK && rsp.StatusCode != http.StatusNoContent {
		return httpStatusError(rsp)
	}
	return nil
}

func (h *HTTP) Get(ctx context.Context, relPath string) (*File, io.ReadCloser, error) {
	req, err := h.newRequest(ctx, http.MethodGet, relPath, nil)
	if err != nil {
		return nil, nil, err
	}

	rsp, err := h.client.Do(req)
	if err != nil {
		return nil, nil, err
	}

	if rsp.StatusCode == http.StatusNotFound {
		defer rsp.Body.Close()
		return nil, nil, fs.ErrNotExist
	}
	if rsp.StatusCode != http.StatusOK {
		defer rsp.Body.Close()
		return nil, nil, httpStatusError(rsp)
	}

	file, err := fileFromResponse(relPath, rsp)
	if err != nil {
		defer rsp.Body.Close()
		return nil, nil, err
	}
	file.URL = req.URL.String()

	return file, &rspReadCloser{
		rsp: rsp,
	}, nil
}

type rspReadCloser struct {
	rsp *http.Response
}

func (r rspReadCloser) Read(p []byte) (int, error) {
	n, err := r.rsp.Body.Read(p)
	if errors.Is(err, io.EOF) {
		if n > 0 {
			// Allow consuming remaining data first
			return n, nil
		}

		// Check for error in trailer once we've got to EOF
		if trailerErr := r.rsp.Trailer.Get(headerStreamError); trailerErr != "" {
			return n, errors.New(trailerErr)
		}
	}
	return n, err
}

func (r rspReadCloser) Close() error {
	err := r.rsp.Body.Close()
	if trailerErr := r.rsp.Trailer.Get(headerStreamError); trailerErr != "" {
		return errors.New(trailerErr)
	}
	return err
}

func (h *HTTP) Head(ctx context.Context, relPath string) (*File, error) {
	req, err := h.newRequest(ctx, http.MethodHead, relPath, nil)
	if err != nil {
		return nil, err
	}

	rsp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()

	if rsp.StatusCode == http.StatusNotFound {
		return nil, fs.ErrNotExist
	}
	if rsp.StatusCode != http.StatusOK {
		return nil, httpStatusError(rsp)
	}

	file, err := fileFromResponse(relPath, rsp)
	if err != nil {
		return nil, err
	}
	file.URL = req.URL.String()
	return file, nil
}

func (h *HTTP) List(ctx context.Context, relPath string, recursive bool) (<-chan *File, <-chan error) {
	fc := make(chan *File)
	ec := make(chan error, 1)

	go func() {
		defer close(fc)
		defer close(ec)

		q := url.Values{}
		q.Set(queryList, "true")
		q.Set(queryPath, relPath)
		if recursive {
			q.Set(queryRecursive, "true")
		}

		req, err := h.newRequest(ctx, http.MethodGet, "", q)
		if err != nil {
			ec <- err
			return
		}

		rsp, err := h.client.Do(req)
		if err != nil {
			ec <- err
			return
		}
		defer rsp.Body.Close()

		if rsp.StatusCode != http.StatusOK {
			ec <- httpStatusError(rsp)
			return
		}

		scanner := bufio.NewScanner(rsp.Body)
		for scanner.Scan() {
			rec, err := decodeListRecord(scanner.Bytes())
			if err != nil {
				ec <- err
				return
			}
			if rec.Error != "" {
				ec <- errors.New(rec.Error)
				return
			}
			if rec.File == nil {
				continue
			}
			select {
			case fc <- rec.File:
			case <-ctx.Done():
				ec <- ctx.Err()
				return
			}
		}
		if err := scanner.Err(); err != nil {
			ec <- err
		}
	}()

	return fc, ec
}

func (h *HTTP) Put(ctx context.Context, relPath string, reader io.Reader) (*File, error) {
	u, err := h.fileURL(relPath)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), reader)
	if err != nil {
		return nil, err
	}
	if reader != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	rsp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()

	if rsp.StatusCode == http.StatusNotFound {
		return nil, fs.ErrNotExist
	}
	if rsp.StatusCode != http.StatusOK && rsp.StatusCode != http.StatusNoContent {
		return nil, httpStatusError(rsp)
	}

	return h.Head(ctx, relPath)
}

func (h *HTTP) Type() string {
	return httpType
}

func (h *HTTP) newRequest(ctx context.Context, method, relPath string, query url.Values) (*http.Request, error) {
	u, err := h.fileURL(relPath)
	if err != nil {
		return nil, err
	}
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	return http.NewRequestWithContext(ctx, method, u.String(), nil)
}

func (h *HTTP) fileURL(relPath string) (*url.URL, error) {
	relPath, err := cleanRelPath(relPath)
	if err != nil {
		return nil, err
	}

	joined, err := url.JoinPath(h.baseURL.String(), relPath)
	if err != nil {
		return nil, err
	}
	return url.Parse(joined)
}

// HTTPHandler serves a read-only HTTP API over an underlying file store.
type HTTPHandler struct {
	fs Interface
}

// NewHTTPHandler creates a handler that exposes fs over HTTP.
// Mount under a non-root path with http.StripPrefix.
func NewHTTPHandler(fs Interface) (*HTTPHandler, error) {
	if fs == nil {
		return nil, errors.New("http handler requires file store")
	}

	return &HTTPHandler{
		fs: fs,
	}, nil
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Get(queryList) != "" {
			h.serveList(w, r)
			return
		}
		h.serveGet(w, r)
	case http.MethodHead:
		h.serveHead(w, r)
	case http.MethodPut, http.MethodDelete:
		http.Error(w, "read-only filestore", http.StatusMethodNotAllowed)
	default:
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
	}
}

func (h *HTTPHandler) serveGet(w http.ResponseWriter, r *http.Request) {
	relPath, err := h.requestPath(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	file, rc, err := h.fs.Get(r.Context(), relPath)
	if err != nil {
		h.writeError(w, err)
		return
	}
	defer rc.Close()

	setFileHeaders(w, file)
	w.Header().Set("Trailer", headerStreamError)
	w.WriteHeader(http.StatusOK)

	if _, err := io.Copy(w, rc); err != nil {
		// Send the error through trailer header since we've already sent the headers.
		w.Header().Set(headerStreamError, err.Error())
		return
	}
}

func (h *HTTPHandler) serveHead(w http.ResponseWriter, r *http.Request) {
	relPath, err := h.requestPath(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	file, err := h.fs.Head(r.Context(), relPath)
	if err != nil {
		h.writeError(w, err)
		return
	}

	setFileHeaders(w, file)
	w.WriteHeader(http.StatusOK)
}

func (h *HTTPHandler) serveList(w http.ResponseWriter, r *http.Request) {
	relPath := r.URL.Query().Get(queryPath)
	recursive, _ := strconv.ParseBool(r.URL.Query().Get(queryRecursive))

	fc, ec := h.fs.List(r.Context(), relPath, recursive)

	w.Header().Set("Content-Type", contentTypeNDJSON)
	w.WriteHeader(http.StatusOK)

	for f := range fc {
		if err := writeListRecord(w, listRecord{File: f}); err != nil {
			return
		}
	}

	if err := <-ec; err != nil {
		_ = writeListRecord(w, listRecord{Error: err.Error()})
	}
}

func (h *HTTPHandler) requestPath(r *http.Request) (string, error) {
	relPath := strings.TrimPrefix(r.URL.Path, "/")
	return cleanRelPath(relPath)
}

func (h *HTTPHandler) writeError(w http.ResponseWriter, err error) {
	if errors.Is(err, fs.ErrNotExist) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// listRecord is one NDJSON line in a list response. Exactly one of File or Error
// should be set.
type listRecord struct {
	File  *File  `json:"file,omitempty"`
	Error string `json:"error,omitempty"`
}

func writeListRecord(w io.Writer, rec listRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = w.Write(data)
	return err
}

func decodeListRecord(data []byte) (listRecord, error) {
	var rec listRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return listRecord{}, err
	}
	if rec.Error != "" && rec.File != nil {
		return listRecord{}, errors.New("invalid list record: file and error set")
	}
	return rec, nil
}

type httpConfig struct {
	client *http.Client
}

type HTTPOption func(*httpConfig) error

func getHTTPOpts(opts []HTTPOption) (httpConfig, error) {
	var cfg httpConfig
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return httpConfig{}, fmt.Errorf("option %d error: %w", i, err)
		}
	}
	return cfg, nil
}

// WithHTTPClient sets the HTTP client used by HTTP file store clients.
func WithHTTPClient(c *http.Client) HTTPOption {
	return func(cfg *httpConfig) error {
		cfg.client = c
		return nil
	}
}

func cleanRelPath(relPath string) (string, error) {
	relPath = path.Clean(relPath)
	if relPath == "." {
		return "", nil
	}
	if relPath == ".." || strings.HasPrefix(relPath, "../") {
		return "", errors.New("invalid path")
	}
	return relPath, nil
}

func setFileHeaders(w http.ResponseWriter, file *File) {
	if file.Path != "" {
		w.Header().Set(headerFilePath, file.Path)
	}
	if file.Size >= 0 {
		// Use custom header as Content-Length would disable trailer headers
		w.Header().Set("Content-Length", strconv.FormatInt(file.Size, 10))
	}
	if !file.Modified.IsZero() {
		w.Header().Set("Last-Modified", file.Modified.UTC().Format(http.TimeFormat))
	}
	w.Header().Set("Content-Type", "application/octet-stream")
}

func fileFromResponse(defaultPath string, rsp *http.Response) (*File, error) {
	file := &File{
		Path: defaultPath,
		Size: rsp.ContentLength,
	}

	if p := rsp.Header.Get(headerFilePath); p != "" {
		file.Path = p
	}

	if lm := rsp.Header.Get("Last-Modified"); lm != "" {
		if t, err := time.Parse(http.TimeFormat, lm); err == nil {
			file.Modified = t
		}
	}

	return file, nil
}

func httpStatusError(rsp *http.Response) error {
	msg, _ := io.ReadAll(io.LimitReader(rsp.Body, 4096))
	if len(msg) == 0 {
		return fmt.Errorf("http %s", rsp.Status)
	}
	return fmt.Errorf("http %s: %s", rsp.Status, strings.TrimSpace(string(msg)))
}
