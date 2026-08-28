package server

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net"
	"net/http"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/ipni/storetheindex/assigner/core"
	"github.com/ipni/storetheindex/assigner/metrics"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("assigner/server")

// maxBodySize is the limit on the request body size that the server will read.
// No request body should be this large, so any request exceeding this size is
// clearly in error.
const maxBodySize = 1024 * 1024

const (
	encodingJSON = "json"
	encodingCBOR = "cbor"
)

type Server struct {
	assigner        *core.Assigner
	server          *http.Server
	listener        net.Listener
	healthMsg       string
	shutdownTimeout time.Duration
}

func New(listen string, assigner *core.Assigner, options ...Option) (*Server, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	l, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	server := &http.Server{
		Handler:      mux,
		WriteTimeout: opts.writeTimeout,
		ReadTimeout:  opts.readTimeout,
	}
	s := &Server{
		assigner:        assigner,
		server:          server,
		listener:        l,
		shutdownTimeout: opts.shutdownTimeout,
	}

	s.healthMsg = "assigner ready"
	if opts.version != "" {
		s.healthMsg += " " + opts.version
	}

	// Direct announce.
	mux.HandleFunc("/announce", s.announce)
	// Health check.
	mux.HandleFunc("/health", s.health)

	// Depricated
	mux.HandleFunc("/ingest/announce", s.announce)

	return s, nil
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.listener.Addr().String())
}

func (s *Server) Start() error {
	log.Infow("http server listening", "listen_addr", s.listener.Addr())
	return s.server.Serve(s.listener)
}

func (s *Server) Close() error {
	log.Info("http server shutdown")

	ctx := context.Background()
	if s.shutdownTimeout > 0 {
		tctx, cancel := context.WithTimeout(ctx, s.shutdownTimeout)
		defer cancel()
		ctx = tctx
	}

	return s.server.Shutdown(ctx)
}

func announceEncoding(contentType string) string {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err == nil && mediaType == "application/json" {
		return encodingJSON
	}
	return encodingCBOR
}

// PUT /announce
func (s *Server) announce(w http.ResponseWriter, r *http.Request) {
	if !methodOK(w, r, http.MethodPut) {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	defer r.Body.Close()

	encoding := announceEncoding(r.Header.Get("Content-Type"))
	an := message.Message{}
	bodyReader := http.MaxBytesReader(w, r.Body, maxBodySize)
	var err error
	if encoding == encodingJSON {
		err = json.NewDecoder(bodyReader).Decode(&an)
	} else {
		err = an.UnmarshalCBOR(bodyReader)
	}
	if err != nil {
		metrics.RecordReceived(encoding, metrics.ResultDecodeError)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(an.Addrs) == 0 {
		metrics.RecordReceived(encoding, metrics.ResultInvalid)
		http.Error(w, "must specify location to fetch on direct announcments", http.StatusBadRequest)
		return
	}
	addrs, err := an.GetAddrs()
	if err != nil {
		metrics.RecordReceived(encoding, metrics.ResultInvalid)
		err = fmt.Errorf("could not decode addrs from announce message: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ais, err := peer.AddrInfosFromP2pAddrs(addrs...)
	if err != nil {
		metrics.RecordReceived(encoding, metrics.ResultInvalid)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(ais) > 1 {
		metrics.RecordReceived(encoding, metrics.ResultInvalid)
		http.Error(w, "peer id must be the same for all addresses", http.StatusBadRequest)
		return
	}
	addrInfo := ais[0]

	if !s.assigner.Allowed(addrInfo.ID) {
		metrics.RecordReceived(encoding, metrics.ResultForbidden)
		http.Error(w, "announce requests not allowed from peer", http.StatusForbidden)
		return
	}

	// Use background context because this will be an async process. We don't
	// want to attach the context to the request context that started this.
	err = s.assigner.Announce(context.Background(), an.Cid, addrInfo)
	if err != nil {
		metrics.RecordReceived(encoding, metrics.ResultError)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	metrics.RecordReceived(encoding, metrics.ResultOK)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if !methodOK(w, r, http.MethodGet) {
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	http.Error(w, s.healthMsg, http.StatusOK)
}

func methodOK(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		w.Header().Set("Allow", method)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return false
	}
	return true
}
