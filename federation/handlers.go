package federation

import (
	"errors"
	"net/http"
	"path"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/schema"
)

func (f *Federation) handleV1FedHead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	switch headNode, err := f.getHeadNode(r.Context()); {
	case err != nil:
		logger.Errorw("Failed to load head snapshot link", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
	default:
		// TODO: Support alternative content types based on request Accept header.
		w.Header().Add("Content-Type", "application/json")
		if err := dagjson.Encode(headNode, w); err != nil {
			logger.Errorw("Failed to encode head", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
		}
	}
}

func (f *Federation) handleV1FedSubtree(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	cidStr := path.Base(r.URL.Path)
	c, err := cid.Decode(cidStr)
	if err != nil {
		http.Error(w, "invalid cid", http.StatusBadRequest)
		return
	}
	logger := logger.With("link", cidStr)

	// Fail fast if codec ask for is not known.
	encoder, err := multicodec.LookupEncoder(c.Prefix().Codec)
	if err != nil {
		logger.Errorw("Failed to find codec for link", "err", err)
		http.Error(w, "codec not supported", http.StatusBadRequest)
		return
	}

	ctx := ipld.LinkContext{Ctx: r.Context()}
	lnk := cidlink.Link{Cid: c}
	node, err := f.linkSystem.Load(ctx, lnk, Prototypes.Snapshot)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) || errors.Is(err, ipld.ErrNotExists{}) {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		logger.Errorw("Failed to load link", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	if err := encoder(node.(schema.TypedNode).Representation(), w); err != nil {
		logger.Errorw("Failed to encode node", "err", err)
	}
}
