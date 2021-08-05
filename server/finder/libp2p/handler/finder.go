package libp2phandler

import (
	"context"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	pb "github.com/filecoin-project/storetheindex/api/v0/finder/pb"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/libp2p/go-libp2p-core/peer"
)

// FinderHandlerFunc
type FinderHandlerFunc func(context.Context, peer.ID, *pb.Message) (*pb.Message, error)

func HandleFinderGet(engine *indexer.Engine, registry *providers.Registry) FinderHandlerFunc {
	return func(ctx context.Context, p peer.ID, msg *pb.Message) (*pb.Message, error) {

		req, err := models.UnmarshalReq(msg.GetData())
		if err != nil {
			return nil, err
		}

		r, err := models.PopulateResponse(engine, registry, req.Cids)
		if err != nil {
			return nil, err
		}
		rb, err := models.MarshalResp(r)
		if err != nil {
			return nil, err
		}

		resp := &pb.Message{
			Type: pb.Message_RESPONSE,
			Data: rb,
		}

		return resp, nil
	}
}
