package models

import (
	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/ipfs/go-cid"
)

type FindResp map[cid.Cid][]entry.Value
