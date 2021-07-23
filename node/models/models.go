package models

import (
	"github.com/filecoin-project/go-indexer-core/store"
	"github.com/ipfs/go-cid"
)

type FindResp map[cid.Cid][]store.IndexEntry
