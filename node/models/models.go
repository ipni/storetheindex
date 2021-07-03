package models

import (
	"github.com/filecoin-project/storetheindex/store"
	"github.com/ipfs/go-cid"
)

type FindResp map[cid.Cid][]store.IndexEntry
