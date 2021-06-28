package models

import (
	"github.com/adlrocha/indexer-node/primary"
	"github.com/ipfs/go-cid"
)

type FindResp map[cid.Cid][]primary.IndexEntry
