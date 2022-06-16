package ingest

import (
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// Selectors captures a collection of IPLD selectors commonly used by the Ingester.
var Selectors struct {
	// One selects a single node in a DAG.
	One ipld.Node
	// All selects all the nodes in a DAG recursively traversing all the edges with no recursion
	// limit.
	All ipld.Node
	// AdSequence selects an schema.Advertisement sequence with recursive edge exploration of
	// PreviousID field.
	AdSequence ipld.Node
	// EntriesWithLimit selects schema.EntryChunk nodes  recursively traversing the edge at Next
	// filed with the given recursion limit.
	EntriesWithLimit func(limit selector.RecursionLimit) ipld.Node
}

func init() {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	all := ssb.ExploreAll(ssb.ExploreRecursiveEdge())
	Selectors.One = ssb.ExploreRecursive(selector.RecursionLimitDepth(0), all).Node()
	Selectors.All = ssb.ExploreRecursive(selector.RecursionLimitNone(), all).Node()
	// Construct a selector that recursively looks for nodes with field
	// "PreviousID" as per Advertisement schema. Note that the entries within
	// an advertisement are synced separately, triggered by storage hook. This
	// allows checking if a chain of chunks already exists before syncing it.
	Selectors.AdSequence = ssb.ExploreFields(
		func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		}).Node()
	Selectors.EntriesWithLimit = func(limit selector.RecursionLimit) ipld.Node {
		return ssb.ExploreRecursive(limit,
			ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert("Next", ssb.ExploreRecursiveEdge()) // Next field in EntryChunk
			})).Node()
	}
}
