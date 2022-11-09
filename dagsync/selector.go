package dagsync

import (
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// ExploreRecursiveWithStop builds a selector that recursively syncs a DAG
// until the link stopLnk is seen. It prevents from having to sync DAGs from
// scratch with every update.
func ExploreRecursiveWithStop(limit selector.RecursionLimit, sequence selectorbuilder.SelectorSpec, stopLnk ipld.Link) ipld.Node {
	return ExploreRecursiveWithStopNode(limit, sequence.Node(), stopLnk)
}

// ExploreRecursiveWithStopNode builds a selector that recursively syncs a DAG
// until the link stopLnk is seen. It prevents from having to sync DAGs from
// scratch with every update.
func ExploreRecursiveWithStopNode(limit selector.RecursionLimit, sequence ipld.Node, stopLnk ipld.Link) ipld.Node {
	if sequence == nil {
		log.Debug("No selector sequence specified; using default explore all with recursive edge.")
		np := basicnode.Prototype__Any{}
		ssb := selectorbuilder.NewSelectorSpecBuilder(np)
		sequence = ssb.ExploreAll(ssb.ExploreRecursiveEdge()).Node()
	}
	np := basicnode.Prototype__Map{}
	return fluent.MustBuildMap(np, 1, func(na fluent.MapAssembler) {
		// RecursionLimit
		na.AssembleEntry(selector.SelectorKey_ExploreRecursive).CreateMap(3, func(na fluent.MapAssembler) {
			na.AssembleEntry(selector.SelectorKey_Limit).CreateMap(1, func(na fluent.MapAssembler) {
				switch limit.Mode() {
				case selector.RecursionLimit_Depth:
					na.AssembleEntry(selector.SelectorKey_LimitDepth).AssignInt(limit.Depth())
				case selector.RecursionLimit_None:
					na.AssembleEntry(selector.SelectorKey_LimitNone).CreateMap(0, func(na fluent.MapAssembler) {})
				default:
					panic("Unsupported recursion limit type")
				}
			})
			// Sequence
			na.AssembleEntry(selector.SelectorKey_Sequence).AssignNode(sequence)

			// Stop condition
			if stopLnk != nil {
				cond := fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
					na.AssembleEntry(string(selector.ConditionMode_Link)).AssignLink(stopLnk)
				})
				na.AssembleEntry(selector.SelectorKey_StopAt).AssignNode(cond)
			}
		})
	})
}

// getStopNode will try to return the stop node from a recursive selector.
func getStopNode(selNode datamodel.Node) (datamodel.Link, bool) {
	if selNode == nil {
		return nil, false
	}
	selNode, err := selNode.LookupByString(selector.SelectorKey_ExploreRecursive)
	if err != nil {
		return nil, false
	}
	selNode, err = selNode.LookupByString(selector.SelectorKey_StopAt)
	if err != nil {
		return nil, false
	}
	selNode, err = selNode.LookupByString(string(selector.ConditionMode_Link))
	if err != nil {
		return nil, false
	}
	stopNodeLink, err := selNode.AsLink()
	return stopNodeLink, err == nil
}

// getRecursionLimit gets the top-most recursion limit from the given selector node.
// If no such selector is found, the returned bool will be false.
func getRecursionLimit(selNode datamodel.Node) (selector.RecursionLimit, bool) {
	if selNode == nil {
		return selector.RecursionLimit{}, false
	}
	selNode, err := selNode.LookupByString(selector.SelectorKey_ExploreRecursive)
	if err != nil {
		return selector.RecursionLimit{}, false
	}
	selNode, err = selNode.LookupByString(selector.SelectorKey_Limit)
	if err != nil {
		return selector.RecursionLimit{}, false
	}
	dln, err := selNode.LookupByString(selector.SelectorKey_LimitDepth)
	if err != nil {
		if _, ok := err.(datamodel.ErrNotExists); ok {
			_, err = selNode.LookupByString(selector.SelectorKey_LimitNone)
			if err == nil {
				return selector.RecursionLimitNone(), true
			}
		}
		return selector.RecursionLimit{}, false
	}
	dl, err := dln.AsInt()
	if err != nil {
		return selector.RecursionLimit{}, false
	}
	return selector.RecursionLimitDepth(dl), true
}

// withRecursionLimit changes the top-most recursion limit for the given selector.
// If no such recursion limit is present in the given selector, returns nil with false bool.
func withRecursionLimit(selNode datamodel.Node, rl selector.RecursionLimit) (datamodel.Node, bool) {
	if selNode == nil {
		return nil, false
	}

	// Expect exactly one root entry since the selector returned will only have one.
	if selNode.Length() != 1 {
		return nil, false
	}

	// Expect the entry to be explore recursive.
	ern, err := selNode.LookupByString(selector.SelectorKey_ExploreRecursive)
	if err != nil {
		return nil, false
	}

	// Store errors that may occur during map iteration to return later.
	var iErr error
	selWithLimit := fluent.MustBuildMap(basicnode.Prototype.Any, 1, func(na fluent.MapAssembler) {
		// Set the recursion limit.
		na.AssembleEntry(selector.SelectorKey_ExploreRecursive).CreateMap(3, func(na fluent.MapAssembler) {
			na.AssembleEntry(selector.SelectorKey_Limit).CreateMap(1, func(na fluent.MapAssembler) {
				switch rl.Mode() {
				case selector.RecursionLimit_Depth:
					na.AssembleEntry(selector.SelectorKey_LimitDepth).AssignInt(rl.Depth())
				case selector.RecursionLimit_None:
					na.AssembleEntry(selector.SelectorKey_LimitNone).CreateMap(0, func(na fluent.MapAssembler) {})
				default:
					panic("Unsupported recursion limit type")
				}
			})

			// Add any remaining entries from the original selector, except recursion limit.
			mi := ern.MapIterator()
			var k, v ipld.Node
			var ks string
			for !mi.Done() {
				k, v, iErr = mi.Next()
				if iErr != nil {
					return
				}

				// If key is a string, then check that it is not a limit selector key.
				if k.Kind() == ipld.Kind_String {
					ks, iErr = k.AsString()
					if iErr != nil {
						return
					}
					if ks == selector.SelectorKey_Limit {
						continue
					}
				}
				na.AssembleKey().AssignNode(k)
				na.AssembleValue().AssignNode(v)
			}
		})
	})
	if iErr != nil {
		return nil, false
	}
	return selWithLimit, true
}

// DagsyncSelector is a convenient function that returns the selector
// used by dagsync subscribers
//
// DagsyncSelector is a "recurse all" selector that provides conditions
// to stop the traversal at a specific link (stopAt).
func DagsyncSelector(limit selector.RecursionLimit, stopLnk ipld.Link) ipld.Node {
	np := basicnode.Prototype__Any{}
	ssb := selectorbuilder.NewSelectorSpecBuilder(np)
	return ExploreRecursiveWithStop(
		limit,
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
		stopLnk)
}
