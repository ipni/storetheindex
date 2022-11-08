package dagsync

import (
	"testing"

	"github.com/filecoin-project/storetheindex/dagsync/test"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"
)

func TestGetStopNode(t *testing.T) {
	c, err := cid.V0Builder{}.Sum([]byte("hi"))
	require.NoError(t, err)
	stopNode := cidlink.Link{Cid: c}
	sel := ExploreRecursiveWithStopNode(selector.RecursionLimitNone(), nil, stopNode)
	actualStopNode, ok := getStopNode(sel)
	require.True(t, ok)
	require.Equal(t, stopNode, actualStopNode)
}

func TestGetStopNodeWhenNil(t *testing.T) {
	sel := ExploreRecursiveWithStopNode(selector.RecursionLimitNone(), nil, nil)
	_, ok := getStopNode(sel)
	require.False(t, ok, "We shouldn't get a stop node out if none was set")
}

func TestGetRecursionLimit(t *testing.T) {
	testCid, err := test.RandomCids(1)
	require.NoError(t, err)
	testStopLink := cidlink.Link{Cid: testCid[0]}
	ssb := selectorbuilder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	tests := []struct {
		name          string
		givenSelector ipld.Node
		wantFound     bool
		wantLimit     selector.RecursionLimit
	}{
		{
			name:          "no limit is not found",
			givenSelector: ssb.ExploreAll(ssb.Matcher()).Node(),
		},
		{
			name:          "unlimited is found",
			givenSelector: ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreRecursiveEdge()).Node(),
			wantFound:     true,
			wantLimit:     selector.RecursionLimitNone(),
		},
		{
			name:          "limited to zero is found",
			givenSelector: ssb.ExploreRecursive(selector.RecursionLimitDepth(0), ssb.ExploreRecursiveEdge()).Node(),
			wantFound:     true,
			wantLimit:     selector.RecursionLimitDepth(0),
		},
		{
			name:          "limited to non-zero is found",
			givenSelector: ssb.ExploreRecursive(selector.RecursionLimitDepth(42), ssb.ExploreRecursiveEdge()).Node(),
			wantFound:     true,
			wantLimit:     selector.RecursionLimitDepth(42),
		},
		{
			name:          "unlimited without stop node is found",
			givenSelector: ExploreRecursiveWithStop(selector.RecursionLimitNone(), ssb.Matcher(), nil),
			wantFound:     true,
			wantLimit:     selector.RecursionLimitNone(),
		},
		{
			name:          "unlimited with stop node is found",
			givenSelector: ExploreRecursiveWithStop(selector.RecursionLimitNone(), ssb.Matcher(), testStopLink),
			wantFound:     true,
			wantLimit:     selector.RecursionLimitNone(),
		},
		{
			name:          "limited without stop node is found",
			givenSelector: ExploreRecursiveWithStop(selector.RecursionLimitDepth(55), ssb.Matcher(), nil),
			wantFound:     true,
			wantLimit:     selector.RecursionLimitDepth(55),
		},
		{
			name:          "unlimited without stop node is found",
			givenSelector: ExploreRecursiveWithStop(selector.RecursionLimitDepth(68), ssb.Matcher(), testStopLink),
			wantFound:     true,
			wantLimit:     selector.RecursionLimitDepth(68),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			gotLimit, gotFound := getRecursionLimit(tt.givenSelector)
			require.Equal(t, tt.wantFound, gotFound)
			require.Equal(t, tt.wantLimit, gotLimit)
		})
	}
}

func TestWithRecursionLimit(t *testing.T) {
	testCid, err := test.RandomCids(1)
	require.NoError(t, err)
	testStopLink := cidlink.Link{Cid: testCid[0]}
	ssb := selectorbuilder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	tests := []struct {
		name          string
		givenSelector ipld.Node
		givenLimit    selector.RecursionLimit
		wantApplied   bool
		wantSelector  ipld.Node
	}{
		{
			name:          "limit is not added to selector with no explore recursive",
			givenSelector: ssb.ExploreAll(ssb.Matcher()).Node(),
		},
		{
			name:          "same limit is applied",
			givenSelector: ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreRecursiveEdge()).Node(),
			givenLimit:    selector.RecursionLimitNone(),
			wantApplied:   true,
			wantSelector:  ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreRecursiveEdge()).Node(),
		},
		{
			name:          "different limit is applied",
			givenSelector: ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreRecursiveEdge()).Node(),
			givenLimit:    selector.RecursionLimitDepth(45),
			wantApplied:   true,
			wantSelector:  ssb.ExploreRecursive(selector.RecursionLimitDepth(45), ssb.ExploreRecursiveEdge()).Node(),
		},
		{
			name: "different limit with complex sequence is applied",
			givenSelector: ssb.ExploreRecursive(
				selector.RecursionLimitDepth(1413),
				ssb.ExploreFields(func(efs selectorbuilder.ExploreFieldsSpecBuilder) {
					efs.Insert("fish", ssb.ExploreRecursiveEdge())
					efs.Insert("lobster", ssb.ExploreRange(2, 3, ssb.ExploreRecursiveEdge()))
				})).Node(),
			givenLimit:  selector.RecursionLimitNone(),
			wantApplied: true,
			wantSelector: ssb.ExploreRecursive(
				selector.RecursionLimitNone(),
				ssb.ExploreFields(func(efs selectorbuilder.ExploreFieldsSpecBuilder) {
					efs.Insert("fish", ssb.ExploreRecursiveEdge())
					efs.Insert("lobster", ssb.ExploreRange(2, 3, ssb.ExploreRecursiveEdge()))
				})).Node(),
		},
		{
			name:          "unlimited without stop node is applied",
			givenSelector: ExploreRecursiveWithStop(selector.RecursionLimitNone(), ssb.Matcher(), nil),
			givenLimit:    selector.RecursionLimitDepth(1413),
			wantApplied:   true,
			wantSelector:  ExploreRecursiveWithStop(selector.RecursionLimitDepth(1413), ssb.Matcher(), nil),
		},
		{
			name:          "unlimited with stop node is applied",
			givenSelector: ExploreRecursiveWithStop(selector.RecursionLimitNone(), ssb.Matcher(), testStopLink),
			givenLimit:    selector.RecursionLimitDepth(1413),
			wantApplied:   true,
			wantSelector:  ExploreRecursiveWithStop(selector.RecursionLimitDepth(1413), ssb.Matcher(), testStopLink),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			gotSelector, gotApplied := withRecursionLimit(tt.givenSelector, tt.givenLimit)
			require.Equal(t, tt.wantApplied, gotApplied)
			require.True(t, datamodel.DeepEqual(tt.wantSelector, gotSelector))
		})
	}
}
