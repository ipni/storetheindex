// +build ignore

package main

import (
	"fmt"
	"os"

	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/schema"
	gengo "github.com/ipld/go-ipld-prime/schema/gen/go"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Must specify destination directory")
		os.Exit(1)
	}

	ts := schema.TypeSystem{}
	ts.Init()
	adjCfg := &gengo.AdjunctCfg{
		CfgUnionMemlayout: map[schema.TypeName]string{
			"Any": "interface",
		},
	}

	// Prelude.  (This is boilerplate; it will be injected automatically by the schema libraries in the future, but isn't yet.)
	ts.Accumulate(schema.SpawnBool("Bool"))
	ts.Accumulate(schema.SpawnInt("Int"))
	ts.Accumulate(schema.SpawnFloat("Float"))
	ts.Accumulate(schema.SpawnString("String"))
	ts.Accumulate(schema.SpawnBytes("Bytes"))
	ts.Accumulate(schema.SpawnMap("Map",
		"String", "Any", true,
	))
	ts.Accumulate(schema.SpawnList("List",
		"Any", true,
	))
	ts.Accumulate(schema.SpawnLink("Link"))
	ts.Accumulate(schema.SpawnUnion("Any",
		[]schema.TypeName{
			"Bool",
			"Int",
			"Float",
			"String",
			"Bytes",
			"Map",
			"List",
			"Link",
		},

		schema.SpawnUnionRepresentationKinded(map[ipld.Kind]schema.TypeName{
			ipld.Kind_Bool:   "Bool",
			ipld.Kind_Int:    "Int",
			ipld.Kind_Float:  "Float",
			ipld.Kind_String: "String",
			ipld.Kind_Bytes:  "Bytes",
			ipld.Kind_Map:    "Map",
			ipld.Kind_List:   "List",
			ipld.Kind_Link:   "Link",
		}),
	))

	// List string
	ts.Accumulate(schema.SpawnList("List_String", "String", false))

	// Advertisement
	ts.Accumulate(schema.SpawnStruct("Advertisement", []schema.StructField{
		schema.SpawnStructField("IndexID", "Link_Index", false, false),
		// Empty bytes for the first advertisement
		// We are using Bytes instead of link because we don't want
		// selectors to follow advertisements as links with the
		// current implementation. this may change in the future.
		schema.SpawnStructField("PreviousID", "Bytes", false, false),
		schema.SpawnStructField("Provider", "String", false, false),
		schema.SpawnStructField("Signature", "Bytes", false, false),
		schema.SpawnStructField("GraphSupport", "Bool", false, false),
	}, schema.SpawnStructRepresentationMap(map[string]string{})))
	ts.Accumulate(schema.SpawnLinkReference("Link_Advertisement", "Advertisement"))

	// IPLD-aware index
	ts.Accumulate(schema.SpawnStruct("Index", []schema.StructField{
		// NOTE: We link the previous index instead of the previous
		// advertisement to this one. If by any change ingestion clients
		// need to also fetch all the chain of advertisements change this
		// to Link_Advertisement of the previous ad.
		schema.SpawnStructField("Previous", "Link_Index", true, false),
		schema.SpawnStructField("Entries", "List_Entry", false, false),
	}, schema.SpawnStructRepresentationMap(map[string]string{})))
	ts.Accumulate(schema.SpawnLinkReference("Link_Index", "Index"))

	// Entries
	ts.Accumulate(schema.SpawnStruct("Entry", []schema.StructField{
		// Use strings instead of CIDs, we don't want to traverse
		schema.SpawnStructField("RmCids", "List_String", true, true),
		schema.SpawnStructField("Cids", "List_String", true, true),
		schema.SpawnStructField("Metadata", "Bytes", true, true),
	}, schema.SpawnStructRepresentationMap(map[string]string{})))
	ts.Accumulate(schema.SpawnList("List_Entry", "Entry", false))

	// Our types.
	if errs := ts.ValidateGraph(); errs != nil {
		for _, err := range errs {
			fmt.Printf("- %s\n", err)
		}
		panic("not happening")
	}

	gengo.Generate(os.Args[1], "ingestion", ts, adjCfg)
}
