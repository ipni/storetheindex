//go:build ignore
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
	ts.Accumulate(schema.SpawnStruct("EntryChunk", []schema.StructField{
		schema.SpawnStructField("Entries", "List_String", false, false),
		schema.SpawnStructField("Next", "Link_EntryChunk", true, false),
	}, schema.SpawnStructRepresentationMap(map[string]string{})))
	ts.Accumulate(schema.SpawnLinkReference("Link_EntryChunk", "EntryChunk"))
	ts.Accumulate(schema.SpawnList("List_String", "String", false))

	// Advertisement
	ts.Accumulate(schema.SpawnStruct("Advertisement", []schema.StructField{
		// Previous advertisement.
		schema.SpawnStructField("PreviousID", "Link_Advertisement", true, false),
		// Provider of the advertisement.
		schema.SpawnStructField("Provider", "String", false, false),
		// Advertisement signature.
		schema.SpawnStructField("Signature", "Bytes", false, false),
		// Entries with a link to the list of CIDs
		schema.SpawnStructField("Entries", "Link", false, false),
		// Metadata for entries.
		schema.SpawnStructField("Metadata", "Bytes", false, false),
		// IsRm or Put?
		schema.SpawnStructField("IsRm", "Bool", false, false),
	}, schema.SpawnStructRepresentationMap(map[string]string{})))
	ts.Accumulate(schema.SpawnLinkReference("Link_Advertisement", "Advertisement"))

	// Our types.
	if errs := ts.ValidateGraph(); errs != nil {
		for _, err := range errs {
			fmt.Printf("- %s\n", err)
		}
		panic("not happening")
	}

	gengo.Generate(os.Args[1], "schema", ts, adjCfg)
}
