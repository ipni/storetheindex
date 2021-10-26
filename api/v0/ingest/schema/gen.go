//go:build ignore
// +build ignore

package main

import (
	_ "embed"
	"fmt"
	"os"

	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/schema"
	gengo "github.com/ipld/go-ipld-prime/schema/gen/go"
)

//go:embed schema.ipldsch
var schemaSrc []byte

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Must specify destination directory")
		os.Exit(1)
	}

	ts, err := ipld.LoadSchemaBytes(schemaSrc)
	if err != nil {
		panic(err)
	}

	adjCfg := &gengo.AdjunctCfg{
		CfgUnionMemlayout: map[schema.TypeName]string{
			"Any": "interface",
		},
	}
	gengo.Generate(os.Args[1], "schema", *ts, adjCfg)
}
