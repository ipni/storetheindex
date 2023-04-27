package main

import (
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/ipni/storetheindex/revision"
)

var version string

//go:embed version.json
var versionJSON []byte

func init() {
	var verMap map[string]string
	json.Unmarshal(versionJSON, &verMap)
	version = fmt.Sprint(verMap["version"], "-", revision.Revision)
}
