// Package revision provides the rcs revision, embedded by the compiler, as a
// global variable.
package revision

import (
	"encoding/json"
	"runtime/debug"
)

var (
	// Revision is the rcs revision embedded by the compiler.
	Revision string
	// RevisionJSON is the rcs revision, in JSON format.
	RevisionJSON []byte
)

func init() {
	// Get the rcs revision from the build info.
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	for i := range bi.Settings {
		if bi.Settings[i].Key == "vcs.revision" {
			Revision = bi.Settings[i].Value
			RevisionJSON, _ = json.Marshal(Revision)
			break
		}
	}
}
