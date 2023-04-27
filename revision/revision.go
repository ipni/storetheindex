package revision

import (
	"encoding/json"
	"runtime/debug"
)

var (
	// Revision is the rcs revision of this package.
	Revision string
	// RevisionJSON is the rcs revision of this package, in JSON format.
	RevisionJSON []byte
)

func init() {
	// If running from a module, try to get the build info.
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	// Get the rcs revision.
	for i := range bi.Settings {
		if bi.Settings[i].Key == "vcs.revision" {
			Revision = bi.Settings[i].Value
			RevisionJSON, _ = json.Marshal(Revision)
			break
		}
	}
}
