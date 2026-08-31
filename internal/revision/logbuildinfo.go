package revision

import "runtime/debug"

type infower interface {
	Infow(msg string, keysAndValues ...any)
}

// Log emits a "starting" line with version and VCS build metadata.
func Log(log infower, version string) {
	log.Infow("starting", fields(version)...)
}

func fields(version string) []any {
	kvs := []any{"version", version}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return kvs
	}
	kvs = append(kvs, "go", bi.GoVersion)
	for i := range bi.Settings {
		switch bi.Settings[i].Key {
		case "vcs.revision", "vcs.time", "vcs.modified":
			kvs = append(kvs, bi.Settings[i].Key, bi.Settings[i].Value)
		}
	}
	return kvs
}
