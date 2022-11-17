// Package version records versioning information about this module.
package version

import (
	"regexp"
)

var GitVersion string = "unknown"

var reVersion = regexp.MustCompile(`^(v\d+\.\d+.\d+)(?:-)?(.+)?$`)

// String formats the version in semver format, see semver.org
func String() string {
	matches := reVersion.FindStringSubmatch(GitVersion)
	if matches == nil || len(matches) < 3 {
		return "v0.0.0+" + GitVersion
	}

	if matches[2] == "" {
		return matches[1]
	}
	return matches[1] + "+" + matches[2]
}
