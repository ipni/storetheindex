package version

import (
	"testing"
)

func TestString(t *testing.T) {
	testCases := map[string]string{
		"f176923-dirty":           "v0.0.0+f176923-dirty",
		"f176923":                 "v0.0.0+f176923",
		"v0.1.3-1-g518f694":       "v0.1.3+1-g518f694",
		"v0.1.3-1-g518f694-dirty": "v0.1.3+1-g518f694-dirty",
		"v0.1.3":                  "v0.1.3",
		"v10.31.93":               "v10.31.93",
	}

	for v, want := range testCases {
		GitVersion = v
		if String() != want {
			t.Errorf("got %q, want %q", String(), want)
		}

	}

}
