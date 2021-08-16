package config

import (
	"encoding"
	"time"
)

// Duration wraps time.Duration to provide json serialization and deserialization.
//
// NOTE: the zero value encodes to an empty string.
type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	dur, err := time.ParseDuration(string(text))
	*d = Duration(dur)
	return err
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d Duration) String() string {
	return time.Duration(d).String()
}

var _ encoding.TextUnmarshaler = (*Duration)(nil)
var _ encoding.TextMarshaler = (*Duration)(nil)
