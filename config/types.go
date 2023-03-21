package config

import (
	"encoding"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ByteSize wraps uint64 to provide json serialization and
// deserialization.
//
// NOTE: the zero value encodes to an empty string.
type ByteSize uint64

const (
	giSuffix = "Gi"
	giSize   = 1 << 30
	miSuffix = "Mi"
	miSize   = 1 << 20
)

func (d *ByteSize) UnmarshalText(text []byte) error {
	str := strings.TrimSpace(string(text))
	// If the value is emoty - defaulting to zero
	if len(str) == 0 {
		*d = ByteSize(0)
		return nil
	}
	// If there is less than two bytes - treating it as a number
	if len(str) <= 2 {
		n, err := strconv.Atoi(str)
		if err != nil {
			return err
		}
		*d = ByteSize(n)
		return nil
	}
	suffix := strings.ToLower(str[len(str)-2:])
	multiplier := 1
	var n int
	var err error
	switch suffix {
	case strings.ToLower(miSuffix):
		n, err = strconv.Atoi(str[:len(str)-2])
		if err != nil {
			return err
		}
		multiplier = miSize
	case strings.ToLower(giSuffix):
		n, err = strconv.Atoi(str[:len(str)-2])
		if err != nil {
			return err
		}
		multiplier = giSize
	default:
		n, err = strconv.Atoi(str)
	}
	*d = ByteSize(n * multiplier)
	return err
}

func (d ByteSize) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d ByteSize) String() string {
	if d%giSize == 0 {
		return fmt.Sprintf("%d%s", d/giSize, giSuffix)
	} else if d%miSize == 0 {
		return fmt.Sprintf("%d%s", d/miSize, miSuffix)
	} else {
		return fmt.Sprintf("%d", d)
	}
}

// Duration wraps time.Duration to provide json serialization and
// deserialization.
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
