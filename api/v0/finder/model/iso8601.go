package model

import (
	"fmt"
	"time"
)

// iso8601 returns the given time as an ISO8601 formatted string.
func iso8601(t time.Time) string {
	tstr := t.Format("2006-01-02T15:04:05")
	_, zoneOffset := t.Zone()
	if zoneOffset == 0 {
		return fmt.Sprintf("%sZ", tstr)
	}
	if zoneOffset < 0 {
		return fmt.Sprintf("%s-%02d%02d", tstr, -zoneOffset/3600,
			(-zoneOffset%3600)/60)
	}
	return fmt.Sprintf("%s+%02d%02d", tstr, zoneOffset/3600,
		(zoneOffset%3600)/60)
}
