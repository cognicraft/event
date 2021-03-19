package event

import "time"

const (
	rfc3339NanoSortable = "2006-01-02T15:04:05.000000000Z07:00"
)

func formatTime(t time.Time) string {
	return t.UTC().Format(rfc3339NanoSortable)
}

func parseTime(raw string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, raw)
	return t
}
