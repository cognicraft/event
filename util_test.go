package event

import (
	"fmt"
	"testing"
	"time"
)

func TestTime(t *testing.T) {
	tests := []struct {
		in  string
		out time.Time
		fmt string
	}{
		{
			in:  "2021-03-19T09:34:15Z",
			out: time.Date(2021, 03, 19, 9, 34, 15, 0, time.UTC),
			fmt: "2021-03-19T09:34:15.000000000Z",
		},
		{
			in:  "2021-03-19T09:34:15.0Z",
			out: time.Date(2021, 03, 19, 9, 34, 15, 0, time.UTC),
			fmt: "2021-03-19T09:34:15.000000000Z",
		},
		{
			in:  "2021-03-19T09:34:15.001Z",
			out: time.Date(2021, 03, 19, 9, 34, 15, 1000000, time.UTC),
			fmt: "2021-03-19T09:34:15.001000000Z",
		},
		{
			in:  "2021-03-19T09:34:15.000001Z",
			out: time.Date(2021, 03, 19, 9, 34, 15, 1000, time.UTC),
			fmt: "2021-03-19T09:34:15.000001000Z",
		},
		{
			in:  "2021-03-19T09:34:15.000001000Z",
			out: time.Date(2021, 03, 19, 9, 34, 15, 1000, time.UTC),
			fmt: "2021-03-19T09:34:15.000001000Z",
		},
		{
			in:  "2021-03-19T09:34:15.000000001Z",
			out: time.Date(2021, 03, 19, 9, 34, 15, 1, time.UTC),
			fmt: "2021-03-19T09:34:15.000000001Z",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			got := parseTime(test.in)
			if !test.out.Equal(got) {
				t.Errorf("want: %s, got: %s", formatTime(test.out), formatTime(got))
			}
			if gotFmt := formatTime(got); test.fmt != gotFmt {
				t.Errorf("want: %s, got: %s", test.fmt, gotFmt)
			}
		})
	}
}
