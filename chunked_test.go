package event

import (
	"reflect"
	"testing"
)

func TestPartitionByOriginStreamID(t *testing.T) {
	tests := []struct {
		name string
		in   Records
		out  []Records
	}{
		{
			name: "nil",
		},
		{
			name: "single",
			in: Records{
				{
					StreamID:          All,
					StreamIndex:       0,
					OriginStreamID:    "one",
					OriginStreamIndex: 0,
				},
				{
					StreamID:          All,
					StreamIndex:       1,
					OriginStreamID:    "one",
					OriginStreamIndex: 1,
				},
			},
			out: []Records{
				{
					{
						StreamID:          "one",
						StreamIndex:       0,
						OriginStreamID:    "one",
						OriginStreamIndex: 0,
					},
					{
						StreamID:          "one",
						StreamIndex:       1,
						OriginStreamID:    "one",
						OriginStreamIndex: 1,
					},
				},
			},
		},
		{
			name: "multiple",
			in: Records{
				{
					StreamID:          All,
					StreamIndex:       0,
					OriginStreamID:    "one",
					OriginStreamIndex: 0,
				},
				{
					StreamID:          All,
					StreamIndex:       1,
					OriginStreamID:    "one",
					OriginStreamIndex: 1,
				},
				{
					StreamID:          All,
					StreamIndex:       2,
					OriginStreamID:    "two",
					OriginStreamIndex: 0,
				},
				{
					StreamID:          All,
					StreamIndex:       3,
					OriginStreamID:    "three",
					OriginStreamIndex: 0,
				},
				{
					StreamID:          All,
					StreamIndex:       4,
					OriginStreamID:    "three",
					OriginStreamIndex: 1,
				},
			},
			out: []Records{
				{
					{
						StreamID:          "one",
						StreamIndex:       0,
						OriginStreamID:    "one",
						OriginStreamIndex: 0,
					},
					{
						StreamID:          "one",
						StreamIndex:       1,
						OriginStreamID:    "one",
						OriginStreamIndex: 1,
					},
				},
				{
					{
						StreamID:          "two",
						StreamIndex:       0,
						OriginStreamID:    "two",
						OriginStreamIndex: 0,
					},
				},
				{
					{
						StreamID:          "three",
						StreamIndex:       0,
						OriginStreamID:    "three",
						OriginStreamIndex: 0,
					},
					{
						StreamID:          "three",
						StreamIndex:       1,
						OriginStreamID:    "three",
						OriginStreamIndex: 1,
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := partitionByOriginStreamID(test.in)
			if !reflect.DeepEqual(test.out, got) {
				t.Errorf("want:\n%#v\ngot:\n%#v", test.out, got)
			}
		})
	}
}
