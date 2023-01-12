package main

import (
	"testing"
)

func TestLineAt(t *testing.T) {
	cases := []struct {
		input    string
		expected []string
	}{
		{"", []string{""}},
		{"foo", []string{"foo"}},
		{"\n", []string{"\n", ""}},
		{"foo\nbar", []string{"foo\n", "bar"}},
		{"foo\n\nbar", []string{"foo\n", "\n", "bar"}},
	}
	for _, test := range cases {
		var h handler
		_, _ = h.b.WriteString(test.input)
		h.calculateNewlineOffsets()
		lines := h.lineCount()
		if lines != len(test.expected) {
			t.FailNow()
		}
		for i := 0; i < lines; i++ {
			got := string(h.lineAt(i))
			if got != test.expected[i] {
				t.FailNow()
			}
		}
	}
}
