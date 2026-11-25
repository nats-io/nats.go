package nats

import (
	"math"
	"strconv"
	"testing"
)

func TestParseInt64RejectsOverflow(t *testing.T) {
	for _, s := range []string{
		strconv.FormatInt(math.MaxInt64, 10) + "0",
		strconv.FormatUint(math.MaxUint64, 10),
		"999999999999999999999999999999999999",
	} {
		if got := parseInt64([]byte(s)); got != -1 {
			t.Errorf("parseInt64(%q) = %d, want -1", s, got)
		}
	}
}

func TestParseInt64AcceptsBoundaryValues(t *testing.T) {
	if got := parseInt64([]byte(strconv.FormatInt(math.MaxInt64, 10))); got != math.MaxInt64 {
		t.Errorf("MaxInt64: got %d", got)
	}
	if got := parseInt64([]byte("0")); got != 0 {
		t.Errorf("zero: got %d", got)
	}
}
