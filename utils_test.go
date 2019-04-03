package bulk

import (
	"fmt"
	"testing"
	"time"
)

func TestNewInfluxTime(t *testing.T) {
	for _, test := range []struct {
		query time.Duration
		want  DurationVector
	}{
		{32 * time.Second, "32s"},
		{32 * 7 * 24 * time.Hour, "32w"},
		{19 * time.Hour +
			32 * time.Minute +
			12 * time.Second +
			124 * time.Millisecond +
			482 * time.Microsecond +
			21 * time.Nanosecond,
			"19h32m12s124ms482u21ns"},
		{73 * 24 * time.Hour +
			19 * time.Hour +
			32 * time.Minute +
			12 * time.Second +
			124 * time.Millisecond +
			482 * time.Microsecond +
			21 * time.Nanosecond,
			"10w3d19h32m12s124ms482u21ns"},
	} {
		if real := DecomposeDuration(test.query); real != test.want {
			fmt.Printf("influxtime conversion mismatch; real: %s, want: %s\n", real, test.want)
			t.FailNow()
		}
	}
}
