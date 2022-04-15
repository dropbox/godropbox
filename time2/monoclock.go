package time2

import "time"

// monoClockEpoch is a completely arbitrary t0.
var monoClockEpoch = time.Now()

// MonoClock returns the number of nanoseconds from an arbitrary and unknowable
// epoch.  This value probably will not decrease between subsequent calls, but
// there are no guarantees.  If a system lacks CLOCK_MONOTONIC, this time could
// conceivably go backwards.  The value returned by MonoClock has NO MEANING
// WHATSOEVER outside of this process.  It also cannot be converted back into a
// time.Time.
//
// In most cases, prefer time.Time as a value for points in time and
// time.Duration for difference between two points.  Since Go 1.9, time.Time
// reads CLOCK_MONOTONIC and time.Add/Sub operate on those monotonic values.
// ```time.Since(t0).Nanoseconds()``` is isomorphic to ```MonoClock() - t0```.
func MonoClock() int64 {
	return time.Since(monoClockEpoch).Nanoseconds()
}
