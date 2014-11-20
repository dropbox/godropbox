package time2

import "time"

// Return the time as a float64, ala Python's time.time().
func NowFloat() float64 {
	return TimeToFloat(time.Now())
}

// Convert Time to epoch seconds with subsecond precision.
func TimeToFloat(t time.Time) float64 {
	return float64(t.Unix()) + (float64(t.Nanosecond()) / 1e9)
}
