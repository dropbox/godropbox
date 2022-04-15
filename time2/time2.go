package time2

import (
	"context"
	"math"
	"time"

	insecure_rand "godropbox/math2/rand2"
)

// Return the time as a float64, ala Python's time.time().
func NowFloat() float64 {
	return TimeToFloat(time.Now())
}

// Convert Time to epoch seconds with subsecond precision.
func TimeToFloat(t time.Time) float64 {
	return float64(t.Unix()) + (float64(t.Nanosecond()) / 1e9)
}

func FloatToTime(f float64) time.Time {
	fs, fns := math.Modf(f)
	i64s := int64(math.Trunc(fs))
	i64ns := int64(math.Trunc(1e9 * fns))
	return time.Unix(i64s, i64ns)
}

// Compute an exponential backoff. gen == 0 is the first backoff.
func ExpBackoff(gen int, minDelay, maxDelay time.Duration) time.Duration {
	delay := math.Min(math.Pow(2, float64(gen))*float64(minDelay), float64(maxDelay))
	return time.Duration(delay)
}

// Uniformly jitters the provided duration by +/- 50%.
func Jitter(period time.Duration) time.Duration {
	if period == 0 {
		return 0
	}
	return period/2 + time.Duration(insecure_rand.Int63n(int64(period)))
}

// Uniformly jitters between the two provided durantions. If max <= min, return min.
func JitterRange(min time.Duration, max time.Duration) time.Duration {
	d := int64(max - min)
	if d <= 0 {
		return min
	}
	return min + time.Duration(insecure_rand.Int63n(d))
}

func MinDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func MaxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// If ctx has a deadline before (now + d), SleepOrExpire immediately returns context.DeadlineExceeded.
// Otherwise, SleepOrExpire blocks until ctx.Done() is closed or d passes,
// If ctx.Done() is closed before d passes, SleepOrExpire returns ctx.Err(). Otherwise, returns nil.
func SleepOrExpire(ctx context.Context, d time.Duration) error {
	deadline, ok := ctx.Deadline()
	if ok && time.Now().Add(d).After(deadline) {
		return context.DeadlineExceeded
	}
	timer := time.NewTimer(d)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}