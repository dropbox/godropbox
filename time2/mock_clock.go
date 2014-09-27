package time2

import (
	"time"
)

// A fake clock useful for testing timing.
type MockClock struct {
	currentTime time.Time
}

// Resets the mock clock back to initial state.
func (c *MockClock) Reset() {
	c.currentTime = time.Time{}
}

// Set the mock clock to a specific time.
func (c *MockClock) Set(t time.Time) {
	c.currentTime = t
}

// Advances the mock clock by the specified duration.
func (c *MockClock) Advance(delta time.Duration) {
	c.currentTime = c.currentTime.Add(delta)
}

// Returns the fake current time.
func (c *MockClock) Now() time.Time {
	return c.currentTime
}

// Returns the time elapsed since the fake current time.
func (c *MockClock) Since(t time.Time) time.Duration {
	return c.currentTime.Sub(t)
}
