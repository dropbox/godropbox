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

// Advances the mock clock by the specified duration.
func (c *MockClock) Advance(delta time.Duration) {
	c.currentTime = c.currentTime.Add(delta)
}

// Returns the fake current time.
func (c *MockClock) Now() time.Time {
	return c.currentTime
}
