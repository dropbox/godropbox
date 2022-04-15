package context2

import (
	"context"
	"time"
)

// A mock implementation of context.Context allowing specifying the return value of Err()
// This mock object doesn't implement the other parts of context cancelation/deadline handling,
// such as the Done() channel.
type MockContextError struct {
	Error error
}

var _ context.Context = &MockContextError{}

func (c *MockContextError) Deadline() (deadline time.Time, ok bool) {
	// does not support setting deadline so always return ok=false
	return time.Time{}, false
}

func (c *MockContextError) Done() <-chan struct{} {
	panic("unimplemented")
}

func (c *MockContextError) Err() error {
	return c.Error
}

func (c *MockContextError) Value(key interface{}) interface{} {
	return nil
}
