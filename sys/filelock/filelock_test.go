package filelock

import (
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the go test runner.
func Test(t *testing.T) {
	TestingT(t)
}

type FileLockSuite struct {
}

var _ = Suite(&FileLockSuite{})

func (s *FileLockSuite) TestSimple(c *C) {
	fl1 := New("testflock1")
	err := fl1.Lock()
	c.Assert(err, IsNil)

	fl2 := New("testflock1")
	err = fl2.TryLock()
	c.Assert(err, NotNil)

	// unlock it now and try again.
	err = fl1.Unlock()
	c.Assert(err, IsNil)

	err = fl2.TryLock()
	c.Assert(err, IsNil)

	// Now perform blocking Lock in Go Routine
	doneChan := make(chan struct{}, 0)
	go func() {
		fl1.Lock()
		close(doneChan)
	}()

	select {
	case <-doneChan:
		c.Fatal("Locking block didn't actually block!")
	case <-time.After(10 * time.Millisecond):
	}

	err = fl2.Unlock()
	c.Assert(err, IsNil)

	<-doneChan
	err = fl1.Unlock()
	c.Assert(err, IsNil)
}
