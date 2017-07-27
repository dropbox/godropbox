package filelock

import (
	"errors"
	"testing"
	"time"

	. "github.com/dropbox/godropbox/gocheck2"

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
	lockName := "testflock0"

	fl1 := New(lockName)
	fl2 := New(lockName)

	c.Assert(fl1.Lock(), IsNil)
	err := fl2.TryLock()
	c.Assert(err, NotNil)
	c.Assert(IsHeldElsewhere(err), IsTrue)

	// unlock it now and try again.
	c.Assert(fl1.Unlock(), IsNil)
	err = fl2.TryLock()
	c.Assert(err, IsNil)
	c.Assert(IsHeldElsewhere(err), IsFalse)

	// now perform blocking Lock in Go Routine
	doneChan := make(chan struct{}, 0)
	go func() {
		c.Assert(fl1.Lock(), IsNil)
		close(doneChan)
	}()

	select {
	case <-doneChan:
		c.Fatal("Locking block didn't actually block!")
	case <-time.After(10 * time.Millisecond):
	}

	c.Assert(fl2.Unlock(), IsNil)

	<-doneChan
	c.Assert(fl1.Unlock(), IsNil)
}

func (s *FileLockSuite) TestSharedSimple(c *C) {
	lockName := "testflock1"

	fl1 := New(lockName)
	fl2 := New(lockName)
	fl3 := New(lockName)

	// test we can acquire multiple shared locks
	c.Assert(fl1.RLock(), IsNil)
	c.Assert(fl2.TryRLock(), IsNil)
	c.Assert(fl3.TryLock(), NotNil)

	// unlock one shared lock, exclusive lock should fail
	c.Assert(fl1.RUnlock(), IsNil)
	c.Assert(fl3.TryLock(), NotNil)

	// unlock both shared locks, exclusive lock should succeed
	c.Assert(fl2.RUnlock(), IsNil)
	c.Assert(fl3.TryLock(), IsNil)

	// now try to acquire blocking shared locks in Go Routine
	doneChan := make(chan struct{}, 0)
	go func() {
		c.Assert(fl1.RLock(), IsNil)
		c.Assert(fl2.RLock(), IsNil)
		close(doneChan)
	}()

	select {
	case <-doneChan:
		c.Fatal("Locking block didn't actually block!")
	case <-time.After(10 * time.Millisecond):
	}

	// shared locks should succeed as soon as we Unlock
	c.Assert(fl3.Unlock(), IsNil)

	<-doneChan

	c.Assert(fl1.RUnlock(), IsNil)

	// lock exclusive should not succeed until all RUnlocked
	doneChan = make(chan struct{}, 0)
	go func() {
		c.Assert(fl3.Lock(), IsNil)
		close(doneChan)
	}()

	select {
	case <-doneChan:
		c.Fatal("Locking block didn't actually block!")
	case <-time.After(10 * time.Millisecond):
	}

	c.Assert(fl2.RUnlock(), IsNil)
	<-doneChan
	c.Assert(fl3.Unlock(), IsNil)

}

func (s *FileLockSuite) TestReuseSimple(c *C) {
	lockName := "testflock2"

	fl1 := New(lockName)
	fl2 := New(lockName)
	fl3 := New(lockName)

	// try using fl1 as an exclusive lock
	c.Assert(fl1.Lock(), IsNil)
	c.Assert(fl2.TryLock(), NotNil)
	c.Assert(fl3.TryRLock(), NotNil)
	c.Assert(fl1.Unlock(), IsNil)

	// now try using fl1 as a read lock
	c.Assert(fl1.RLock(), IsNil)
	c.Assert(fl3.RLock(), IsNil)
	c.Assert(fl2.TryLock(), NotNil)
	c.Assert(fl1.RUnlock(), IsNil)
	c.Assert(fl3.RUnlock(), IsNil)
}

func (s *FileLockSuite) TestAllTransitions(c *C) {
	wouldBlock := ".*FileLock: would block(.|\n)*"
	blocked := ".*FileLock: blocked(.|\n)*"
	unacquired := ".*FileLock: never acquired(.|\n)*"
	wrongType := ".*FileLock: wrong lock type(.|\n)*"
	doubleLock := ".*FileLock: already acquired(.|\n)*"

	states := []string{"UNLOCKED", "LOCKED", "RLOCKED"}
	actions := []string{"TryRLock", "RLock", "TryLock", "Lock", "RUnlock", "Unlock"}
	errResults := make(map[string]map[string]map[string]error)

	for _, backgroundState := range states {
		errResults[backgroundState] = make(map[string]map[string]error)
		for _, testLockState := range states {
			errResults[backgroundState][testLockState] = make(map[string]error)
			for _, action := range actions {
				errChan := make(chan error, 1)
				go performTransition(errChan, backgroundState, testLockState, action)
				select {
				case err := <-errChan:
					if IsHeldElsewhere(err) {
						errResults[backgroundState][testLockState][action] = errors.New(wouldBlock)
					} else {
						errResults[backgroundState][testLockState][action] = err
					}
				case <-time.After(50 * time.Millisecond):
					errResults[backgroundState][testLockState][action] = errors.New(blocked)
				}
			}
		}
	}

	// errResults[kernelState][testState][testAction]
	c.Assert(errResults["UNLOCKED"]["UNLOCKED"]["TryRLock"], IsNil)
	c.Assert(errResults["UNLOCKED"]["UNLOCKED"]["RLock"], IsNil)
	c.Assert(errResults["UNLOCKED"]["UNLOCKED"]["TryLock"], IsNil)
	c.Assert(errResults["UNLOCKED"]["UNLOCKED"]["Lock"], IsNil)
	c.Assert(errResults["UNLOCKED"]["UNLOCKED"]["RUnlock"], ErrorMatches, unacquired)
	c.Assert(errResults["UNLOCKED"]["UNLOCKED"]["Unlock"], ErrorMatches, unacquired)

	c.Assert(errResults["UNLOCKED"]["RLOCKED"]["TryRLock"], ErrorMatches, doubleLock)
	c.Assert(errResults["UNLOCKED"]["RLOCKED"]["RLock"], ErrorMatches, doubleLock)
	c.Assert(errResults["UNLOCKED"]["RLOCKED"]["TryLock"], ErrorMatches, doubleLock)
	c.Assert(errResults["UNLOCKED"]["RLOCKED"]["Lock"], ErrorMatches, doubleLock)
	c.Assert(errResults["UNLOCKED"]["RLOCKED"]["RUnlock"], IsNil)
	c.Assert(errResults["UNLOCKED"]["RLOCKED"]["Unlock"], ErrorMatches, wrongType)

	c.Assert(errResults["UNLOCKED"]["LOCKED"]["TryRLock"], ErrorMatches, doubleLock)
	c.Assert(errResults["UNLOCKED"]["LOCKED"]["RLock"], ErrorMatches, doubleLock)
	c.Assert(errResults["UNLOCKED"]["LOCKED"]["TryLock"], ErrorMatches, doubleLock)
	c.Assert(errResults["UNLOCKED"]["LOCKED"]["Lock"], ErrorMatches, doubleLock)
	c.Assert(errResults["UNLOCKED"]["LOCKED"]["RUnlock"], ErrorMatches, wrongType)
	c.Assert(errResults["UNLOCKED"]["LOCKED"]["Unlock"], IsNil)

	c.Assert(errResults["RLOCKED"]["UNLOCKED"]["TryRLock"], IsNil)
	c.Assert(errResults["RLOCKED"]["UNLOCKED"]["RLock"], IsNil)
	c.Assert(errResults["RLOCKED"]["UNLOCKED"]["TryLock"], ErrorMatches, wouldBlock)
	c.Assert(errResults["RLOCKED"]["UNLOCKED"]["Lock"], ErrorMatches, blocked)
	c.Assert(errResults["RLOCKED"]["UNLOCKED"]["RUnlock"], ErrorMatches, unacquired)
	c.Assert(errResults["RLOCKED"]["UNLOCKED"]["Unlock"], ErrorMatches, unacquired)

	c.Assert(errResults["RLOCKED"]["RLOCKED"]["TryRLock"], ErrorMatches, doubleLock)
	c.Assert(errResults["RLOCKED"]["RLOCKED"]["RLock"], ErrorMatches, doubleLock)
	c.Assert(errResults["RLOCKED"]["RLOCKED"]["TryLock"], ErrorMatches, doubleLock)
	c.Assert(errResults["RLOCKED"]["RLOCKED"]["Lock"], ErrorMatches, doubleLock)
	c.Assert(errResults["RLOCKED"]["RLOCKED"]["RUnlock"], IsNil)
	c.Assert(errResults["RLOCKED"]["RLOCKED"]["Unlock"], ErrorMatches, wrongType)

	// errResults["RLOCKED"]["LOCKED"] is a clearly invalid setup, everything should be blocked
	c.Assert(errResults["RLOCKED"]["LOCKED"]["TryRLock"], ErrorMatches, blocked)
	c.Assert(errResults["RLOCKED"]["LOCKED"]["RLock"], ErrorMatches, blocked)
	c.Assert(errResults["RLOCKED"]["LOCKED"]["TryLock"], ErrorMatches, blocked)
	c.Assert(errResults["RLOCKED"]["LOCKED"]["Lock"], ErrorMatches, blocked)
	c.Assert(errResults["RLOCKED"]["LOCKED"]["RUnlock"], ErrorMatches, blocked)
	c.Assert(errResults["RLOCKED"]["LOCKED"]["Unlock"], ErrorMatches, blocked)

	c.Assert(errResults["LOCKED"]["UNLOCKED"]["TryRLock"], ErrorMatches, wouldBlock)
	c.Assert(errResults["LOCKED"]["UNLOCKED"]["RLock"], ErrorMatches, blocked)
	c.Assert(errResults["LOCKED"]["UNLOCKED"]["TryLock"], ErrorMatches, wouldBlock)
	c.Assert(errResults["LOCKED"]["UNLOCKED"]["Lock"], ErrorMatches, blocked)
	c.Assert(errResults["LOCKED"]["UNLOCKED"]["RUnlock"], ErrorMatches, unacquired)
	c.Assert(errResults["LOCKED"]["UNLOCKED"]["Unlock"], ErrorMatches, unacquired)

	// errResults["LOCKED"]["RLOCKED"] is a clearly invalid setup, everything should be blocked
	c.Assert(errResults["LOCKED"]["RLOCKED"]["TryRLock"], ErrorMatches, blocked)
	c.Assert(errResults["LOCKED"]["RLOCKED"]["RLock"], ErrorMatches, blocked)
	c.Assert(errResults["LOCKED"]["RLOCKED"]["TryLock"], ErrorMatches, blocked)
	c.Assert(errResults["LOCKED"]["RLOCKED"]["Lock"], ErrorMatches, blocked)
	c.Assert(errResults["LOCKED"]["RLOCKED"]["RUnlock"], ErrorMatches, blocked)
	c.Assert(errResults["LOCKED"]["RLOCKED"]["Unlock"], ErrorMatches, blocked)

	// errResults["LOCKED"]["LOCKED"] is a clearly invalid setup, everything should be blocked
	c.Assert(errResults["LOCKED"]["LOCKED"]["TryRLock"], ErrorMatches, blocked)
	c.Assert(errResults["LOCKED"]["LOCKED"]["RLock"], ErrorMatches, blocked)
	c.Assert(errResults["LOCKED"]["LOCKED"]["TryLock"], ErrorMatches, blocked)
	c.Assert(errResults["LOCKED"]["LOCKED"]["Lock"], ErrorMatches, blocked)
	c.Assert(errResults["LOCKED"]["LOCKED"]["RUnlock"], ErrorMatches, blocked)
	c.Assert(errResults["LOCKED"]["LOCKED"]["Unlock"], ErrorMatches, blocked)
}

func performTransition(errChan chan error, bgState, testState, action string) {
	lockName := bgState + testState + action
	bgLock := New(lockName)
	tLock := New(lockName)

	if bgState == "LOCKED" {
		bgLock.Lock()
	} else if bgState == "RLOCKED" {
		bgLock.RLock()
	}

	if testState == "LOCKED" {
		tLock.Lock()
	} else if testState == "RLOCKED" {
		tLock.RLock()
	}

	switch action {
	case "TryRLock":
		errChan <- tLock.TryRLock()
	case "RLock":
		errChan <- tLock.RLock()
	case "TryLock":
		errChan <- tLock.TryLock()
	case "Lock":
		errChan <- tLock.Lock()
	case "RUnlock":
		errChan <- tLock.RUnlock()
	case "Unlock":
		errChan <- tLock.Unlock()
	}
}
