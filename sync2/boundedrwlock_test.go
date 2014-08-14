package sync2

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

func TestBoundedRWLock(t *testing.T) {
	TestingT(t)
}

type BoundedRWLockSuite struct {
}

var _ = Suite(&BoundedRWLockSuite{})

func (suite *BoundedRWLockSuite) TestWriterTimeout(t *C) {
	clocked := make(chan bool)
	cunlock := make(chan bool)
	cunlocked := make(chan bool)
	var rwl *BoundedRWLock = NewBoundedRWLock(100)
	go func() {
		if err := rwl.RLock(100 * time.Millisecond); err != nil {
			panic(fmt.Sprintf("contention??"))
		}

		clocked <- true
		<-cunlock
		rwl.RUnlock()
		cunlocked <- true
	}()

	<-clocked
	begin := time.Now()
	err := rwl.WLock(100 * time.Millisecond)
	t.Assert(err, NotNil)
	t.Assert(time.Now().Sub(begin) >= 100*time.Millisecond, Equals, true)

	cunlock <- true
	<-cunlocked
	err = rwl.WLock(100 * time.Millisecond)
	t.Assert(err, IsNil)
	rwl.WUnlock()
}

func (suite *BoundedRWLockSuite) TestReaderTimeout(t *C) {
	clocked := make(chan bool)
	cunlock := make(chan bool)
	cunlocked := make(chan bool)
	var rwl *BoundedRWLock = NewBoundedRWLock(100)
	go func() {
		if err := rwl.WLock(100 * time.Millisecond); err != nil {
			panic(fmt.Sprintf("contention??"))
		}

		clocked <- true
		<-cunlock
		rwl.WUnlock()
		cunlocked <- true
	}()

	<-clocked
	begin := time.Now()
	err := rwl.RLock(100 * time.Millisecond)
	t.Assert(err, NotNil)
	t.Assert(time.Now().Sub(begin) >= 100*time.Millisecond, Equals, true)

	cunlock <- true
	<-cunlocked
	err = rwl.RLock(100 * time.Millisecond)
	t.Assert(err, IsNil)
	rwl.RUnlock()
}

func doTestParallelReaders(gomaxprocs, numReaders int) {
	runtime.GOMAXPROCS(gomaxprocs)

	var rwl *BoundedRWLock = NewBoundedRWLock(int(numReaders))
	clocked := make(chan bool)
	cunlock := make(chan bool)
	cdone := make(chan bool)

	reader := func() {
		if err := rwl.RLock(100 * time.Millisecond); err != nil {
			panic(fmt.Sprintf("contention??"))
		}

		clocked <- true
		<-cunlock
		rwl.RUnlock()
		cdone <- true
	}

	// kick off parallel readers
	for i := 0; i < numReaders; i++ {
		go reader()
	}

	// wait for them to all acquire the rlock
	for i := 0; i < numReaders; i++ {
		<-clocked
	}

	// ask them to unlock
	for i := 0; i < numReaders; i++ {
		cunlock <- true
	}

	// wait for them to unlock
	for i := 0; i < numReaders; i++ {
		<-cdone
	}
}

func (suite *BoundedRWLockSuite) TestParallelReaders(t *C) {
	// restore the original value after we are done with the test
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))

	doTestParallelReaders(1, 4)
	doTestParallelReaders(3, 4)
	doTestParallelReaders(4, 2)
	doTestParallelReaders(8, 100)
}

// Stress test adapted from sync/rwmutex_test.go
func stressRWMutex(gomaxprocs int, numReaders int32, numIterations int) {
	runtime.GOMAXPROCS(gomaxprocs)

	var counter int32
	var rwl *BoundedRWLock = NewBoundedRWLock(int(numReaders + 2))
	cdone := make(chan bool)
	var maxLockWait = 100 * time.Millisecond

	writer := func() {
		for i := 0; i < numIterations; i++ {
			if err := rwl.WLock(maxLockWait); err != nil {
				i--
				continue
			}
			n := atomic.AddInt32(&counter, numReaders+1)
			if n != numReaders+1 {
				panic(fmt.Sprintf("counter=%d, another writer/reader??", counter))
			}
			for j := 0; j < 100; j++ {
			}
			atomic.AddInt32(&counter, -1*(numReaders+1))
			rwl.WUnlock()
		}
		cdone <- true
	}

	reader := func() {
		for i := 0; i < numIterations; i++ {
			if err := rwl.RLock(maxLockWait); err != nil {
				i--
				continue
			}
			n := atomic.AddInt32(&counter, 1)
			if n < 1 || n >= numReaders+1 {
				panic(fmt.Sprintf("counter=%d, writer??\n", counter))
			}
			for j := 0; j < 100; j++ {
			}
			atomic.AddInt32(&counter, -1)
			rwl.RUnlock()
		}
		cdone <- true
	}

	go writer()
	var i int
	for i = 0; i < int(numReaders/2); i++ {
		go reader()
	}
	go writer()
	for ; i < int(numReaders); i++ {
		go reader()
	}

	// wait for 2 writers + all readers to finish
	for i := 0; i < 2+int(numReaders); i++ {
		<-cdone
	}
}

func (suite *BoundedRWLockSuite) TestRWMutex(t *C) {
	// restore the original value after we are done with the test
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))

	numIterations := 1000
	stressRWMutex(1, 1, numIterations)
	stressRWMutex(1, 1, numIterations)
	stressRWMutex(1, 3, numIterations)
	stressRWMutex(1, 10, numIterations)
	stressRWMutex(4, 1, numIterations)
	stressRWMutex(4, 3, numIterations)
	stressRWMutex(4, 10, numIterations)
	stressRWMutex(10, 1, numIterations)
	stressRWMutex(10, 3, numIterations)
	stressRWMutex(10, 10, numIterations)
	stressRWMutex(10, 5, numIterations)
	stressRWMutex(10, 100, numIterations)
}
