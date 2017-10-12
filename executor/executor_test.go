package executor

import (
	"time"

	. "gopkg.in/check.v1"

	"github.com/dropbox/godropbox/time2"
)

type testRequest struct {
	i          int
	startChan  chan struct{}
	finishChan chan struct{}

	executeChan chan int
	cancelChan  chan int

	doneChan chan struct{}
}

func (r *testRequest) Execute() {
	r.startChan <- struct{}{}
	<-r.finishChan

	r.executeChan <- r.i
}

func (r *testRequest) Cancel() {
	r.startChan <- struct{}{}
	<-r.finishChan

	r.cancelChan <- r.i
}

func (r *testRequest) Finish() {
	close(r.finishChan)
}

func (r *testRequest) AssertNotStarted(c *C) {
	select {
	case <-r.startChan:
		c.Log(r.i, " started")
		c.FailNow()
	case <-time.After(10 * time.Millisecond):
	}
}

func (r *testRequest) WaitTilStarted(c *C) {
	select {
	case <-r.startChan:
	case <-time.After(100 * time.Millisecond):
		c.Log(r.i, " not started")
		c.FailNow()
	}
}

func (r *testRequest) AssertDone(c *C) {
	select {
	case <-r.doneChan:
		// ok
	case <-time.After(100 * time.Millisecond):
		c.Log(r.i, " not done")
		c.FailNow()
	}
}

func (r *testRequest) AssertNotDone(c *C) {
	select {
	case <-r.doneChan:
		c.Log(r.i, " done")
		c.FailNow()
	case <-time.After(10 * time.Millisecond):
		// ok
	}
}

type inlineTestRequest struct {
	testRequest
}

func (r *inlineTestRequest) CanExecuteInline() {
}

type WorkPoolExecutorSuite struct {
	clock *time2.MockClock

	i        int
	executor *WorkPoolExecutor

	executeChan chan int
	cancelChan  chan int
}

var _ = Suite(&WorkPoolExecutorSuite{})

func (s *WorkPoolExecutorSuite) SetUpTest(c *C) {
	s.clock = &time2.MockClock{}
	s.i = 1
	s.executor = NewWorkPoolExecutor()
	s.executor.now = s.clock.Now
	s.executor.after = s.clock.After
	s.executeChan = make(chan int, 1000)
	s.cancelChan = make(chan int, 1000)
}

func (s *WorkPoolExecutorSuite) TearDownTest(c *C) {
	// stop truncation loop
	err := s.executor.Configure(WorkPoolExecutorParams{})
	c.Assert(err, IsNil)
}

func (s *WorkPoolExecutorSuite) queue() *testRequest {
	i := s.i
	s.i += 1

	req := &testRequest{
		i:           i,
		startChan:   make(chan struct{}),
		finishChan:  make(chan struct{}),
		executeChan: s.executeChan,
		cancelChan:  s.cancelChan,
		doneChan:    make(chan struct{}),
	}

	wg := s.executor.Process(req)

	go func() {
		wg.Wait()
		close(req.doneChan)
	}()

	return req
}

func (s *WorkPoolExecutorSuite) TestConfigureError(c *C) {
	err := s.executor.Configure(
		WorkPoolExecutorParams{
			NumWorkers:    1,
			QueueTimeout:  time.Second,
			CheckInterval: 0,
		})
	c.Assert(err, NotNil)
}

func (s *WorkPoolExecutorSuite) TestResizeWorkPool(c *C) {
	err := s.executor.Configure(
		WorkPoolExecutorParams{
			NumWorkers: 1,
		})
	c.Assert(err, IsNil)

	workers := s.executor.getWorkers()
	c.Assert(len(workers), Equals, 1)
	c.Assert(workers[0].id, Equals, 0)
	c.Assert(workers[0].getShouldStop(), Equals, false)

	// bump up to 4 workers
	err = s.executor.Configure(
		WorkPoolExecutorParams{
			NumWorkers: 4,
		})
	c.Assert(err, IsNil)

	workers = s.executor.getWorkers()
	c.Assert(len(workers), Equals, 4)
	for i, w := range workers {
		c.Assert(w.id, Equals, i)
		c.Assert(w.getShouldStop(), Equals, false)
	}

	// shrink to 2 workers
	err = s.executor.Configure(
		WorkPoolExecutorParams{
			NumWorkers: 2,
		})
	c.Assert(err, IsNil)

	for i, w := range workers {
		if i < 2 {
			c.Assert(w.getShouldStop(), Equals, false)
		} else {
			c.Assert(w.getShouldStop(), Equals, true)
		}
	}

	workers = s.executor.getWorkers()
	c.Assert(len(workers), Equals, 2)
	for i, w := range workers {
		c.Assert(w.id, Equals, i)
		c.Assert(w.getShouldStop(), Equals, false)
	}

	// bump back up to 5 workers
	err = s.executor.Configure(
		WorkPoolExecutorParams{
			NumWorkers: 5,
		})
	c.Assert(err, IsNil)

	workers = s.executor.getWorkers()
	c.Assert(len(workers), Equals, 5)
	for i, id := range []int{0, 1, 4, 5, 6} {
		c.Assert(workers[i].id, Equals, id)
		c.Assert(workers[i].getShouldStop(), Equals, false)
	}

	// shrink back to 3 workers
	err = s.executor.Configure(
		WorkPoolExecutorParams{
			NumWorkers: 3,
		})
	c.Assert(err, IsNil)

	for i, _ := range []int{0, 1, 4, 5, 6} {
		if i < 3 {
			c.Assert(workers[i].getShouldStop(), Equals, false)
		} else {
			c.Assert(workers[i].getShouldStop(), Equals, true)
		}
	}

	workers = s.executor.getWorkers()
	c.Assert(len(workers), Equals, 3)
	for i, id := range []int{0, 1, 4} {
		c.Assert(workers[i].id, Equals, id)
		c.Assert(workers[i].getShouldStop(), Equals, false)
	}

	// "unlimited"
	err = s.executor.Configure(WorkPoolExecutorParams{})
	c.Assert(err, IsNil)

	for i, _ := range []int{0, 1, 4} {
		c.Assert(workers[i].getShouldStop(), Equals, true)
	}

	workers = s.executor.getWorkers()
	c.Assert(len(workers), Equals, 0)
}

func (s *WorkPoolExecutorSuite) TestMaxConcurrencyAndLifo(c *C) {
	err := s.executor.Configure(
		WorkPoolExecutorParams{
			NumWorkers: 2,
		})
	c.Assert(err, IsNil)

	workers := s.executor.getWorkers()
	c.Assert(len(workers), Equals, 2)

	req1 := s.queue()
	req2 := s.queue()

	req1.WaitTilStarted(c)
	req2.WaitTilStarted(c)

	req3 := s.queue()
	req4 := s.queue()
	req5 := s.queue()

	req3.Finish()
	req4.Finish()
	req5.Finish()

	req3.AssertNotStarted(c)
	req4.AssertNotStarted(c)
	req5.AssertNotStarted(c)

	req2.Finish()
	req2.AssertDone(c)

	req5.WaitTilStarted(c)
	req5.AssertDone(c)

	req4.WaitTilStarted(c)
	req4.AssertDone(c)

	req3.WaitTilStarted(c)
	req3.AssertDone(c)

	req1.Finish()
	req1.AssertDone(c)

	obtained := []int{}
	close(s.executeChan)
	for i := range s.executeChan {
		obtained = append(obtained, i)
	}
	c.Assert([]int{2, 5, 4, 3, 1}, DeepEquals, obtained)
}

func (s *WorkPoolExecutorSuite) TestDrainUnlimited(c *C) {
	// Queue up a bunch of requests as test setup.
	err := s.executor.Configure(
		WorkPoolExecutorParams{
			NumWorkers: 1,
		})
	c.Assert(err, IsNil)

	workers := s.executor.getWorkers()
	c.Assert(len(workers), Equals, 1)

	req1 := s.queue()

	req1.WaitTilStarted(c)

	req2 := s.queue()
	req3 := s.queue()
	req4 := s.queue()

	req2.Finish()
	req3.Finish()
	req4.Finish()

	req2.AssertNotStarted(c)
	req3.AssertNotStarted(c)
	req4.AssertNotStarted(c)

	// Resetting the executor to unlimited causes all queued requests to be
	// processed.
	err = s.executor.Configure(WorkPoolExecutorParams{})
	c.Assert(err, IsNil)

	c.Assert(len(s.executor.getWorkers()), Equals, 0)

	req2.WaitTilStarted(c)
	req3.WaitTilStarted(c)
	req4.WaitTilStarted(c)

	req2.AssertDone(c)
	req3.AssertDone(c)
	req4.AssertDone(c)

	req1.AssertNotDone(c)

	req1.Finish()
	req1.AssertDone(c)

	c.Assert(workers[0].getShouldStop(), Equals, true)

	obtained := []int{}
	close(s.executeChan)
	for i := range s.executeChan {
		obtained = append(obtained, i)
	}
	c.Assert(len(obtained), Equals, 4)
	c.Assert(obtained[3], Equals, 1)
}

func (s *WorkPoolExecutorSuite) TestInlineExecute(c *C) {
	req := &inlineTestRequest{
		testRequest{
			i:           0,
			startChan:   make(chan struct{}),
			finishChan:  make(chan struct{}),
			executeChan: s.executeChan,
			cancelChan:  s.cancelChan,
			doneChan:    make(chan struct{}),
		},
	}

	start := time.Now()

	go func() {
		req.WaitTilStarted(c)
		time.Sleep(200 * time.Millisecond)
		req.Finish()
	}()

	wg := s.executor.Process(req)

	end := time.Now()

	go func() {
		wg.Wait()
		close(req.doneChan)
	}()

	c.Assert(end.Sub(start) >= 200*time.Millisecond, Equals, true)
	req.AssertDone(c)
}

func retryAssert(c *C, cond func() bool) {
	if cond() {
		return
	}

	failCount := 0
	for ; failCount < 10; failCount++ {
		time.Sleep(100 * time.Millisecond)
		if cond() {
			break
		}
	}

	c.Assert(failCount < 10, Equals, true)
}

func (s *WorkPoolExecutorSuite) TestTimeOutOnPop(c *C) {
	err := s.executor.Configure(
		WorkPoolExecutorParams{
			NumWorkers:    1,
			QueueTimeout:  time.Second,
			CheckInterval: time.Minute,
		})
	c.Assert(err, IsNil)

	retryAssert(c, func() bool { return s.clock.WakeupsCount() > 0 })
	c.Assert(s.clock.NextWakeupTime(), Equals, time.Time{}.Add(time.Minute))

	req1 := s.queue()

	req1.WaitTilStarted(c)

	req2 := s.queue()
	req2.Finish()

	s.clock.Advance(time.Second + 1)

	req1.Finish()

	req2.WaitTilStarted(c)

	req1.AssertDone(c)
	req2.AssertDone(c)

	// After has not triggered.
	c.Assert(s.clock.NextWakeupTime(), Equals, time.Time{}.Add(time.Minute))

	close(s.executeChan)
	executed := []int{}
	for i := range s.executeChan {
		executed = append(executed, i)
	}

	c.Assert(len(executed), Equals, 1)
	c.Assert(executed[0], Equals, 1)

	close(s.cancelChan)
	cancelled := []int{}
	for i := range s.cancelChan {
		cancelled = append(cancelled, i)
	}

	c.Assert(len(cancelled), Equals, 1)
	c.Assert(cancelled[0], Equals, 2)
}

func (s *WorkPoolExecutorSuite) TestTimeOutTruncateLoop(c *C) {
	err := s.executor.Configure(
		WorkPoolExecutorParams{
			NumWorkers:    1,
			QueueTimeout:  time.Second,
			CheckInterval: time.Minute,
		})
	c.Assert(err, IsNil)

	// The first check should happen at the 1 minute mark.
	retryAssert(c, func() bool { return s.clock.WakeupsCount() > 0 })
	c.Assert(s.clock.NextWakeupTime(), Equals, time.Time{}.Add(time.Minute))

	req1 := s.queue()

	req1.WaitTilStarted(c)

	req2 := s.queue()
	req2.Finish()

	s.clock.Advance(time.Minute + 1)

	req2.WaitTilStarted(c)

	req1.Finish()
	req1.AssertDone(c)
	req2.AssertDone(c)

	// Reschedule to check at 2 minute mark.
	retryAssert(c, func() bool { return s.clock.WakeupsCount() > 0 })
	c.Assert(s.clock.NextWakeupTime(), Equals, time.Time{}.Add(2*time.Minute+1))

	close(s.executeChan)
	executed := []int{}
	for i := range s.executeChan {
		executed = append(executed, i)
	}

	c.Assert(len(executed), Equals, 1)
	c.Assert(executed[0], Equals, 1)

	close(s.cancelChan)
	cancelled := []int{}
	for i := range s.cancelChan {
		cancelled = append(cancelled, i)
	}

	c.Assert(len(cancelled), Equals, 1)
	c.Assert(cancelled[0], Equals, 2)
}

func (s *WorkPoolExecutorSuite) TestMaybeTruncate(c *C) {
	err := s.executor.Configure(
		WorkPoolExecutorParams{
			NumWorkers:    1,
			QueueTimeout:  5 * time.Second,
			CheckInterval: 1000 * time.Hour, // "disable"
		})
	c.Assert(err, IsNil)

	retryAssert(c, func() bool { return s.clock.WakeupsCount() > 0 })

	req1 := s.queue()

	req1.WaitTilStarted(c)

	requests := []*testRequest{}
	for i := 0; i < 8; i++ {
		req := s.queue()
		req.Finish()
		requests = append(requests, req)
		s.clock.Advance(time.Second)
	}

	// At 9 second, the requests 2-6 timed out.
	s.clock.Advance(time.Second + 1)

	s.executor.maybeTruncate(s.clock.Now())

	for i, req := range requests {
		if i < 5 {
			req.WaitTilStarted(c)
			req.AssertDone(c)
		} else {
			req.AssertNotStarted(c)
			req.AssertNotDone(c)
		}
	}

	req1.Finish()
	req1.AssertDone(c)

	// Backlogs are processed in lifo order.
	for i := 7; i >= 5; i-- {
		req := requests[i]
		req.WaitTilStarted(c)
		req.AssertDone(c)
	}

	close(s.executeChan)
	executed := []int{}
	for i := range s.executeChan {
		executed = append(executed, i)
	}

	c.Assert(len(executed), Equals, 4)
	c.Assert(executed, DeepEquals, []int{1, 9, 8, 7})

	close(s.cancelChan)
	cancelled := make(map[int]struct{})
	for i := range s.cancelChan {
		cancelled[i] = struct{}{}
	}

	c.Assert(len(cancelled), Equals, 5)
	c.Assert(
		cancelled,
		DeepEquals,
		map[int]struct{}{
			2: {},
			3: {},
			4: {},
			5: {},
			6: {},
		})
}
