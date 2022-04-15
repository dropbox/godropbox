package executor

import (
	"github.com/stretchr/testify/require"
	. "gopkg.in/check.v1"
	"testing"
	"time"
)

type WorkPoolExecutorWithPriorityQueueSuite struct {
	*WorkPoolExecutorSuite
}

var _ = Suite(&WorkPoolExecutorWithPriorityQueueSuite{})

func (s *WorkPoolExecutorWithPriorityQueueSuite) SetUpTest(c *C) {
	s.WorkPoolExecutorSuite = &WorkPoolExecutorSuite{}
	s.WorkPoolExecutorSuite.SetUpTest(c)

	s.executor = NewWorkPoolExecutor(WithPriorityQueue([]uint16{1}))
	s.executor.now = s.clock.Now
	s.executor.after = s.clock.After
}

func testPrioritizedRequest(enqueueTime time.Time, priority uint16) *queuedRequest {
	return &queuedRequest{request: &prioritizedRequest{pri: priority}, enqueueTime: enqueueTime}
}

func TestPriorityQueueDrain(t *testing.T) {
	for name, testCase := range map[string]struct {
		requests []*queuedRequest
	}{
		"no requests": {},
		"one requests": {
			requests: []*queuedRequest{
				testPrioritizedRequest(time.Time{}, 1),
			},
		},
		"many requests": {
			requests: []*queuedRequest{
				testPrioritizedRequest(time.Time{}, 1),
				testPrioritizedRequest(time.Time{}, 1),
				testPrioritizedRequest(time.Time{}, 2),
				testPrioritizedRequest(time.Time{}, 2),
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			q := newPriorityQueue([]uint16{1, 2})
			for _, req := range testCase.requests {
				q.Push(req, len(testCase.requests))
			}

			require.Equal(t, len(testCase.requests), q.Size())

			got := q.Drain()
			require.ElementsMatch(t, testCase.requests, got)
			require.Zero(t, q.Size())
		})
	}
}

func TestPriorityQueuePushPop(t *testing.T) {
	for name, testCase := range map[string]struct {
		requests []*queuedRequest

		want []int // order in which requests should be popped.
	}{
		"no requests": {
			want: nil,
		},
		"one requests": {
			requests: []*queuedRequest{
				testPrioritizedRequest(time.Time{}, 1),
			},
			want: []int{0},
		},
		"many requests": {
			requests: []*queuedRequest{
				testPrioritizedRequest(time.Time{}, 2),
				testPrioritizedRequest(time.Time{}, 2),
				testPrioritizedRequest(time.Time{}, 1),
				testPrioritizedRequest(time.Time{}, 4),
				testPrioritizedRequest(time.Time{}, 1),
				testPrioritizedRequest(time.Time{}, 3),
			},
			want: []int{2, 4, 0, 1, 5, 3},
		},
		"new high priority": {
			requests: []*queuedRequest{
				testPrioritizedRequest(time.Time{}, 2),
				testPrioritizedRequest(time.Time{}, 1),
				testPrioritizedRequest(time.Time{}, 0), // will be treated as 1
			},
			want: []int{2, 1, 0},
		},
		"new low priority": {
			requests: []*queuedRequest{
				testPrioritizedRequest(time.Time{}, 4),
				testPrioritizedRequest(time.Time{}, 1),
				testPrioritizedRequest(time.Time{}, 8), // will be treated as 4
			},
			want: []int{1, 2, 0},
		},
	} {
		t.Run(name, func(t *testing.T) {
			q := newPriorityQueue([]uint16{1, 2, 3, 4})
			for _, req := range testCase.requests {
				q.Push(req, len(testCase.requests))
			}

			want := testCase.want
			require.Equal(t, len(want), q.Size())
			for {
				got := q.Pop()
				if len(want) == 0 {
					require.Nil(t, got)
					break
				} else {
					require.Equal(t, testCase.requests[want[0]], got)
					want = want[1:]
				}
			}
		})
	}
}

func TestPriorityQueuePruneExpiredRequests(t *testing.T) {
	for name, testCase := range map[string]struct {
		requests []*queuedRequest
		cutoff   time.Time
	}{
		"no requests": {},
		"one requests after cutoff": {
			requests: []*queuedRequest{
				testPrioritizedRequest(time.Unix(2, 0), 1),
			},
			cutoff: time.Unix(1, 0),
		},
		"one request at cutoff": {
			requests: []*queuedRequest{
				testPrioritizedRequest(time.Unix(2, 0), 1),
			},
			cutoff: time.Unix(2, 0),
		},
		"one request before cutoff": {
			requests: []*queuedRequest{
				testPrioritizedRequest(time.Unix(2, 0), 1),
			},
			cutoff: time.Unix(3, 0),
		},
		"many requests": {
			requests: []*queuedRequest{
				testPrioritizedRequest(time.Unix(1, 0), 1),
				testPrioritizedRequest(time.Unix(1, 0), 2),
				testPrioritizedRequest(time.Unix(1, 0), 3),
				testPrioritizedRequest(time.Unix(2, 0), 1),
				testPrioritizedRequest(time.Unix(2, 0), 2),
				testPrioritizedRequest(time.Unix(2, 0), 3),
				testPrioritizedRequest(time.Unix(3, 0), 1),
				testPrioritizedRequest(time.Unix(3, 0), 2),
				testPrioritizedRequest(time.Unix(3, 0), 3),
			},
			cutoff: time.Unix(2, 0),
		},
	} {
		t.Run(name, func(t *testing.T) {
			q := newPriorityQueue([]uint16{1, 2, 3})
			for _, req := range testCase.requests {
				q.Push(req, len(testCase.requests))
			}

			got := q.PruneExpiredRequests(testCase.cutoff)

			var want []*queuedRequest
			for _, req := range testCase.requests {
				if req.enqueueTime.Before(testCase.cutoff) {
					want = append(want, req)
				}
			}

			require.ElementsMatch(t, want, got)
			require.Equal(t, len(testCase.requests)-len(got), q.Size())

			again := q.PruneExpiredRequests(testCase.cutoff)
			require.Empty(t, again)

			require.ElementsMatch(t, testCase.requests, append(got, q.Drain()...))
		})
	}
}
