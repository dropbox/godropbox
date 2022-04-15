package time2

import (
	"context"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"testing"
	"time"
)

func TestSleepOrExpire(t *testing.T) {
	ctx := context.Background()

	// no error if the context has no deadline and is not cancelled
	err := SleepOrExpire(ctx, 100*time.Millisecond)
	require.NoError(t, err)

	// test that cancelling the context works
	ctx, cancel := context.WithCancel(context.Background())
	eg := errgroup.Group{}
	t0 := time.Now()
	eg.Go(func() error {
		return SleepOrExpire(ctx, 10*time.Minute)
	})
	cancel()
	err = eg.Wait()
	require.Error(t, err)
	require.Equal(t, ctx.Err(), err)
	require.Less(t, time.Now().Sub(t0).Seconds(), 10*time.Second.Seconds())

	// test that context timeout works
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	t0 = time.Now()
	err = SleepOrExpire(ctx, 10*time.Minute)
	require.Error(t, err)
	require.Equal(t, context.DeadlineExceeded, err)
	require.Less(t, time.Now().Sub(t0).Seconds(), 10*time.Second.Seconds())

	// test immediate return
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t0 = time.Now()
	err = SleepOrExpire(ctx, 20*time.Minute)
	require.Error(t, err)
	require.Equal(t, context.DeadlineExceeded, err)
	require.Less(t, time.Now().Sub(t0).Seconds(), 10*time.Second.Seconds())
}
