package flock

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func genLockfilePath() string {
	return fmt.Sprintf("/tmp/flock-test-%d.%d.lock", rand.Int63(), os.Getpid())
}

func getLockfile(t *testing.T, path string) *Flock {
	lock, err := Open(path)
	require.NoError(t, err)
	return lock
}

func TestLock(t *testing.T) {
	lock := getLockfile(t, genLockfilePath())

	err := lock.Lock()
	require.NoError(t, err)

	err = lock.Unlock()
	require.NoError(t, err)
}

func TestTryLock(t *testing.T) {
	lock := getLockfile(t, genLockfilePath())

	locked, err := lock.TryLock()
	require.NoError(t, err)
	require.True(t, locked)

	err = lock.Unlock()
	require.NoError(t, err)
}

func TestTryLockFailsWhenHeld(t *testing.T) {
	path := genLockfilePath()
	lock := getLockfile(t, path)
	lock2 := getLockfile(t, path)

	err := lock.Lock()
	require.NoError(t, err)

	locked, err := lock2.TryLock()
	require.NoError(t, err)
	require.False(t, locked)

	err = lock.Unlock()
	require.NoError(t, err)

	locked, err = lock2.TryLock()
	require.NoError(t, err)
	require.True(t, locked)

	err = lock2.Unlock()
	require.NoError(t, err)
}
