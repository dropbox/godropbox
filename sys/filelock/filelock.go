// FileLock object can be used to create locks that can be used to synchronise
// different processes running on the same machine.
//
// The current implementation of FileLock will always release all acquired locks
// once the process itself exits.
//
// In order to reduce syscall overhead/complexity, this does not implement a fair lock.
// In particular, flock is reader-preferred and may starve writers. If you require
// fairness, you can implement it on top of this interface using multiple FileLocks.
//
// NOTE: FileLock is not thread safe, since it is not meant to be used from multiple
// threads but from different processes instead!
package filelock

import (
	"godropbox/errors"
	"os"
	"path"
	"syscall"
)

type FileLock struct {
	name   string
	path   string
	prefix string

	flockType int
	fileH     *os.File
}

// Creates new FileLock object given a unique name.
func New(name string) *FileLock {
	return NewInDir("/tmp", name)
}

// Creates new FileLock object given a unique name and a directory to store the file.
// NOTE:
// 1) The directory must already exist and must have write permissions.
// 2) Default location of file locks is /tmp. If you are specifying a different
// directory here or changing the directory of an existing file lock
// make sure you do not break mutual exclusion for production services and applications.
// If prior versions of your service/app are creating these locks in default/old location.
// then new version must acquire file locks in both old and new directories to migrate
// safely.
func NewInDir(dir string, name string) *FileLock {
	return &FileLock{
		name:   name,
		path:   dir,
		prefix: "flock-",
	}
}

func (f *FileLock) performLock(flockHow int) (performErr error) {
	if f.fileH != nil {
		return errors.Newf("FileLock: already acquired!")
	}

	filePath := path.Join(f.path, f.prefix+f.name)
	// GoLang os.OpenFile creates file with FD_CLOEXEC flag already set on it.
	// This means file will get closed automatically once the process exits,
	// thus we dont need to manually set that flag.
	fileH, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer func() {
		if performErr != nil {
			_ = fileH.Close()
		}
	}()

	if err := syscall.Flock(int(fileH.Fd()), flockHow); err != nil {
		return err
	}

	f.flockType = flockHow
	f.fileH = fileH
	return nil
}

// Non blocking way to try to acquire a shared lock.
func (f *FileLock) TryRLock() error {
	return f.performLock(syscall.LOCK_SH | syscall.LOCK_NB)
}

// Blocking way to acquire a shared lock.
func (f *FileLock) RLock() error {
	return f.performLock(syscall.LOCK_SH)
}

// Non blocking way to try to acquire an exclusive lock.
func (f *FileLock) TryLock() error {
	return f.performLock(syscall.LOCK_EX | syscall.LOCK_NB)
}

// Blocking way to acquire an exclusive lock.
func (f *FileLock) Lock() error {
	return f.performLock(syscall.LOCK_EX)
}

func (f *FileLock) performUnlock(flockHow int) (performErr error) {
	if f.fileH == nil {
		return errors.Newf("FileLock: never acquired!")
	} else if f.flockType&flockHow == 0 {
		return errors.Newf("FileLock: wrong lock type!")
	}

	err := f.fileH.Close()
	f.flockType = 0
	f.fileH = nil
	return err
}

// Unlocks the shared lock. If the Lock was never acquired before or if the
// Lock was acquired but was not shared, it will return an error.
func (f *FileLock) RUnlock() error {
	return f.performUnlock(syscall.LOCK_SH)
}

// Unlocks the exclusive lock. If the Lock was never acquired before or if the
// Lock was acquired but was not exclusive, it will return an error.
func (f *FileLock) Unlock() error {
	return f.performUnlock(syscall.LOCK_EX)
}

// Returns whether the error returned by TryLock/TryRLock is the result of
// the lock already being held by another process.
func IsHeldElsewhere(err error) bool {
	if errno, ok := err.(syscall.Errno); ok {
		return errno == syscall.EWOULDBLOCK
	}
	return false
}
