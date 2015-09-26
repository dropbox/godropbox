package filelock

import (
	"github.com/dropbox/godropbox/errors"
	"os"
	"path"
	"syscall"
)

// FileLock object can be used to create locks that can be used to synchronise
// different processes running on the same machine.
// The current implementation of FileLock will always release all acquired locks
// once the process itself exits.
// NOTE: FileLock is not thread safe, since it is not meant to be used from multiple
// threads but from different processes instead!
type FileLock struct {
	name      string
	path      string
	prefix    string
	flockType int

	fileH *os.File
}

// Creates new FileLock object given a unique name.
func New(name string) *FileLock {
	return &FileLock{
		name:      name,
		path:      "/tmp",
		prefix:    "flock-",
		flockType: syscall.LOCK_EX,
	}
}

func (f *FileLock) performLock(flockHow int) (performErr error) {
	if f.fileH != nil {
		return errors.Newf("FileLock is already acquired!")
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

	f.fileH = fileH
	return nil
}

// Non blocking way to try to acquire the lock.
func (f *FileLock) TryLock() error {
	return f.performLock(f.flockType | syscall.LOCK_NB)
}

// Blocking way to acquire the lock.
func (f *FileLock) Lock() error {
	return f.performLock(f.flockType)
}

// Unlocks the lock. If the Lock was never acquired before it will
// cause a panic.
func (f *FileLock) Unlock() error {
	err := f.fileH.Close()
	f.fileH = nil
	return err
}
