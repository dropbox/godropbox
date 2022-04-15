// +build linux darwin

package flock

import (
	"os"
	"syscall"
)

type Flock struct {
	file *os.File
}

// Create a lock file if necessary. This should be closed to release the file handle.
func Open(path string) (*Flock, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	return &Flock{file}, nil
}

// Return true if the lock was acquired without error.
func (fl *Flock) TryLock() (bool, error) {
	err := syscall.Flock(int(fl.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == syscall.EWOULDBLOCK {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (fl *Flock) Lock() error {
	// There is a limit to how many consecutive interrupts make sense.
	var err error
	for i := 0; i < 4; i++ {
		err = syscall.Flock(int(fl.file.Fd()), syscall.LOCK_EX)
		if err != syscall.EINTR {
			return err
		}
	}
	return err
}

func (fl *Flock) Unlock() error {
	return syscall.Flock(int(fl.file.Fd()), syscall.LOCK_UN)
}

func (fl *Flock) Close() error {
	return fl.file.Close()
}
