// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Copyright 2016, Dropbox Inc. All rights reserved.
// This is a modified version of https://github.com/youtube/vitess/go/ioutil2

// Package ioutil2 provides extra functionality along similar lines to io/ioutil.
package ioutil2

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"

	"godropbox/errors"
)

// Write file to temp and atomically move when everything else succeeds.
func WriteFileAtomic(filename string, data []byte, perm os.FileMode) (err error) {
	return WriteFileAtomicReader(filename, bytes.NewReader(data), perm)
}

// Write file to temp and atomically move when everything else succeeds.
func WriteFileAtomicReader(filename string, reader io.Reader, perm os.FileMode) (err error) {
	dir := filepath.Dir(filename)
	name := filepath.Base(filename)

	fDir, dirErr := os.Open(dir)
	if dirErr != nil {
		return dirErr
	}
	defer func() {
		if closeErr := fDir.Close(); err == nil {
			err = closeErr
		}
	}()

	f, err := ioutil.TempFile(dir, name)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, reader)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if permErr := os.Chmod(f.Name(), perm); err == nil {
		err = permErr
	}
	// Any err should result in full cleanup.
	if err != nil {
		_ = os.Remove(f.Name())
		return err
	}
	if err = os.Rename(f.Name(), filename); err != nil {
		return err
	}
	return fDir.Sync()
}

// CreateFileIfNotExists creates the specified file if it does not
// already exist.
func CreateFileIfNotExists(filename string, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, perm)
	if err != nil {
		return err
	}
	return f.Close()
}

func UnlinkedTempFile(dir, pattern string) (f *os.File, err error) {
	file, err := ioutil.TempFile(dir, pattern)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create a temporary file")
	}
	err = syscall.Unlink(file.Name())
	if err != nil {
		file.Close()
		return nil, errors.Wrapf(err, "failed to unlink %s so that it goes away even if process dies", file.Name())
	}
	return file, nil
}
