// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Copyright 2016, Dropbox Inc. All rights reserved.
// This is a modified version of https://github.com/youtube/vitess/go/ioutil2

// Package ioutil2 provides extra functionality along similar lines to io/ioutil.
package ioutil2

import (
	"io/ioutil"
	"os"
	"path"
)

// Write file to temp and atomically move when everything else succeeds.
func WriteFileAtomic(filename string, data []byte, perm os.FileMode) error {
	dir, name := path.Split(filename)
	fDir, dirErr := os.Open(dir)
	if dirErr != nil {
		return dirErr
	}
	f, err := ioutil.TempFile(dir, name)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
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
	err = fDir.Sync()
	if closeErr := fDir.Close(); err == nil {
		err = closeErr
	}
	return err
}
