package io2

import (
	"io"
	"os/exec"
)

type ReadSubprocess struct {
	upstream io.Reader
	cmd      *exec.Cmd
	pipe     io.ReadCloser
}

func NewReadSubprocess(
	cmd *exec.Cmd,
	upstream io.Reader) (ReadSubprocess, error) {

	cmd.Stdin = upstream
	stdout, err := cmd.StdoutPipe()
	ret := ReadSubprocess{cmd: cmd, upstream: upstream, pipe: stdout}
	if err != nil {
		return ret, err
	}
	err = ret.cmd.Start()
	return ret, err
}

func (readSubprocess *ReadSubprocess) Read(data []byte) (int, error) {
	return readSubprocess.pipe.Read(data)
}

func (readSubprocess *ReadSubprocess) Close() error {
	var err error
	if upstreamCloser, ok := readSubprocess.upstream.(io.ReadCloser); ok {
		err = upstreamCloser.Close()
	}
	subErr := readSubprocess.pipe.Close()
	procErr := readSubprocess.cmd.Wait()
	if err == nil && subErr == nil {
		return procErr
	}
	if subErr != nil {
		return subErr
	}
	return err
}
