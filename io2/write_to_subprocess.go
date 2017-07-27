package io2

import (
	"io"
	"os/exec"
)

type WriteSubprocess struct {
	downstream io.Writer
	cmd        *exec.Cmd
	pipe       io.WriteCloser
}

type closeWrapper struct {
	writer io.Writer
}

func (cwself closeWrapper) Write(data []byte) (int, error) {
	return cwself.writer.Write(data)
}

func (cwself closeWrapper) Close() error {
	return nil
}

func NewWriteSubprocess(
	cmd *exec.Cmd,
	downstream io.Writer) (WriteSubprocess, error) {

	cmd.Stdout = downstream
	ret := WriteSubprocess{cmd: cmd, downstream: downstream}
	var err error
	ret.pipe, err = cmd.StdinPipe()
	if err == nil {
		err = ret.cmd.Start()
	}
	return ret, err
}

func (writeSubprocess *WriteSubprocess) Write(data []byte) (int, error) {
	return writeSubprocess.pipe.Write(data)
}

func (writeSubprocess *WriteSubprocess) Close() error {
	err := writeSubprocess.pipe.Close()
	procErr := writeSubprocess.cmd.Wait()
	var closeErr error
	if writeCloser, ok := writeSubprocess.downstream.(io.WriteCloser); ok {
		closeErr = writeCloser.Close()
	}
	if err != nil {
		return err
	}
	if procErr != nil {
		return procErr
	}
	if closeErr != nil {
		return closeErr
	}
	return nil
}
