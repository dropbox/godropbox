// This module implements functions which manipulate errors and provide stack
// trace information.
//
// NOTE: This package intentionally mirrors the standard "errors" module.
// All dropbox code should use this.
package errors

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"sync"
)

// This interface exposes additional information about the error.
type DropboxError interface {
	// This returns the error message without the stack trace.
	GetMessage() string

	// This returns the wrapped error.  This returns nil if this does not wrap
	// another error.
	GetInner() error

	// Implements the built-in error interface.
	Error() string

	// Returns stack addresses as a string that can be supplied to
	// a helper tool to get the actual stack trace. This function doesn't result
	// in resolving full stack frames thus is a lot more efficient.
	StackAddrs() string

	// Returns stack frames.
	StackFrames() []StackFrame

	// Returns string representation of stack frames.
	// Stack frame formatting looks generally something like this:
	// dropbox/rpc.(*clientV4).Do
	//   /srv/server/go/src/dropbox/rpc/client.go:87 +0xbf9
	// dropbox/exclog.Report
	//   /srv/server/go/src/dropbox/exclog/client.go:129 +0x9e5
	// main.main
	//   /home/cdo/tmp/report_exception.go:13 +0x84
	// It is discouraged to parse stack frames using string parsing since it can change at any time.
	// Use StackFrames() function instead to get actual stack frame metadata.
	GetStack() string
}

// Represents a single stack frame.
type StackFrame struct {
	PC         uintptr
	Func       *runtime.Func
	FuncName   string
	File       string
	LineNumber int
}

// Standard struct for general types of errors.
//
// For an example of custom error type, look at databaseError/newDatabaseError
// in errors_test.go.
type baseError struct {
	msg   string
	inner error

	stack       []uintptr
	framesOnce  sync.Once
	stackFrames []StackFrame
}

// This returns the error string without stack trace information.
func GetMessage(err interface{}) string {
	switch e := err.(type) {
	case DropboxError:
		return extractFullErrorMessage(e, false)
	case runtime.Error:
		return runtime.Error(e).Error()
	case error:
		return e.Error()
	default:
		return "Passed a non-error to GetMessage"
	}
}

// This returns a string with all available error information, including inner
// errors that are wrapped by this errors.
func (e *baseError) Error() string {
	return extractFullErrorMessage(e, true)
}

// Implements DropboxError interface.
func (e *baseError) GetMessage() string {
	return e.msg
}

// Implements DropboxError interface.
func (e *baseError) GetInner() error {
	return e.inner
}

// Implements DropboxError interface.
func (e *baseError) StackAddrs() string {
	buf := bytes.NewBuffer(make([]byte, 0, len(e.stack)*8))
	for _, pc := range e.stack {
		fmt.Fprintf(buf, "0x%x ", pc)
	}
	bufBytes := buf.Bytes()
	return string(bufBytes[:len(bufBytes)-1])
}

// Implements DropboxError interface.
func (e *baseError) StackFrames() []StackFrame {
	e.framesOnce.Do(func() {
		e.stackFrames = make([]StackFrame, len(e.stack))
		for i, pc := range e.stack {
			frame := &e.stackFrames[i]
			frame.PC = pc
			frame.Func = runtime.FuncForPC(pc)
			if frame.Func != nil {
				frame.FuncName = frame.Func.Name()
				frame.File, frame.LineNumber = frame.Func.FileLine(frame.PC - 1)
			}
		}
	})
	return e.stackFrames
}

// Implements DropboxError interface.
func (e *baseError) GetStack() string {
	stackFrames := e.StackFrames()
	buf := bytes.NewBuffer(make([]byte, 0, 256))
	for _, frame := range stackFrames {
		_, _ = buf.WriteString(frame.FuncName)
		_, _ = buf.WriteString("\n")
		fmt.Fprintf(buf, "\t%s:%d +0x%x\n",
			frame.File, frame.LineNumber, frame.PC)
	}
	return buf.String()
}

// This returns a new baseError initialized with the given message and
// the current stack trace.
func New(msg string) DropboxError {
	return new(nil, msg)
}

// Same as New, but with fmt.Printf-style parameters.
func Newf(format string, args ...interface{}) DropboxError {
	return new(nil, fmt.Sprintf(format, args...))
}

// Wraps another error in a new baseError.
func Wrap(err error, msg string) DropboxError {
	return new(err, msg)
}

// Same as Wrap, but with fmt.Printf-style parameters.
func Wrapf(err error, format string, args ...interface{}) DropboxError {
	return new(err, fmt.Sprintf(format, args...))
}

// Internal helper function to create new baseError objects,
// note that if there is more than one level of redirection to call this function,
// stack frame information will include that level too.
func new(err error, msg string) *baseError {
	stack := make([]uintptr, 200)
	stackLength := runtime.Callers(3, stack)
	return &baseError{
		msg:   msg,
		stack: stack[:stackLength],
		inner: err,
	}
}

// Constructs full error message for a given DropboxError by traversing
// all of its inner errors. If includeStack is True it will also include
// stack trace from deepest DropboxError in the chain.
func extractFullErrorMessage(e DropboxError, includeStack bool) string {
	var ok bool
	var lastDbxErr DropboxError
	errMsg := bytes.NewBuffer(make([]byte, 0, 1024))

	dbxErr := e
	for {
		lastDbxErr = dbxErr
		errMsg.WriteString(dbxErr.GetMessage())

		innerErr := dbxErr.GetInner()
		if innerErr == nil {
			break
		}
		dbxErr, ok = innerErr.(DropboxError)
		if !ok {
			// We have reached the end and traveresed all inner errors.
			// Add last message and exit loop.
			errMsg.WriteString(innerErr.Error())
			break
		}
		errMsg.WriteString("\n")
	}
	if includeStack {
		errMsg.WriteString("\nORIGINAL STACK TRACE:\n")
		errMsg.WriteString(lastDbxErr.GetStack())
	}
	return errMsg.String()
}

// Return a wrapped error or nil if there is none.
func unwrapError(ierr error) (nerr error) {
	// Internal errors have a well defined bit of context.
	if dbxErr, ok := ierr.(DropboxError); ok {
		return dbxErr.GetInner()
	}

	// At this point, if anything goes wrong, just return nil.
	defer func() {
		if x := recover(); x != nil {
			nerr = nil
		}
	}()

	// Go system errors have a convention but paradoxically no
	// interface.  All of these panic on error.
	errV := reflect.ValueOf(ierr).Elem()
	errV = errV.FieldByName("Err")
	return errV.Interface().(error)
}

// Keep peeling away layers or context until a primitive error is revealed.
func RootError(ierr error) (nerr error) {
	nerr = ierr
	for i := 0; i < 20; i++ {
		terr := unwrapError(nerr)
		if terr == nil {
			return nerr
		}
		nerr = terr
	}
	return fmt.Errorf("too many iterations: %T", nerr)
}

// Perform a deep check, unwrapping errors as much as possilbe and
// comparing the string version of the error.
func IsError(err, errConst error) bool {
	if err == errConst {
		return true
	}
	// Must rely on string equivalence, otherwise a value is not equal
	// to its pointer value.
	rootErrStr := ""
	rootErr := RootError(err)
	if rootErr != nil {
		rootErrStr = rootErr.Error()
	}
	errConstStr := ""
	if errConst != nil {
		errConstStr = errConst.Error()
	}
	return rootErrStr == errConstStr
}
