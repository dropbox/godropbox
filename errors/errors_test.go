package errors

import (
	"fmt"
	"strings"
	"testing"
)

func TestStackTrace(t *testing.T) {
	const testMsg = "test error"
	er := New(testMsg)
	e := er.(*DropboxBaseError)

	if e.Msg != testMsg {
		t.Error("error message %s != expected %s", e.Msg, testMsg)
	}

	if strings.Index(e.Stack, "dropbox/util/errors/errors.go") != -1 {
		t.Error("stack trace generation code should not be in the error stack trace")
	}

	if strings.Index(e.Stack, "TestStackTrace") == -1 {
		t.Error("stack trace must have test code in it")
	}

	// compile-time test to ensure that DropboxError conforms to error interface
	var err error = e
	_ = err
}

func TestWrappedError(t *testing.T) {
	const (
		innerMsg  = "I am inner error"
		middleMsg = "I am the middle error"
		outerMsg  = "I am the mighty outer error"
	)
	inner := fmt.Errorf(innerMsg)
	middle := Wrap(inner, middleMsg)
	outer := Wrap(middle, outerMsg)
	errorStr := outer.Error()

	if strings.Index(errorStr, innerMsg) == -1 {
		t.Errorf("couldn't find inner error message in:\n%s", errorStr)
	}

	if strings.Index(errorStr, middleMsg) == -1 {
		t.Errorf("couldn't find middle error message in:\n%s", errorStr)
	}

	if strings.Index(errorStr, outerMsg) == -1 {
		t.Errorf("couldn't find outer error message in:\n%s", errorStr)
	}
}

// ---------------------------------------
// minimal example + test for custom error
//
type databaseError struct {
	Msg     string
	Code    int
	Stack   string
	Context string
}

// "constructor" for creating error (needs to store return value of StackTrace() to get the
// )
func newDatabaseError(msg string, code int) databaseError {
	stack, context := StackTrace()
	return databaseError{msg, code, stack, context}
}

// needed to satisfy "error" interface
func (e databaseError) Error() string {
	return DefaultError(e)
}

// for the DropboxError interface
func (e databaseError) GetMessage() string { return e.Msg }
func (e databaseError) GetStack() string   { return e.Stack }
func (e databaseError) GetContext() string { return e.Context }
func (e databaseError) GetInner() error    { return nil }

// ---------------------------------------

func TestCustomError(t *testing.T) {
	dbMsg := "database error 1205 (lock wait time exceeded)"
	outerMsg := "outer msg"

	dbError := newDatabaseError(dbMsg, 1205)
	outerError := Wrap(dbError, outerMsg)

	errorStr := outerError.Error()
	if strings.Index(errorStr, dbMsg) == -1 {
		t.Errorf("couldn't find database error message in:\n%s", errorStr)
	}

	if strings.Index(errorStr, outerMsg) == -1 {
		t.Errorf("couldn't find outer error message in:\n%s", errorStr)
	}

	if strings.Index(errorStr, "errors.TestCustomError") == -1 {
		t.Errorf("couldn't find this function in stack trace:\n%s", errorStr)
	}
}
