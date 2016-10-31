package errors

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"unicode"
)

func TestStackTrace(t *testing.T) {
	const testMsg = "test error"
	er := New(testMsg)

	if er.GetMessage() != testMsg {
		t.Errorf("error message %s != expected %s", er.GetMessage(), testMsg)
	}

	if strings.Index(er.GetStack(), "github.com/dropbox/godropbox/errors/errors.go") != -1 {
		t.Error("stack trace generation code should not be in the error stack trace")
	}

	if strings.Index(er.GetStack(), "TestStackTrace") == -1 {
		t.Error("stack trace must have test code in it")
	}

	for i, r := range er.GetStack() {
		if !(unicode.IsSpace(r) || unicode.IsPrint(r)) {
			t.Errorf("stack trace has an unexpected rune at index %v (%q)", i, r)
			break
		}
	}
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

func TestStackAddrs(t *testing.T) {
	pat := regexp.MustCompile("^0x[a-h0-9]+( 0x[a-h0-9]+)*$")
	er := New("big trouble")
	if !pat.MatchString(er.StackAddrs()) {
		t.Errorf("StackAddrs didn't match `%s`: %q", pat, er.StackAddrs())
	}
}

// ---------------------------------------
// minimal example + test for custom error
//
type databaseError struct {
	DropboxError
	code int
}

// "constructor" for creating error (needs to store return value of StackTrace() to get the
// )
func newDatabaseError(msg string, code int) databaseError {
	return databaseError{DropboxError: New(msg), code: code}
}

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

type customErr struct {
}

func (ce *customErr) Error() string { return "testing error" }

type customNestedErr struct {
	Err interface{}
}

func (cne *customNestedErr) Error() string { return "nested testing error" }

func TestRootError(t *testing.T) {
	err := RootError(nil)
	if err != nil {
		t.Fatalf("expected nil error")
	}
	var ce *customErr
	err = RootError(ce)
	if err != ce {
		t.Fatalf("expected err on invalid nil-ptr custom error %T %v", err, err)
	}
	ce = &customErr{}
	err = RootError(ce)
	if err != ce {
		t.Fatalf("expected err on valid custom error")
	}

	cne := &customNestedErr{}
	err = RootError(cne)
	if err != cne {
		t.Fatalf("expected err on empty custom error: %T %v", err, err)
	}

	cne = &customNestedErr{reflect.ValueOf(ce).Pointer()}
	err = RootError(cne)
	if err != cne {
		t.Fatalf("expected err on invalid nested uniptr: %T %v", err, err)
	}

	cne = &customNestedErr{ce}
	err = RootError(cne)
	if err != ce {
		t.Fatalf("expected ce on valid nested error: %T %v", err, err)
	}

	cne = &customNestedErr{ce}
	err = RootError(syscall.ECONNREFUSED)
	if err != syscall.ECONNREFUSED {
		t.Fatalf("expected ECONNREFUSED on valid nested error: %T %v", err, err)
	}
}

// Benchmarks creation of new errors.
// Current expected range is ~0.1-0.2ms to create errors from 100 go routines
// simultaneously. This is fairly close to just spinning up go routines
// and putting stuff on channels and doing some very simple work, thus
// error creation should be cheap enough for all most all use cases.
func BenchmarkNew(b *testing.B) {
	a := func() error {
		b := func() error {
			c := func() error {
				return New("Hello world, grab me a stack trace!")
			}
			return c()
		}
		return b()
	}
	nRoutines := 100
	errChan := make(chan error, nRoutines)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k := 0; k < nRoutines; k++ {
			go func() {
				err := a()
				errChan <- err
			}()
		}
		for k := 0; k < nRoutines; k++ {
			<-errChan
		}
	}
}
