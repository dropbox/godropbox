// Extensions to the go-check unittest framework.
//
// NOTE: see https://github.com/go-check/check/pull/6 for reasons why these
// checkers live here.
package gocheck2

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"

	. "gopkg.in/check.v1"
)

// -----------------------------------------------------------------------
// IsTrue / IsFalse checker.

type isBoolValueChecker struct {
	*CheckerInfo
	expected bool
}

func (checker *isBoolValueChecker) Check(
	params []interface{},
	names []string) (
	result bool,
	error string) {

	obtained, ok := params[0].(bool)
	if !ok {
		return false, "Argument to " + checker.Name + " must be bool"
	}

	return obtained == checker.expected, ""
}

// The IsTrue checker verifies that the obtained value is true.
//
// For example:
//
//     c.Assert(value, IsTrue)
//
var IsTrue Checker = &isBoolValueChecker{
	&CheckerInfo{Name: "IsTrue", Params: []string{"obtained"}},
	true,
}

// The IsFalse checker verifies that the obtained value is false.
//
// For example:
//
//     c.Assert(value, IsFalse)
//
var IsFalse Checker = &isBoolValueChecker{
	&CheckerInfo{Name: "IsFalse", Params: []string{"obtained"}},
	false,
}

// -----------------------------------------------------------------------
// BytesEquals checker.

type bytesEquals struct{}

func (b *bytesEquals) Check(params []interface{}, names []string) (bool, string) {
	if len(params) != 2 {
		return false, "BytesEqual takes 2 bytestring arguments"
	}
	b1, ok1 := params[0].([]byte)
	b2, ok2 := params[1].([]byte)

	if !(ok1 && ok2) {
		return false, "Arguments to BytesEqual must both be bytestrings"
	}

	if bytes.Equal(b1, b2) {
		return true, ""
	}
	return false, "Byte arrays were different"
}

func (b *bytesEquals) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   "BytesEquals",
		Params: []string{"bytes_one", "bytes_two"},
	}
}

// BytesEquals checker compares two bytes sequence using bytes.Equal.
//
// For example:
//
//     c.Assert(b, BytesEquals, []byte("bar"))
//
// Main difference between DeepEquals and BytesEquals is that BytesEquals treats
// `nil` as empty byte sequence while DeepEquals doesn't.
//
//     c.Assert(nil, BytesEquals, []byte("")) // succeeds
//     c.Assert(nil, DeepEquals, []byte("")) // fails
var BytesEquals = &bytesEquals{}

// -----------------------------------------------------------------------
// AlmostEqual checker.
// Meant to compare floats with some margin of error which might arrise
// from rounding errors.

type almostEqualChecker struct{}

func (ae *almostEqualChecker) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   "AlmostEqual",
		Params: []string{"obtained", "expected", "margin"},
	}
}

func (ae *almostEqualChecker) Check(params []interface{}, names []string) (bool, string) {
	if len(params) != 3 {
		return false, "AlmostEqual takes exactly 3 arguments"
	}
	obtained, ok1 := params[0].(float64)
	expected, ok2 := params[1].(float64)
	margin, ok3 := params[2].(float64)

	if !(ok1 && ok2 && ok3) {
		return false, "All arguments to AlmostEqual must be float64"
	}

	if margin < 0 {
		return false, "Margin must be non-negative"
	}

	if obtained < (expected-margin) || obtained > (expected+margin) {
		return false, fmt.Sprintf("Obtained %f different from expected %f by more than %f margin",
			obtained, expected, margin)
	}
	return true, ""
}

var AlmostEqual = &almostEqualChecker{}

// -----------------------------------------------------------------------
// HasKey checker.

type hasKey struct{}

func (h *hasKey) Check(params []interface{}, names []string) (bool, string) {
	if len(params) != 2 {
		return false, "HasKey takes 2 arguments: a map and a key"
	}

	mapValue := reflect.ValueOf(params[0])
	if mapValue.Kind() != reflect.Map {
		return false, "First argument to HasKey must be a map"
	}

	keyValue := reflect.ValueOf(params[1])
	if !keyValue.Type().AssignableTo(mapValue.Type().Key()) {
		return false, "Second argument must be assignable to the map key type"
	}

	return mapValue.MapIndex(keyValue).IsValid(), ""
}

func (h *hasKey) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   "HasKey",
		Params: []string{"obtained", "key"},
	}
}

// The HasKey checker verifies that the obtained map contains the given key.
//
// For example:
//
//     c.Assert(myMap, HasKey, "foo")
//
var HasKey = &hasKey{}

// -----------------------------------------------------------------------
// NoErr checker.

type noErr struct{}

// This exists to implement fmt.GoStringer and force the `%#v` format to show
// the string unescaped, newlines and all.
type rawString string

func (r rawString) GoString() string {
	return string(r)
}

func (c noErr) Check(params []interface{}, names []string) (bool, string) {
	err, ok := params[0].(error)
	if !ok {
		return true, ""
	}
	params[0] = rawString("\n" + err.Error())
	return false, ""
}

func (c noErr) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   "NoErr",
		Params: []string{"error"},
	}
}

// The NoErr checker tests that the obtained value is nil.  On failure,
// the checker adjusts the output so that a multi-line error message will
// be printed in a readable fashion.
//
// For example:
//
//     c.Assert(err, NoErr)
var NoErr = &noErr{}

// -----------------------------------------------------------------------
// MultilineErrorMatches: Multiline ErrorMatches
// The standard gocheck ErrorMatches brackets the regular expression that
// the error must match in ^ and $, so that it can only match single-line
// errors messages. Most dropbox errors are created using godropbox.errors,
// which produce error objects that have multiline message, which means
// that our standard errors will never be matched by ErrorMatches.
//
// This is a variant of the normal ErrorMatches which avoids that problem,
// and works with dropbox errors.
// It takes two parameters:
// 1: An error object, and
// 2: a string containing a regular expression.
// The check succeeds if the error's message contains a match for the regular expression
// at any position.
type multilineErrorMatches struct{}

func (e multilineErrorMatches) Check(params []interface{}, names []string) (bool, string) {
	if len(params) != 2 {
		return false, "MultilineErrorMatches take 2 arguments: an error, and a regular expression"
	}
	errValue, errIsError := params[0].(error)
	if !errIsError {
		return false, "the first parameter value must be an error!"
	}
	regexpStr, reIsStr := params[1].(string)
	if !reIsStr {
		return false, "the second parameter value must be a string containing a regular expression"
	}
	matches, err := regexp.MatchString(regexpStr, errValue.Error())
	if err != nil {
		return false, fmt.Sprintf("Error in regular expression: %v", err.Error())
	}
	return matches, ""
}

func (h multilineErrorMatches) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   "MultilineErrorMatches",
		Params: []string{"error", "pattern"},
	}
}

var MultilineErrorMatches multilineErrorMatches = multilineErrorMatches{}

type greaterThan struct{}

func (c greaterThan) Check(params []interface{}, names []string) (bool, string) {
	obtained, ok := params[0].(int)
	if !ok {
		return false, "obtained is not int"
	}
	expected, ok := params[1].(int)
	if !ok {
		return false, "expected is not int"
	}

	if obtained > expected {
		return true, ""
	}
	return false, fmt.Sprintf("%d is less than or equal to %d", obtained, expected)
}
func (c greaterThan) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   "GreaterThan",
		Params: []string{"obtained", "expected"},
	}
}

var GreaterThan = &greaterThan{}
