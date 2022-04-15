// Extensions to the go-check unittest framework.
//
// NOTE: see https://github.com/go-check/check/pull/6 for reasons why these
// checkers live here.
package gocheck2

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"regexp"

	"github.com/davecgh/go-spew/spew"
	"github.com/pmezard/go-difflib/difflib"
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
// Meant to compare floats with some margin of error which might arise
// from rounding errors.

type almostEqualChecker struct{}

func (ae *almostEqualChecker) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   "AlmostEqual",
		Params: []string{"obtained", "expected", "margin"},
	}
}

func (ae *almostEqualChecker) convertToFloat64(d interface{}) (float64, bool) {
	f64, ok := d.(float64)
	if ok {
		return f64, ok
	}
	f32, ok := d.(float32)
	if ok {
		return float64(f32), ok
	}
	return 0.0, false
}

func (ae *almostEqualChecker) Check(params []interface{}, names []string) (bool, string) {
	if len(params) != 3 {
		return false, "AlmostEqual takes exactly 3 arguments"
	}
	obtained, ok1 := ae.convertToFloat64(params[0])
	expected, ok2 := ae.convertToFloat64(params[1])
	margin, ok3 := ae.convertToFloat64(params[2])

	if !(ok1 && ok2 && ok3) {
		return false, "All arguments to AlmostEqual must be float64 or float32"
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

// -----------------------------------------------------------------------
// MultilineMatches: Multiline Matches
// The standard gocheck Matches brackets the regular expression that
// the string must match in ^ and $, so that it can only match single-line
// messages.
//
// This is a variant of the normal Matches which avoids that problem.
// It takes two parameters:
// 1: A string, and
// 2: a string containing a regular expression.
// The check succeeds if the first string contains a match for the regular expression
// at any position.
type multilineMatches struct{}

func (e multilineMatches) Check(params []interface{}, names []string) (bool, string) {
	if len(params) != 2 {
		return false, "MultilineMatches take 2 arguments: a string, and a regular expression"
	}
	strValue, errIsString := params[0].(string)
	if !errIsString {
		return false, "the first parameter value must be a string"
	}
	regexpStr, reIsStr := params[1].(string)
	if !reIsStr {
		return false, "the second parameter value must be a string containing a regular expression"
	}
	matches, err := regexp.MatchString(regexpStr, strValue)
	if err != nil {
		return false, fmt.Sprintf("Error in regular expression: %v", err.Error())
	}
	return matches, ""
}

func (h multilineMatches) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   "MultilineMatches",
		Params: []string{"string", "pattern"},
	}
}

var MultilineMatches multilineMatches = multilineMatches{}

type intCmp struct {
	name string
	op   string
	cmp  func(a, b int64) bool
}

func (c intCmp) Check(params []interface{}, names []string) (bool, string) {
	asInt64 := func(a interface{}) (int64, bool) {
		// :|
		v := reflect.ValueOf(a)
		int64Type := reflect.TypeOf(int64(0))
		if v.Type().ConvertibleTo(int64Type) {
			return v.Convert(int64Type).Int(), true
		}
		return 0, false
	}
	obtained, ok := asInt64(params[0])
	if !ok {
		return false, "obtained wasn't an integer type"
	}
	expected, ok := asInt64(params[1])
	if !ok {
		return false, "expected wasn't an integer type"
	}

	if c.cmp(obtained, expected) {
		return true, ""
	}
	return false, fmt.Sprintf("%d%s%d isn't true", obtained, c.op, expected)
}
func (c intCmp) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   c.name,
		Params: []string{"obtained", "expected"},
	}
}

var LessThan = &intCmp{"LessThan", "<", func(a, b int64) bool { return a < b }}
var LessOrEquals = &intCmp{"LessOrEquals", "<=", func(a, b int64) bool { return a <= b }}
var GreaterThan = &intCmp{"GreaterThan", ">", func(a, b int64) bool { return a > b }}
var GreaterOrEquals = &intCmp{"GreaterOrEquals", ">=", func(a, b int64) bool { return a >= b }}

type deepEqualsPretty struct{}

func (c deepEqualsPretty) Check(params []interface{}, names []string) (bool, string) {
	result := reflect.DeepEqual(params[0], params[1])
	if result {
		return true, ""
	}

	spewCfg := spew.NewDefaultConfig()
	spewCfg.ContinueOnMethod = true
	spewCfg.Indent = "  "
	spewCfg.DisablePointerAddresses = true
	spewCfg.SortKeys = true
	spewCfg.SpewKeys = true
	spewCfg.DisableCapacities = true
	diffStr, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A: difflib.SplitLines(spewCfg.Sdump(params[0])),
		B: difflib.SplitLines(spewCfg.Sdump(params[1])),
		// We want to just render the entire thing until somebody uses this with
		// an untenably large structure.
		Context: math.MaxInt32,
	})
	if err != nil {
		return false, err.Error()
	}

	return false, diffStr
}
func (c deepEqualsPretty) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   "DeepEqualsPretty",
		Params: []string{"obtained", "expected"},
	}
}

// Standard gocheck DeepEquals just prints the values using %#v when it fails,
// which does traverse pointers during comparison but doesn't for printing, so
// if the difference is through a pointer the debug message is completely
// useless. Plus they're printed all on one line and very hard to read.
//
// DeepEqualsPretty prints the entire structure of both args, with newlines and
// indendation, and diffs them, so it's easy to pick out what's different.
var DeepEqualsPretty = &deepEqualsPretty{}
