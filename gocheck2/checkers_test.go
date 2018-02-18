package gocheck2

import (
	stdlibErrors "errors"
	"fmt"
	"testing"

	. "gopkg.in/check.v1"

	"github.com/dropbox/godropbox/errors"
)

// Hook up gocheck into go test runner
func Test(t *testing.T) {
	TestingT(t)
}

type CheckersSuite struct{}

var _ = Suite(&CheckersSuite{})

func (s *CheckersSuite) SetUpTest(c *C) {
}

func test(c *C, ch Checker,
	expectedResult bool, expectedErr string, params ...interface{}) {

	actualResult, actualErr := ch.Check(params, nil)
	if actualResult != expectedResult || actualErr != expectedErr {
		c.Fatalf(
			"Check returned (%#v, %#v) rather than (%#v, %#v)",
			actualResult, actualErr, expectedResult, expectedErr)
	}
}

func (s *CheckersSuite) TestHasKey(c *C) {
	test(c, HasKey, true, "", map[string]int{"foo": 1}, "foo")
	test(c, HasKey, false, "", map[string]int{"foo": 1}, "bar")
	test(c, HasKey, true, "", map[int][]byte{10: nil}, 10)

	// If lesser than 2 params Check returns false
	test(c, HasKey, false, "HasKey takes 2 arguments: a map and a key",
		map[string]int{"foo": 1})

	test(c, HasKey, false, "First argument to HasKey must be a map", nil, "bar")
	test(c, HasKey,
		false, "Second argument must be assignable to the map key type",
		map[string]int{"foo": 1}, 10)
}

func (s *CheckersSuite) TestAssertHasKey(c *C) {
	c.Assert(map[string]int{"foo": 1}, HasKey, "foo")
}

func (s *CheckersSuite) TestNoErr(c *C) {
	// Test the true/false behavior.
	test(c, NoErr, true, "", nil)
	test(c, NoErr, true, "", 3)
	test(c, NoErr, true, "", error(nil))
	test(c, NoErr, false, "", stdlibErrors.New("message"))
	test(c, NoErr, false, "", errors.New("message"))

	// Test the message behavior.
	params := []interface{}{errors.New("1\n2\n3")}
	text := params[0].(error).Error()
	NoErr.Check(params, nil)
	c.Assert(fmt.Sprintf("%#v", params[0]), Equals, "\n"+text)
}

func (s *CheckersSuite) TestAssertNoErr(c *C) {
	c.Assert(error(nil), NoErr)
}

func (s *CheckersSuite) TestErrorMatches(c *C) {
	test(c, MultilineErrorMatches, true, "",
		errors.Newf("Oh damn, this stinks"), "stinks")
	test(c, MultilineErrorMatches, false, "",
		errors.Newf("Oh damn, this stinks"), "skinks")
	test(c, MultilineErrorMatches, false, "MultilineErrorMatches take 2 arguments: an error, "+
		"and a regular expression")
	test(c, MultilineErrorMatches, false, "the first parameter value must be an error!",
		"foo", "bar")
	test(c, MultilineErrorMatches, false, "the second parameter value must be a string containing a regular expression",
		errors.Newf("Oh damn, this stinks"), nil)
	test(c, MultilineErrorMatches, false, "Error in regular expression: error parsing regexp: invalid nested repetition operator: `++`",
		errors.Newf("Foo Error"), "[a-z]++")
}

func (s *CheckersSuite) TestAssertMultilineErrorMatch(c *C) {
	c.Assert(errors.Newf("Error received : foo"), MultilineErrorMatches, "foo")
}
func (s *CheckersSuite) TestAlmostEqual(c *C) {
	// Test margins.
	test(c, AlmostEqual, true, "", 5.0, 5.0, 0.0)
	test(c, AlmostEqual, true, "", 5.0, 5.0, 0.1)
	test(c, AlmostEqual, true, "", 5.0, 4.995, 0.01)
	test(c, AlmostEqual, false, "Obtained 5.000000 different from expected 4.995000 by more than 0.001000 margin", 5.0, 4.995, 0.001)

	// Test invalid args.
	test(c, AlmostEqual, false, "AlmostEqual takes exactly 3 arguments", 5.0, 4.99)
	test(c, AlmostEqual, false, "All arguments to AlmostEqual must be float64", "5.0", 5.0, 0.1)
	test(c, AlmostEqual, false, "Margin must be non-negative", 5.0, 5.0, -0.1)
}

func (s *CheckersSuite) TestAssertAlmostEqual(c *C) {
	c.Assert(4.9975, AlmostEqual, 5.0, 0.01)
}

func (s *CheckersSuite) TestGreaterThan(c *C) {
	test(c, GreaterThan, true, "", 6, 5)
	test(c, GreaterThan, false, "obtained is not int", 6.0, 5)
	test(c, GreaterThan, false, "expected is not int", 6, 5.0)
	test(c, GreaterThan, false, "5 is less than or equal to 6", 5, 6)
}

func (s *CheckersSuite) TestAssertGreaterThan(c *C) {
	c.Assert(6, GreaterThan, 5)
}

func (s *CheckersSuite) TestIsBoolValueChecker(c *C) {
	test(c, IsTrue, true, "", true)
	test(c, IsFalse, true, "", false)

	test(c, IsTrue, false, "Argument to IsTrue must be bool", "true")

}

func (s *CheckersSuite) TestBytesEquals(c *C) {
	b1 := []byte("foo")
	b2 := []byte("foo")
	test(c, BytesEquals, true, "", b1, b2)

	test(c, BytesEquals, false, "Arguments to BytesEqual must both be bytestrings", "foo", b2)
	test(c, BytesEquals, false, "BytesEqual takes 2 bytestring arguments", b1)
	test(c, BytesEquals, false, "Byte arrays were different", b1, []byte("foo2"))

}

func (s *CheckersSuite) TestAssertBytesEquals(c *C) {
	c.Assert([]byte("baz"), BytesEquals, []byte("baz"))
}
