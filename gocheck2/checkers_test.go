package gocheck2

import (
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into go test runner
func Test(t *testing.T) {
	TestingT(t)
}

type CheckersSuite struct{}

var _ = Suite(&CheckersSuite{})

func (s *CheckersSuite) SetUpTest(c *C) {
}

func testHasKey(c *C, expectedResult bool, expectedErr string, params ...interface{}) {
	actualResult, actualErr := HasKey.Check(params, nil)
	if actualResult != expectedResult || actualErr != expectedErr {
		c.Fatalf(
			"Check returned (%#v, %#v) rather than (%#v, %#v)",
			actualResult, actualErr, expectedResult, expectedErr)
	}
}

func (s *CheckersSuite) TestHasKey(c *C) {
	testHasKey(c, true, "", map[string]int{"foo": 1}, "foo")
	testHasKey(c, false, "", map[string]int{"foo": 1}, "bar")
	testHasKey(c, true, "", map[int][]byte{10: nil}, 10)

	testHasKey(c, false, "First argument to HasKey must be a map", nil, "bar")
	testHasKey(
		c, false, "Second argument must be assignable to the map key type",
		map[string]int{"foo": 1}, 10)
}
