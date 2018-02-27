package strings

import (
	"bytes"
	. "gopkg.in/check.v1"
	"reflect"
	"testing"
	"unsafe"
)

func Test(t *testing.T) {
	TestingT(t)
}

type InternStringSuite struct {
}

func (i *InternStringSuite) SetUpTest(c *C) {
}

var _ = Suite(&InternStringSuite{})

func (i *InternStringSuite) TestIntern(c *C) {

	s := "foo bar"

	// Creating a string using byte arrays
	var buffer bytes.Buffer
	buffer.WriteString("f")
	buffer.WriteString("o")
	buffer.WriteString("o")
	buffer.WriteString(" ")
	buffer.WriteString("b")
	buffer.WriteString("a")
	buffer.WriteString("r")
	b := buffer.String()

	// Get StringHeaders for the two strings
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := (*reflect.StringHeader)(unsafe.Pointer(&b))

	// Assert that their StringHeader data are unequal
	c.Assert(sh.Data, Not(Equals), bh.Data)
	// The StringHeader Length should be equal
	c.Assert(sh.Len, Equals, bh.Len)
	// The two strings should be equal
	c.Assert(s, Equals, b)

	// Now to intern these two strings into the intern pool
	i1 := Intern(s)
	i2 := Intern(b)

	ih1 := (*reflect.StringHeader)(unsafe.Pointer(&i1))
	ih2 := (*reflect.StringHeader)(unsafe.Pointer(&i2))

	// Assert that interned strings have equal StringHeader Data
	c.Assert(ih1.Data, Equals, ih2.Data)
	// The StringHeader Length should be equal
	c.Assert(ih1.Len, Equals, ih2.Len)
	// The two strings should be equal
	c.Assert(i1, Equals, i2)

	// Intern byte array of characters
	ba := buffer.Bytes()
	i3 := InternBytes(ba)
	ih3 := (*reflect.StringHeader)(unsafe.Pointer(&i3))

	// Assert that interned strings have equal StringHeader Data
	c.Assert(ih3.Data, Equals, ih2.Data)
	// The StringHeader Length should be equal
	c.Assert(ih3.Len, Equals, ih2.Len)
	// The two strings should be equal
	c.Assert(i3, Equals, i2)
}
