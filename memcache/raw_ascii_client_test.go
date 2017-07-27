package memcache

import (
	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
)

type RawAsciiClientSuite struct {
	rw     *mockReadWriter
	client *RawAsciiClient
}

var _ = Suite(&RawAsciiClientSuite{})

func (s *RawAsciiClientSuite) SetUpTest(c *C) {
	s.rw = newMockReadWriter()
	s.client = NewRawAsciiClient(0, s.rw).(*RawAsciiClient)
}

func (s *RawAsciiClientSuite) TestGet(c *C) {
	s.rw.recvBuf.WriteString("VALUE key 333 4 12345\r\nitem\r\n")
	s.rw.recvBuf.WriteString("VALUE key2 42 6 14\r\nAB\r\nCD\r\n")
	s.rw.recvBuf.WriteString("END\r\n")

	responses := s.client.GetMulti([]string{"key2", "key"})

	c.Assert(s.rw.sendBuf.String(), Equals, "gets key2 key\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(len(responses), Equals, 2)

	resp, ok := responses["key"]
	c.Assert(ok, IsTrue)
	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusNoError)
	c.Assert(resp.Key(), Equals, "key")
	c.Assert(resp.Value(), DeepEquals, []byte("item"))
	c.Assert(resp.Flags(), Equals, uint32(333))
	c.Assert(resp.DataVersionId(), Equals, uint64(12345))

	resp, ok = responses["key2"]
	c.Assert(ok, IsTrue)
	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusNoError)
	c.Assert(resp.Key(), Equals, "key2")
	c.Assert(resp.Value(), DeepEquals, []byte("AB\r\nCD"))
	c.Assert(resp.Flags(), Equals, uint32(42))
	c.Assert(resp.DataVersionId(), Equals, uint64(14))
}

func (s *RawAsciiClientSuite) TestGetNotFound(c *C) {
	s.rw.recvBuf.WriteString("END\r\n")

	resp := s.client.Get("key")

	c.Assert(s.rw.sendBuf.String(), Equals, "gets key\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusKeyNotFound)
	c.Assert(resp.Key(), Equals, "key")
	c.Assert(resp.Value(), IsNil)
	c.Assert(resp.Flags(), Equals, uint32(0))
	c.Assert(resp.DataVersionId(), Equals, uint64(0))
}

func (s *RawAsciiClientSuite) TestGetDupKeys(c *C) {
	s.rw.recvBuf.WriteString("END\r\n")

	_ = s.client.GetMulti([]string{"key", "key", "key2", "key"})

	c.Assert(s.rw.sendBuf.String(), Equals, "gets key key2\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)
}

func (s *RawAsciiClientSuite) TestGetBadKey(c *C) {
	resp := s.client.Get("b a d")

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestGetErrorMidStream(c *C) {
	s.rw.recvBuf.WriteString("VALUE key 333 100 12345\r\nunexpected eof ...")

	responses := s.client.GetMulti([]string{"key2", "key"})

	c.Assert(s.rw.sendBuf.String(), Equals, "gets key2 key\r\n")
	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(len(responses), Equals, 2)

	resp, ok := responses["key"]
	c.Assert(ok, IsTrue)
	c.Assert(resp.Error(), NotNil)

	resp, ok = responses["key2"]
	c.Assert(ok, IsTrue)
	c.Assert(resp.Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestGetCheckEmptyBuffers(c *C) {
	s.rw.recvBuf.WriteString("VALUE key 1 4 2\r\nitem\r\n")
	s.rw.recvBuf.WriteString("END\r\nextra stuff")

	resp := s.client.Get("key")

	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusNoError)
	c.Assert(resp.Key(), Equals, "key")
	c.Assert(resp.Value(), DeepEquals, []byte("item"))
}

func (s *RawAsciiClientSuite) TestSet(c *C) {
	s.rw.recvBuf.WriteString("STORED\r\nSTORED\r\nSTORED\r\n")

	// item1 uses set because cas id is 0
	item1 := &Item{
		Key:           "key1",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	// item2 uses cas because cas id is 92
	item2 := &Item{
		Key:           "key2",
		Value:         []byte("i t e m 2 "),
		Flags:         234,
		DataVersionId: 92,
		Expiration:    747,
	}

	// item3 uses set because cas id is 0
	item3 := &Item{
		Key:           "key3",
		Value:         []byte("it\r\nem3"),
		Flags:         9,
		DataVersionId: 0,
		Expiration:    4,
	}

	responses := s.client.SetMulti([]*Item{item1, item2, item3})

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"set key1 123 555 5\r\nitem1\r\n"+
			"cas key2 234 747 10 92\r\ni t e m 2 \r\n"+
			"set key3 9 4 7\r\nit\r\nem3\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(len(responses), Equals, 3)
	for _, resp := range responses {
		c.Assert(resp.Error(), IsNil)
		c.Assert(resp.Status(), Equals, StatusNoError)
	}
}

func (s *RawAsciiClientSuite) TestSetNilItem(c *C) {
	resp := s.client.Set(nil)

	c.Assert(resp.Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestSetBadKey(c *C) {
	item := &Item{
		Key:           "b a d",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	resp := s.client.Set(item)

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestSetBadValue(c *C) {
	item := &Item{
		Key:           "key",
		Value:         make([]byte, defaultMaxValueLength+1),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	resp := s.client.Set(item)

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestStoreNotFound(c *C) {
	s.rw.recvBuf.WriteString("NOT_FOUND\r\n")

	item := &Item{
		Key:           "key",
		Value:         []byte("item"),
		Flags:         123,
		DataVersionId: 666,
		Expiration:    555,
	}

	resp := s.client.Set(item)
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"cas key 123 555 4 666\r\nitem\r\n")

	c.Assert(resp.Error(), NotNil)
	c.Assert(resp.Status(), Equals, StatusKeyNotFound)
}

func (s *RawAsciiClientSuite) TestStoreItemNotStore(c *C) {
	s.rw.recvBuf.WriteString("NOT_STORED\r\n")

	item := &Item{
		Key:           "key",
		Value:         []byte("item"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	resp := s.client.Add(item)
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"add key 123 555 4\r\nitem\r\n")

	c.Assert(resp.Error(), NotNil)
	c.Assert(resp.Status(), Equals, StatusItemNotStored)
}

func (s *RawAsciiClientSuite) TestStoreKeyExists(c *C) {
	s.rw.recvBuf.WriteString("EXISTS\r\n")

	item := &Item{
		Key:           "key",
		Value:         []byte("item"),
		Flags:         123,
		DataVersionId: 666,
		Expiration:    555,
	}

	resp := s.client.Set(item)
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"cas key 123 555 4 666\r\nitem\r\n")

	c.Assert(resp.Error(), NotNil)
	c.Assert(resp.Status(), Equals, StatusKeyExists)
}

func (s *RawAsciiClientSuite) TestStoreError(c *C) {
	s.rw.recvBuf.WriteString("SERVER_ERROR\r\n")

	item := &Item{
		Key:           "key",
		Value:         []byte("item"),
		Flags:         123,
		DataVersionId: 666,
		Expiration:    555,
	}

	resp := s.client.Set(item)
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"cas key 123 555 4 666\r\nitem\r\n")

	c.Assert(resp.Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestStoreErrorMidStream(c *C) {
	s.rw.recvBuf.WriteString("STORED\r\nSTO") // unexpected eof

	item1 := &Item{
		Key:           "key1",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	item2 := &Item{
		Key:           "key2",
		Value:         []byte("i t e m 2 "),
		Flags:         234,
		DataVersionId: 92,
		Expiration:    747,
	}

	item3 := &Item{
		Key:           "key3",
		Value:         []byte("it\r\nem3"),
		Flags:         9,
		DataVersionId: 0,
		Expiration:    4,
	}

	responses := s.client.SetMulti([]*Item{item1, item2, item3})

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"set key1 123 555 5\r\nitem1\r\n"+
			"cas key2 234 747 10 92\r\ni t e m 2 \r\n"+
			"set key3 9 4 7\r\nit\r\nem3\r\n")
	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(len(responses), Equals, 3)
	c.Assert(responses[0].Error(), IsNil)
	c.Assert(responses[1].Error(), NotNil)
	c.Assert(responses[2].Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestStoreCheckEmptyBuffers(c *C) {
	s.rw.recvBuf.WriteString("STORED\r\ncrap")

	item := &Item{
		Key:           "key1",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	resp := s.client.Set(item)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"set key1 123 555 5\r\nitem1\r\n")
	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(resp.Error(), IsNil)
}

func (s *RawAsciiClientSuite) TestAdd(c *C) {
	s.rw.recvBuf.WriteString("STORED\r\n")

	item := &Item{
		Key:           "key1",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	resp := s.client.Add(item)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"add key1 123 555 5\r\nitem1\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
}

func (s *RawAsciiClientSuite) TestAddInvalidCasId(c *C) {
	item := &Item{
		Key:           "key1",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 14,
		Expiration:    555,
	}

	resp := s.client.Add(item)

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestReplace(c *C) {
	s.rw.recvBuf.WriteString("STORED\r\n")

	item := &Item{
		Key:           "key1",
		Value:         []byte("item1"),
		Flags:         123,
		DataVersionId: 0,
		Expiration:    555,
	}

	resp := s.client.Replace(item)

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"replace key1 123 555 5\r\nitem1\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
}

func (s *RawAsciiClientSuite) TestAppend(c *C) {
	s.rw.recvBuf.WriteString("STORED\r\n")

	resp := s.client.Append("key", []byte("suffix"))

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"append key 0 0 6\r\nsuffix\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
}

func (s *RawAsciiClientSuite) TestPrepend(c *C) {
	s.rw.recvBuf.WriteString("STORED\r\n")

	resp := s.client.Prepend("key", []byte("prefix"))

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"prepend key 0 0 6\r\nprefix\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
}

func (s *RawAsciiClientSuite) TestDelete(c *C) {
	s.rw.recvBuf.WriteString("DELETED\r\nDELETED\r\n")

	responses := s.client.DeleteMulti([]string{"key1", "key2"})

	c.Assert(
		s.rw.sendBuf.String(),
		Equals,
		"delete key1\r\ndelete key2\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(len(responses), Equals, 2)
	for _, resp := range responses {
		c.Assert(resp.Error(), IsNil)
		c.Assert(resp.Status(), Equals, StatusNoError)
	}
}

func (s *RawAsciiClientSuite) TestDeleteNotFound(c *C) {
	s.rw.recvBuf.WriteString("NOT_FOUND\r\n")

	resp := s.client.Delete("key")

	c.Assert(s.rw.sendBuf.String(), Equals, "delete key\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	c.Assert(resp.Status(), Equals, StatusKeyNotFound)
}

func (s *RawAsciiClientSuite) TestDeleteError(c *C) {
	s.rw.recvBuf.WriteString("SERVER_ERROR\r\n")

	resp := s.client.Delete("key")

	c.Assert(s.rw.sendBuf.String(), Equals, "delete key\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestDeleteBadKey(c *C) {
	resp := s.client.Delete("b a d")

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestDeleteCheckEmptyBuffers(c *C) {
	s.rw.recvBuf.WriteString("DELETED\r\nextra")

	resp := s.client.Delete("key")

	c.Assert(s.rw.sendBuf.String(), Equals, "delete key\r\n")
	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusNoError)
}

func (s *RawAsciiClientSuite) TestIncrement(c *C) {
	s.rw.recvBuf.WriteString("16\r\n")

	resp := s.client.Increment("key", 2, 0, 0xffffffff)

	c.Assert(s.rw.sendBuf.String(), Equals, "incr key 2\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusNoError)
	c.Assert(resp.Key(), Equals, "key")
	c.Assert(resp.Count(), Equals, uint64(16))
}

func (s *RawAsciiClientSuite) TestIncrementBadKey(c *C) {
	resp := s.client.Increment("b a d", 2, 0, 0xffffffff)

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestIncrementBadExpiration(c *C) {
	resp := s.client.Increment("key", 2, 0, 0)

	c.Assert(s.rw.sendBuf.String(), Equals, "")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestIncrementNotFound(c *C) {
	s.rw.recvBuf.WriteString("NOT_FOUND\r\n")

	resp := s.client.Increment("key", 2, 0, 0xffffffff)

	c.Assert(s.rw.sendBuf.String(), Equals, "incr key 2\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
	c.Assert(resp.Status(), Equals, StatusKeyNotFound)
	c.Assert(resp.Key(), Equals, "key")
	c.Assert(resp.Count(), Equals, uint64(0))
}

func (s *RawAsciiClientSuite) TestIncrementError(c *C) {
	s.rw.recvBuf.WriteString("SERVER_ERROR\r\n")

	resp := s.client.Increment("key", 2, 0, 0xffffffff)

	c.Assert(s.rw.sendBuf.String(), Equals, "incr key 2\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestIncrementCheckEmptyBuffers(c *C) {
	s.rw.recvBuf.WriteString("89\r\nextra")

	resp := s.client.Increment("key", 24, 0, 0xffffffff)

	c.Assert(s.rw.sendBuf.String(), Equals, "incr key 24\r\n")
	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Count(), Equals, uint64(89))
}

func (s *RawAsciiClientSuite) TestDecrement(c *C) {
	s.rw.recvBuf.WriteString("123\r\n")

	resp := s.client.Decrement("key1", 5, 0, 0xffffffff)

	c.Assert(s.rw.sendBuf.String(), Equals, "decr key1 5\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Status(), Equals, StatusNoError)
	c.Assert(resp.Key(), Equals, "key1")
	c.Assert(resp.Count(), Equals, uint64(123))
}

func (s *RawAsciiClientSuite) TestFlush(c *C) {
	s.rw.recvBuf.WriteString("OK\r\n")

	resp := s.client.Flush(123)

	c.Assert(s.rw.sendBuf.String(), Equals, "flush_all 123\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), IsNil)
}

func (s *RawAsciiClientSuite) TestFlushError(c *C) {
	s.rw.recvBuf.WriteString("SERVER_ERROR\r\n")

	resp := s.client.Flush(0)

	c.Assert(s.rw.sendBuf.String(), Equals, "flush_all 0\r\n")
	c.Assert(s.client.IsValidState(), IsTrue)

	c.Assert(resp.Error(), NotNil)
}

func (s *RawAsciiClientSuite) TestFlushCheckEmptyBuffers(c *C) {
	s.rw.recvBuf.WriteString("OK\r\nextra")

	resp := s.client.Flush(123)

	c.Assert(s.rw.sendBuf.String(), Equals, "flush_all 123\r\n")
	c.Assert(s.client.IsValidState(), IsFalse)

	c.Assert(resp.Error(), IsNil)
}
