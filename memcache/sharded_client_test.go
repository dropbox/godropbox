package memcache

import (
	"context"
	susanin_pb "dropbox/proto/susanin"
	"strings"

	. "godropbox/gocheck2"
	"godropbox/net2"
	. "gopkg.in/check.v1"
)

type ShardedClientSuite struct{}

var _ = Suite(&ShardedClientSuite{})

func assertNoShardError(c *C, resp Response) {
	err := resp.Error()
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "No Memcache shard"), IsTrue)
}

func (s *ShardedClientSuite) TestNoShards(c *C) {
	testKeys := []string{"key1", "key2"}
	manager := NewBaseShardManagerWithOptions(
		func(err error) {},
		func(pool net2.ConnectionPool) {},
		[]*susanin_pb.Address{}, DefaultConnectionOptions())
	shardedClient := NewShardedClient(manager, NewRawBinaryClient)
	ctx := context.Background()
	assertNoShardError(c, shardedClient.Set(ctx, &Item{Key: testKeys[0], Value: []byte("value")}))
	assertNoShardError(c, shardedClient.Get(ctx, testKeys[0]))

	items := make([]*Item, len(testKeys))
	for i, key := range testKeys {
		items[i] = &Item{Key: key, Value: []byte("testVal")}
	}
	setResps := shardedClient.SetMulti(ctx, items)
	for _, resp := range setResps {
		assertNoShardError(c, resp)
	}
	getResps := shardedClient.GetMulti(ctx, testKeys)
	for _, key := range testKeys {
		assertNoShardError(c, getResps[key])
	}
}
