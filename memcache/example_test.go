package memcache_test

import (
	"fmt"
	"hash/crc32"
	"net"

	"github.com/dropbox/godropbox/memcache"
	"github.com/dropbox/godropbox/net2"
)

func ExampleRawClient() {
	conn, _ := net.Dial("tcp", "localhost:11211")

	client := memcache.NewRawClient(0, conn)

	get := func(key string) {
		resp := client.Get(key)
		fmt.Println("Get", resp.Key())
		fmt.Println(
			"  Status:",
			resp.Status(),
			"-",
			memcache.NewStatusCodeError(resp.Status()))
		fmt.Println("  Error:", resp.Error())
		fmt.Println("  Value:", resp.Value())
	}

	set := func(item *memcache.Item) {
		resp := client.Set(item)
		fmt.Println("Set", resp.Key())
		fmt.Println(
			"  Status:",
			resp.Status(),
			"-",
			memcache.NewStatusCodeError(resp.Status()))
		fmt.Println("  Error:", resp.Error())
	}

	del := func(key string) {
		resp := client.Delete(key)
		fmt.Println("Delete", resp.Key())
		fmt.Println(
			"  Status:",
			resp.Status(),
			"-",
			memcache.NewStatusCodeError(resp.Status()))
		fmt.Println("  Error:", resp.Error())
	}

	item := memcache.Item{
		Key:   "bar",
		Value: []byte("Hello World"),
		Flags: uint32(123),
	}

	get("foo")
	get("bar")
	set(&item)
	get("bar")
	del("bar")
	get("bar")
}

func ExampleShardedClient() {
	options := net2.ConnectionOptions{
		MaxActiveConnections: 4,
	}
	manager := memcache.NewStaticShardManager(
		[]string{"localhost:11211", "localhost:11212"},
		func(key string, numShard int) int {
			return int(crc32.ChecksumIEEE([]byte(key))) % 2
		},
		options)

	client := memcache.NewShardedClient(manager)

	get := func(key string) {
		resp := client.Get(key)
		fmt.Println("Get", resp.Key())
		fmt.Println(
			"  Status:",
			resp.Status(),
			"-",
			memcache.NewStatusCodeError(resp.Status()))
		fmt.Println("  Error:", resp.Error())
		fmt.Println("  Value:", string(resp.Value()))
	}

	set := func(item *memcache.Item) {
		resp := client.Set(item)
		fmt.Println("Set", resp.Key())
		fmt.Println(
			"  Status:",
			resp.Status(),
			"-",
			memcache.NewStatusCodeError(resp.Status()))
		fmt.Println("  Error:", resp.Error())
	}

	del := func(key string) {
		resp := client.Delete(key)
		fmt.Println("Delete", resp.Key())
		fmt.Println(
			"  Status:",
			resp.Status(),
			"-",
			memcache.NewStatusCodeError(resp.Status()))
		fmt.Println("  Error:", resp.Error())
	}

	item := memcache.Item{
		Key:   "bar",
		Value: []byte("Hello World"),
		Flags: uint32(123),
	}

	get("foo")
	get("bar")
	set(&item)
	get("bar")
	del("bar")
	get("bar")
}
