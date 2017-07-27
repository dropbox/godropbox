package memcache_test

import (
	"fmt"
	"hash/crc32"
	"net"

	"github.com/dropbox/godropbox/memcache"
	"github.com/dropbox/godropbox/net2"
)

func ExampleRawBinaryClient() {
	conn, _ := net.Dial("tcp", "localhost:11211")

	client := memcache.NewRawBinaryClient(0, conn)

	clientExample(client)
}

func ExampleRawAsciiClient() {
	conn, _ := net.Dial("tcp", "localhost:11211")

	client := memcache.NewRawAsciiClient(0, conn)

	clientExample(client)
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

	client := memcache.NewShardedClient(manager, memcache.NewRawBinaryClient)

	clientExample(client)
}

func clientExample(client memcache.Client) {
	version := func() {
		resp := client.Version()
		fmt.Println("Version")
		for i, v := range resp.Versions() {
			fmt.Println("  Shard", i, "Version", v)
		}
		fmt.Println("  Status:", resp.Status())
		if resp.Error() != nil {
			fmt.Println("  Error:", resp.Error())
		}
	}

	verbosity := func(v uint32) {
		resp := client.Verbosity(v)
		fmt.Println("Verbosity")
		fmt.Println("  Status:", resp.Status())
		if resp.Error() != nil {
			fmt.Println("  Error:", resp.Error())
		}
	}

	stats := func(key string) {
		resp := client.Stat(key)
		fmt.Println("Stats:", key)
		for i, s := range resp.Entries() {
			fmt.Println("  Shard", i)
			for k, v := range s {
				fmt.Println("    ", k, ":", v)
			}
		}
		fmt.Println("  Status:", resp.Status())
		if resp.Error() != nil {
			fmt.Println("  Error:", resp.Error())
		}
	}

	flush := func() {
		resp := client.Flush(0)
		fmt.Println("Flush")
		fmt.Println("  Status:", resp.Status())
		if resp.Error() != nil {
			fmt.Println("  Error:", resp.Error())
		}
	}

	get := func(key string) uint64 {
		resp := client.Get(key)
		fmt.Println("Get", resp.Key())
		fmt.Println("  Status:", resp.Status())
		fmt.Println("  Value:", string(resp.Value()))
		fmt.Println("  Flags:", resp.Flags())
		fmt.Println("  CasId:", resp.DataVersionId())
		if resp.Error() != nil {
			fmt.Println("  Error:", resp.Error())
		}

		return resp.DataVersionId()
	}

	set := func(item *memcache.Item) {
		resp := client.Set(item)
		fmt.Println("Set", resp.Key())
		fmt.Println("  Status:", resp.Status())
		if resp.Error() != nil {
			fmt.Println("  Error:", resp.Error())
		}
	}

	add := func(item *memcache.Item) {
		resp := client.Add(item)
		fmt.Println("Add", resp.Key())
		fmt.Println("  Status:", resp.Status())
		if resp.Error() != nil {
			fmt.Println("  Error:", resp.Error())
		}
	}

	replace := func(item *memcache.Item) {
		resp := client.Replace(item)
		fmt.Println("Replace", resp.Key())
		fmt.Println("  Status:", resp.Status())
		if resp.Error() != nil {
			fmt.Println("  Error:", resp.Error())
		}
	}

	del := func(key string) {
		resp := client.Delete(key)
		fmt.Println("Delete", resp.Key())
		fmt.Println("  Status:", resp.Status())
		if resp.Error() != nil {
			fmt.Println("  Error:", resp.Error())
		}
	}

	inc := func(key string) {
		resp := client.Increment(key, 1, 0, 0xffffffff)
		fmt.Println("Inc:", key)
		fmt.Println("  Status:", resp.Status())
		fmt.Println("  Key:", resp.Key())
		fmt.Println("  Count:", resp.Count())
		if resp.Error() != nil {
			fmt.Println("  Error:", resp.Error())
		}
	}

	dec := func(key string) {
		resp := client.Decrement(key, 1, 0, 0xffffffff)
		fmt.Println("Dec:", key)
		fmt.Println("  Status:", resp.Status())
		fmt.Println("  Key:", resp.Key())
		fmt.Println("  Count:", resp.Count())
		if resp.Error() != nil {
			fmt.Println("  Error:", resp.Error())
		}
	}

	pre := func(key string, prefix []byte) {
		resp := client.Prepend(key, prefix)
		fmt.Println("Prepend:", key)
		fmt.Println("  Status:", resp.Status())
		if resp.Error() != nil {
			fmt.Println("  Error:", resp.Error())
		}
	}

	app := func(key string, suffix []byte) {
		resp := client.Append(key, suffix)
		fmt.Println("Append:", key)
		fmt.Println("  Status:", resp.Status())
		if resp.Error() != nil {
			fmt.Println("  Error:", resp.Error())
		}
	}

	item := memcache.Item{
		Key:   "bar",
		Value: []byte("Hello World"),
		Flags: uint32(123),
	}

	item2 := memcache.Item{
		Key:   "zzz",
		Value: []byte("1"),
		Flags: uint32(666),
	}

	version()
	verbosity(2)
	stats("")
	flush()
	get("foo")
	get("bar")
	replace(&item)
	set(&item)
	get("bar")
	add(&item)
	item.Flags = 321
	replace(&item)
	get("bar")
	del("bar")
	get("bar")

	get("zzz")
	add(&item2)
	get("zzz")
	inc("zzz")
	dec("zzz")
	pre("zzz", []byte{'6'})
	app("zzz", []byte{'9'})
	get("zzz")
}

/*
func main() {
	ExampleRawAsciiClient()
}
*/
