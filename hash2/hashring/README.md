hashring
============================

Implements consistent hashing that can be used when
the number of server nodes can increase or decrease (like in memcached).
The hashing ring is built using the same algorithm as libketama.

This is a port of Python hash_ring library <https://pypi.python.org/pypi/hash_ring/>
in Go.

It is a modified implementation of https://github.com/serialx/hashring


Using
============================

Basic example usage ::

```go
memcacheServers := []string{"192.168.0.246:11212",
                            "192.168.0.247:11212",
                            "192.168.0.249:11212"}

ring := hashring.New(memcacheServers)
server := ring.GetNode("my_key")
```

You can iterate through all servers starting with the key's initial corresponding
position on the ring. This can be used to favor availability > consistency, or
perform key replication.
```go
serversInRing := []string{"192.168.0.246:11212",
                          "192.168.0.247:11212",
                          "192.168.0.248:11212",
                          "192.168.0.249:11212",
                          "192.168.0.250:11212",
                          "192.168.0.251:11212",
                          "192.168.0.252:11212"}

ring := hashring.New(serversInRing)
server := ring.GetNodes("my_key")
```
