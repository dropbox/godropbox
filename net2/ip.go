package net2

import (
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/dropbox/godropbox/errors"
)

var myHostname string
var myHostnameOnce sync.Once

// Like os.Hostname but caches first successful result, making it cheap to call it
// over and over.
// It will also crash whole process if fetching Hostname fails!
func MyHostname() string {
	myHostnameOnce.Do(func() {
		var err error
		myHostname, err = os.Hostname()
		if err != nil {
			log.Fatal(err)
		}
	})
	return myHostname
}

var myIp4 *net.IPAddr
var myIp4Once sync.Once

// Resolves `MyHostname()` to an Ip4 address. Caches first successful result, making it
// cheap to call it over and over.
// It will also crash whole process if resolving the IP fails!
func MyIp4() *net.IPAddr {
	myIp4Once.Do(func() {
		var err error
		myIp4, err = net.ResolveIPAddr("ip4", MyHostname())
		if err != nil {
			log.Fatal(err)
		}
	})
	return myIp4
}

var myIp6 *net.IPAddr
var myIp6Once sync.Once

// Resolves `MyHostname()` to an Ip6 address. Caches first successful result, making it
// cheap to call it over and over.
// It will also crash whole process if resolving the IP fails!
func MyIp6() *net.IPAddr {
	myIp6Once.Do(func() {
		var err error
		myIp6, err = net.ResolveIPAddr("ip6", MyHostname())
		if err != nil {
			log.Fatal(err)
		}
	})
	return myIp6
}

// This returns the list of local ip addresses which other hosts can connect
// to (NOTE: Loopback ip is ignored).
// Also resolves Hostname to an address and adds it to the list too, so
// IPs from /etc/hosts can work too.
func GetLocalIPs() ([]*net.IP, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to lookup hostname")
	}
	// Resolves IP Address from Hostname, this way overrides in /etc/hosts
	// can work too for IP resolution.
	ipInfo, err := net.ResolveIPAddr("ip4", hostname)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to resolve ip")
	}
	ips := []*net.IP{&ipInfo.IP}

	// TODO(zviad): Is rest of the code really necessary?
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get interface addresses.")
	}
	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}

		if ipnet.IP.IsLoopback() {
			continue
		}

		ips = append(ips, &ipnet.IP)
	}
	return ips, nil
}

// Given a host string, return true if the host is an ip (v4/v6) localhost.
func IsLocalhost(host string) bool {
	return host == "localhost" ||
		host == "ip6-localhost" ||
		host == "ipv6-localhost"
}

// Resolves hostnames in addresses to actual IP4 addresses. Skips all invalid addresses
// and all addresses that can't be resolved.
// `addrs` are assumed to be of form: ["<hostname>:<port>", ...]
// Returns an error in addition to resolved addresses if not all resolutions succeed.
func ResolveIP4s(addrs []string) ([]string, error) {
	resolvedAddrs := make([]string, 0, len(addrs))
	var lastErr error

	for _, server := range addrs {
		hostPort := strings.Split(server, ":")
		if len(hostPort) != 2 {
			lastErr = errors.Newf(
				"Skipping invalid address: %s", server)
			continue
		}

		ip, err := net.ResolveIPAddr("ip4", hostPort[0])
		if err != nil {
			lastErr = err
			continue
		}
		resolvedAddrs = append(resolvedAddrs, ip.IP.String()+":"+hostPort[1])
	}
	return resolvedAddrs, lastErr
}
