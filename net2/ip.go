package net2

import (
	"net"

	"github.com/dropbox/godropbox/errors"
)

// This returns the list of local ip addresses which other hosts can connect
// to (NOTE: Loopback ip is ignored).
func GetLocalIPs() ([]*net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get interface addresses.")
	}

	ips := make([]*net.IP, 0)
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
