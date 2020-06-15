package kafka

import (
	"net"
	"strings"
)

// TCP constructs an address with the network set to "tcp".
func TCP(address ...string) net.Addr { return makeNetAddr("tcp", address) }

func makeNetAddr(network string, addresses []string) net.Addr {
	switch len(addresses) {
	case 0:
		return nil // maybe panic instead?
	case 1:
		return makeAddr(network, addresses[0])
	default:
		return makeMultiAddr(network, addresses)
	}
}

func makeAddr(network, address string) net.Addr {
	host, port, _ := net.SplitHostPort(address)
	if port == "" {
		port = "9092"
	}
	if host == "" {
		host = address
	}
	return &Addr{
		Net:  network,
		Addr: net.JoinHostPort(host, port),
	}
}

func makeMultiAddr(network string, addresses []string) net.Addr {
	multi := make(MultiAddr, len(addresses))
	for i, address := range addresses {
		multi[i] = makeAddr(network, address)
	}
	return multi
}

// Addr is a generic implementation of the net.Addr interface.
type Addr struct {
	Net  string
	Addr string
}

// Network returns a.Net, satisfies the net.Addr interface.
func (a *Addr) Network() string { return a.Net }

// String returns a.Addr, satisifes the net.Addr interface.
func (a *Addr) String() string { return a.Addr }

// MultiAddr is an implementation of the net.Addr interface for a set of network
// addresses.
type MultiAddr []net.Addr

// Network returns the comma-separated list of networks included in m.
func (m MultiAddr) Network() string { return m.join(net.Addr.Network) }

// String returns the comma-separated list of addresses included in m.
func (m MultiAddr) String() string { return m.join(net.Addr.String) }

func (m MultiAddr) join(f func(net.Addr) string) string {
	switch len(m) {
	case 0:
		return ""
	case 1:
		return f(m[0])
	}
	s := make([]string, len(m))
	for i, a := range m {
		s[i] = f(a)
	}
	return strings.Join(s, ",")
}
