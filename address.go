package kafka

import "net"

// TCP constructs an address with the network set to "tcp".
func TCP(address string) net.Addr { return Addr("tcp", address) }

// TLS constructs an address with the network set to "tls".
func TLS(address string) net.Addr { return Addr("tls", address) }

// Addr returns a net.Addr from a pair of network and address.
func Addr(network, address string) net.Addr {
	return &networkAddress{
		network: network,
		address: address,
	}
}

type networkAddress struct {
	network string
	address string
}

func (a *networkAddress) Network() string { return a.network }

func (a *networkAddress) String() string { return a.address }
