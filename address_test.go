package kafka

import (
	"net"
	"testing"
)

func TestNetworkAddress(t *testing.T) {
	tests := []struct {
		addr    net.Addr
		network string
		address string
	}{
		{
			addr:    TCP("127.0.0.1"),
			network: "tcp",
			address: "127.0.0.1:9092",
		},

		{
			addr:    TCP("::1"),
			network: "tcp",
			address: "[::1]:9092",
		},

		{
			addr:    TCP("localhost"),
			network: "tcp",
			address: "localhost:9092",
		},

		{
			addr:    TCP("localhost:9092"),
			network: "tcp",
			address: "localhost:9092",
		},

		{
			addr:    TCP("localhost", "localhost:9093", "localhost:9094"),
			network: "tcp,tcp,tcp",
			address: "localhost:9092,localhost:9093,localhost:9094",
		},
	}

	for _, test := range tests {
		t.Run(test.network+"+"+test.address, func(t *testing.T) {
			if s := test.addr.Network(); s != test.network {
				t.Errorf("network mismatch: want %q but got %q", test.network, s)
			}
			if s := test.addr.String(); s != test.address {
				t.Errorf("network mismatch: want %q but got %q", test.address, s)
			}
		})
	}
}
