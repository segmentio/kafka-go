//go:build !debug
// +build !debug

package kafka

import (
	"context"
	"crypto/tls"
	"net"
	"time"
)

func dialTLS(ctx context.Context, conn net.Conn, tlsConfig *tls.Config, netAddr net.Addr, deadline time.Time) (net.Conn, error) {
	if tlsConfig.ServerName == "" {
		host, _ := splitHostPort(netAddr.String())
		tlsConfig = tlsConfig.Clone()
		tlsConfig.ServerName = host
	}
	tlsConn := tls.Client(conn, tlsConfig)
	err := performTLSHandshake(ctx, tlsConn, deadline)
	if err != nil {
		return nil, err
	}

	return tlsConn, nil
}
