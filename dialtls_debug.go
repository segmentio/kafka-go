//go:build debug
// +build debug

package kafka

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

	return errorDecoratingConn{tlsConn}, nil
}

type errorDecoratingConn struct {
	net.Conn
}

func (l errorDecoratingConn) Read(b []byte) (n int, err error) {
	n, err = l.Conn.Read(b)
	if errors.Is(err, io.EOF) {
		err = fmt.Errorf("error reading %s <- %s: %w", l.LocalAddr(), l.RemoteAddr(), err)
	}
	return n, err
}

func (l errorDecoratingConn) Write(b []byte) (n int, err error) {
	n, err = l.Conn.Write(b)
	if errors.Is(err, io.EOF) {
		err = fmt.Errorf("error writing %s -> %s: %w", l.LocalAddr(), l.RemoteAddr(), err)
	}
	return n, err
}
