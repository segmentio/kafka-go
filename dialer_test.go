package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestDialer(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T, context.Context, *Dialer)
	}{
		{
			scenario: "looking up partitions returns the list of available partitions for a topic",
			function: testDialerLookupPartitions,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			testFunc(t, ctx, &Dialer{})
		})
	}
}

func testDialerLookupPartitions(t *testing.T, ctx context.Context, d *Dialer) {
	const topic = "test-dialer-LookupPartitions"

	createTopic(t, topic, 1)

	// Write a message to ensure the partition gets created.
	w := NewWriter(WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   topic,
	})
	w.WriteMessages(ctx, Message{})
	w.Close()

	partitions, err := d.LookupPartitions(ctx, "tcp", "localhost:9092", topic)
	if err != nil {
		t.Error(err)
		return
	}

	sort.Slice(partitions, func(i int, j int) bool {
		return partitions[i].ID < partitions[j].ID
	})

	want := []Partition{
		{
			Topic:    "test-dialer-LookupPartitions",
			Leader:   Broker{Host: "localhost", Port: 9092, ID: 1},
			Replicas: []Broker{{Host: "localhost", Port: 9092, ID: 1}},
			Isr:      []Broker{{Host: "localhost", Port: 9092, ID: 1}},
			ID:       0,
		},
	}
	if !reflect.DeepEqual(partitions, want) {
		t.Errorf("bad partitions:\ngot:  %+v\nwant: %+v", partitions, want)
	}
}

func tlsConfig(t *testing.T) *tls.Config {
	const (
		certPEM = `-----BEGIN CERTIFICATE-----
MIID2zCCAsOgAwIBAgIJAMSqbewCgw4xMA0GCSqGSIb3DQEBCwUAMGAxCzAJBgNV
BAYTAlVTMRMwEQYDVQQIDApDYWxpZm9ybmlhMRYwFAYDVQQHDA1TYW4gRnJhbmNp
c2NvMRAwDgYDVQQKDAdTZWdtZW50MRIwEAYDVQQDDAlsb2NhbGhvc3QwHhcNMTcx
MjIzMTU1NzAxWhcNMjcxMjIxMTU1NzAxWjBgMQswCQYDVQQGEwJVUzETMBEGA1UE
CAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzEQMA4GA1UECgwH
U2VnbWVudDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOC
AQ8AMIIBCgKCAQEAtda9OWKYNtINe/BKAoB+/zLg2qbaTeHN7L722Ug7YoY6zMVB
aQEHrUmshw/TOrT7GLN/6e6rFN74UuNg72C1tsflZvxqkGdrup3I3jxMh2ApAxLi
zem/M6Eke2OAqt+SzRPqc5GXH/nrWVd3wqg48DZOAR0jVTY2e0fWy+Er/cPJI1lc
L6ZMIRJikHTXkaiFj2Jct1iWvgizx5HZJBxXJn2Awix5nvc+zmXM0ZhoedbJRoBC
dGkRXd3xv2F4lqgVHtP3Ydjc/wYoPiGudSAkhyl9tnkHjvIjA/LeRNshWHbCIaQX
yemnXIcyyf+W+7EK0gXio7uiP+QSoM5v/oeVMQIDAQABo4GXMIGUMHoGA1UdIwRz
MHGhZKRiMGAxCzAJBgNVBAYTAlVTMRMwEQYDVQQIDApDYWxpZm9ybmlhMRYwFAYD
VQQHDA1TYW4gRnJhbmNpc2NvMRAwDgYDVQQKDAdTZWdtZW50MRIwEAYDVQQDDAls
b2NhbGhvc3SCCQCBYUuEuypDMTAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DANBgkq
hkiG9w0BAQsFAAOCAQEATk6IlVsXtNp4C1yeegaM+jE8qgKJfNm1sV27zKx8HPiO
F7LvTGYIG7zd+bf3pDSwRxfBhsLEwmN9TUN1d6Aa9zeu95qOnR76POfHILgttu2w
IzegO8I7BycnLjU9o/l9gCpusnN95tIYQhfD08ygUpYTQRuI0cmZ/Dp3xb0S9f5N
miYTuUoStYSA4RWbDWo+Is9YWPu7rwieziOZ96oguGz3mtqvkjxVAQH1xZr3bKHr
HU9LpQh0i6oTK0UCqnDwlhJl1c7A3UooxFpc3NGxyjogzTfI/gnBKfPo7eeswwsV
77rjIkhBW49L35KOo1uyblgK1vTT7VPtzJnuDq3ORg==
-----END CERTIFICATE-----`

		keyPEM = `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAtda9OWKYNtINe/BKAoB+/zLg2qbaTeHN7L722Ug7YoY6zMVB
aQEHrUmshw/TOrT7GLN/6e6rFN74UuNg72C1tsflZvxqkGdrup3I3jxMh2ApAxLi
zem/M6Eke2OAqt+SzRPqc5GXH/nrWVd3wqg48DZOAR0jVTY2e0fWy+Er/cPJI1lc
L6ZMIRJikHTXkaiFj2Jct1iWvgizx5HZJBxXJn2Awix5nvc+zmXM0ZhoedbJRoBC
dGkRXd3xv2F4lqgVHtP3Ydjc/wYoPiGudSAkhyl9tnkHjvIjA/LeRNshWHbCIaQX
yemnXIcyyf+W+7EK0gXio7uiP+QSoM5v/oeVMQIDAQABAoIBAQCa6roHW8JGYipu
vsau3v5TOOtsHN67n3arDf6MGwfM5oLN1ffmF6SMs8myv36781hBMRv3FwjWHSf+
pgz9o6zsbd05Ii8/m3yiXq609zZT107ZeYuU1mG5AL5uCNWjvhn5cdA6aX0RFwC0
+tnjEyJ/NCS8ujBR9n/wA8IxrEKoTGcxRb6qFPPKWYoBevu34td1Szf0kH8AKjtQ
rdPK0Of/ZEiAUxNMLTBEOmC0ZabxJV/YGWcUU4DpmEDZSgQSr4yLT4BFUwF2VC8t
8VXn5dBP3RMo4h7JlteulcKYsMQZXD6KvUwY2LaEpFM/b14r+TZTUQGhwS+Ha11m
xa4eNwFhAoGBANshGlpR9cUUq8vNex0Wb63P9BTRTXwg1yEJVMSua+DlaaqaX/hS
hOxl3K4y2V5OCK31C+SOAqqbrGtMXVym5c5pX8YyC11HupFJwdFLUEc74uF3CtWY
GMMvEvItCK5ZvYvS5I2CQGcp1fhEMle/Uz+hFi1eeWepMqgHbVx5vkdtAoGBANRv
XYQsTAGSkhcHB++/ASDskAew5EoHfwtJzSX0BZC6DCACF/U4dCKzBVndOrELOPXs
2CZXCG4ptWzNgt6YTlMX9U7nLei5pPjoivIJsMudnc22DrDS7C94rCk++M3JeLOM
KSN0ou9+1iEdE7rQdMgZMryaY71OBonCIDsWgJZVAoGAB+k0CFq5IrpSUXNDpJMw
yPee+jlsMLUGzzyFAOzDHEVsASq9mDtybQ5oXymay1rJ2W3lVgUCd6JTITSKklO8
LC2FtaQM4Ps78w7Unne3mDrDQByKGZf6HOHQL0oM7C51N10Pv0Qaix7piKL9pklT
+hIYuN6WR3XGTGaoPhRvGCkCgYBqaQ5y8q1v7Dd5iXAUS50JHPZYo+b2niKpSOKW
LFHNWSRRtDrD/u9Nolb/2K1ZmcGCjo0HR3lVlVbnlVoEnk49mTaru2lntfZJKFLR
QsFofR9at+NL95uPe+bhEkYW7uCjL4Y72GT1ipdAJwyG+3xD7ztW9g8X+EmWH8N9
VZw7sQKBgGxp820jbjWhG1O9RnYLwflcZzUlSkhWJDg9tKJXBjD+hFX98Okuf0gu
DUpdbxbJHSi0xAjOjLVswNws4pVwzgtZVK8R7k8j3Z5TtYTJTSQLfgVowuyEdAaI
C8OxVJ/At/IJGnWSIz8z+/YCUf7p4jd2LJgmZVVzXeDsOFcH62gu
-----END RSA PRIVATE KEY-----`

		caPEM = `-----BEGIN CERTIFICATE-----
MIIDPDCCAiQCCQCBYUuEuypDMTANBgkqhkiG9w0BAQsFADBgMQswCQYDVQQGEwJV
UzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5jaXNjbzEQ
MA4GA1UECgwHU2VnbWVudDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTE3MTIyMzE1
NTMxOVoXDTI3MTIyMTE1NTMxOVowYDELMAkGA1UEBhMCVVMxEzARBgNVBAgMCkNh
bGlmb3JuaWExFjAUBgNVBAcMDVNhbiBGcmFuY2lzY28xEDAOBgNVBAoMB1NlZ21l
bnQxEjAQBgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBAJwB+Yp6MyUepgtaRDxVjpMI2RmlAaV1qApMWu60LWGKJs4KWoIoLl6p
oSEqnWrpMmb38pyGP99X1+t3uZjiK9L8nFhuKZ581tsTKLxaSl+YVg7JbH5LVCS6
opsfB5ON1gJxf1HA9YyMqKHkBFh8/hdOGR0T6Bll9TPO1NQB/UqMy/tKr3sA3KZm
XVDbRKSuUAQWz5J9/hLPmVMU41F/uD7mvyDY+x8GymInZjUXG4e0oq2RJgU6SYZ8
mkscM6qhKY3mL487w/kHVFtFlMkOhvI7LIh3zVvWwgGSAoAv9yai9BDZNFSk0cEb
bb/IK7BQW9sNI3lcnGirdbnjV94X9/sCAwEAATANBgkqhkiG9w0BAQsFAAOCAQEA
MJLeGdYO3dpsPx2R39Bw0qa5cUh42huPf8n7rp4a4Ca5jJjcAlCYV8HzqOzpiKYy
ZNuHy8LnNVYYh5Qoh8EO45bplMV1wnHfi6hW6DY5j3SQdcxkoVsW5R7rBF7a7SDg
6uChVRPHgsnALUUc7Wvvd3sAs/NKHzHu86mgD3EefkdqWAaCapzcqT9mo9KXkWJM
DhSJS+/iIaroc8umDnbPfhhgnlMf0/D4q0TjiLSSqyLzVifxnv9yHz56TrhHG/QP
E/8+FEGCHYKM4JLr5smGlzv72Kfx9E1CkG6TgFNIHjipVv1AtYDvaNMdPF2533+F
wE3YmpC3Q0g9r44nEbz4Bw==
-----END CERTIFICATE-----`
	)

	// Define TLS configuration
	certificate, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM([]byte(caPEM)); !ok {
		t.Error(err)
		t.FailNow()
	}

	return &tls.Config{
		Certificates:       []tls.Certificate{certificate},
		RootCAs:            caCertPool,
		InsecureSkipVerify: true,
	}
}

func TestDialerTLS(t *testing.T) {
	t.Parallel()

	const topic = "test-dialer-LookupPartitions"

	createTopic(t, topic, 1)

	// Write a message to ensure the partition gets created.
	w := NewWriter(WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   topic,
	})
	w.WriteMessages(context.Background(), Message{})
	w.Close()

	// Create an SSL proxy using the tls.Config that connects to the
	// docker-composed kafka
	config := tlsConfig(t)
	l, err := tls.Listen("tcp", "127.0.0.1:", config)
	if err != nil {
		t.Error(err)
		return
	}
	defer l.Close()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return // intentionally ignored
			}

			go func(in net.Conn) {
				out, err := net.Dial("tcp", "localhost:9092")
				if err != nil {
					t.Error(err)
					return
				}
				defer out.Close()

				go io.Copy(in, out)
				io.Copy(out, in)
			}(conn)
		}
	}()

	// Use the tls.Config and connect to the SSL proxy
	d := &Dialer{
		TLS: config,
	}
	partitions, err := d.LookupPartitions(context.Background(), "tcp", l.Addr().String(), topic)
	if err != nil {
		t.Error(err)
		return
	}

	// Verify returned partition data is what we expect
	sort.Slice(partitions, func(i int, j int) bool {
		return partitions[i].ID < partitions[j].ID
	})

	want := []Partition{
		{
			Topic:    topic,
			Leader:   Broker{Host: "localhost", Port: 9092, ID: 1},
			Replicas: []Broker{{Host: "localhost", Port: 9092, ID: 1}},
			Isr:      []Broker{{Host: "localhost", Port: 9092, ID: 1}},
			ID:       0,
		},
	}
	if !reflect.DeepEqual(partitions, want) {
		t.Errorf("bad partitions:\ngot:  %+v\nwant: %+v", partitions, want)
	}
}

type MockConn struct {
	net.Conn
	done       chan struct{}
	partitions []Partition
}

func (m *MockConn) Read(b []byte) (n int, err error) {
	select {
	case <-time.After(time.Minute):
	case <-m.done:
		return 0, context.Canceled
	}

	return 0, io.EOF
}

func (m *MockConn) Write(b []byte) (n int, err error) {
	select {
	case <-time.After(time.Minute):
	case <-m.done:
		return 0, context.Canceled
	}

	return 0, io.EOF
}

func (m *MockConn) Close() error {
	select {
	case <-m.done:
	default:
		close(m.done)
	}
	return nil
}

func (m *MockConn) ReadPartitions(topics ...string) (partitions []Partition, err error) {
	return m.partitions, err
}

func TestDialerConnectTLSHonorsContext(t *testing.T) {
	config := tlsConfig(t)
	d := &Dialer{
		TLS: config,
	}

	conn := &MockConn{
		done: make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*25)
	defer cancel()

	_, err := d.connectTLS(ctx, conn, d.TLS)
	if context.DeadlineExceeded != err {
		t.Errorf("expected err to be %v; got %v", context.DeadlineExceeded, err)
		t.FailNow()
	}
}
