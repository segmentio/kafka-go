package gssapi

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"io/ioutil"

	"github.com/jcmturner/gofork/encoding/asn1"
	"github.com/jcmturner/gokrb5/v8/asn1tools"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/chksumtype"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/segmentio/kafka-go/sasl"
)

const (
	// https://tools.ietf.org/html/rfc2743#page-81
	tokenTag byte = 0x60
	// https://tools.ietf.org/html/rfc1964#section-1.1
	tokenID uint16 = 0x100
)

type mechanism struct {
	principal string
	realm     string
	spn       string
	cfg       *config.Config
	kt        *keytab.Keytab
}

// KerberosConfig contains the necessary data for authenticating via Kerberos.
type KerberosConfig struct {
	Principal     string
	Realm         string
	SPN           string
	Configuration io.Reader
	Keytab        io.Reader
}

// Mechanism creates a new sasl.Mechanism using the GSSAPI Kerberos based
// authentication.
func Mechanism(conf KerberosConfig) (sasl.Mechanism, error) {
	cfg, err := config.NewFromReader(conf.Configuration)
	if err != nil {
		return nil, err
	}

	ktbytes, err := ioutil.ReadAll(conf.Keytab)
	if err != nil {
		return nil, err
	}

	kt := keytab.New()
	if err := kt.Unmarshal(ktbytes); err != nil {
		return nil, err
	}

	return &mechanism{
		principal: conf.Principal,
		realm:     conf.Realm,
		spn:       conf.SPN,
		cfg:       cfg,
		kt:        kt,
	}, nil
}

// Name returns the identifier of the GSSAPI mechanism.
func (*mechanism) Name() string {
	return "GSSAPI"
}

// https://tools.ietf.org/html/rfc1964#section-1.1.1
func gssapiCksum() types.Checksum {
	cksum := make([]byte, 24)

	// Lgth
	binary.LittleEndian.PutUint32(cksum[:4], 16)

	// Flags
	flags := gssapi.ContextFlagInteg | gssapi.ContextFlagConf
	binary.LittleEndian.PutUint32(cksum[20:24], uint32(flags))

	return types.Checksum{
		CksumType: chksumtype.GSSAPI,
		Checksum:  cksum,
	}
}

func (m *mechanism) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	cl := client.NewWithKeytab(m.principal, m.realm, m.kt, m.cfg)
	defer cl.Destroy()

	if err := cl.Login(); err != nil {
		return nil, nil, err
	}

	ticket, encKey, err := cl.GetServiceTicket(m.spn)
	if err != nil {
		return nil, nil, err
	}

	authenticator, err := types.NewAuthenticator(cl.Credentials.Domain(), cl.Credentials.CName())
	if err != nil {
		return nil, nil, err
	}
	authenticator.Cksum = gssapiCksum()

	req, err := messages.NewAPReq(ticket, encKey, authenticator)
	if err != nil {
		return nil, nil, err
	}

	reqBytes, err := req.Marshal()
	if err != nil {
		return nil, nil, err
	}

	// The first two bytes of oidBytes contain the tag 0x06 and the length of the
	// KRB5 OID 0x09.
	oidBytes, err := asn1.Marshal(gssapi.OIDKRB5.OID())
	if err != nil {
		return nil, nil, err
	}

	length := asn1tools.MarshalLengthBytes(len(oidBytes) + 2 + len(reqBytes))

	// https://tools.ietf.org/html/rfc2743#page-81
	buf := new(bytes.Buffer)
	buf.WriteByte(tokenTag)
	buf.Write(length)
	buf.Write(oidBytes)

	binary.Write(buf, binary.BigEndian, tokenID)
	buf.Write(reqBytes)

	return &session{
		encKey: encKey,
	}, buf.Bytes(), nil
}

type session struct {
	verified bool
	encKey   types.EncryptionKey
}

func (s *session) Next(ctx context.Context, challenge []byte) (bool, []byte, error) {
	if s.verified {
		return true, nil, nil
	}

	req := gssapi.WrapToken{}
	if err := req.Unmarshal(challenge, true); err != nil {
		return false, nil, err
	}

	verified, err := req.Verify(s.encKey, keyusage.GSSAPI_ACCEPTOR_SEAL)
	if !verified {
		return false, nil, err
	}
	s.verified = verified

	resp, err := gssapi.NewInitiatorWrapToken(req.Payload, s.encKey)
	if err != nil {
		return false, nil, err
	}

	bytes, err := resp.Marshal()
	return false, bytes, err
}
