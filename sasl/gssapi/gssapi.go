package gssapi

import (
	"bytes"
	"context"
	"encoding/asn1"
	"encoding/binary"

	"github.com/jcmturner/gokrb5/asn1tools"
	"github.com/jcmturner/gokrb5/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/client"
	krbconf "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/chksumtype"
	"github.com/jcmturner/gokrb5/v8/iana/etypeID"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/segmentio/kafka-go/sasl"
)

// todo : add'l config
type Config struct {
	Realm    string
	Username string
	Password string // todo : also support keytab
	Service  string
	KDCAddr  string
}

const (
	// todo : why these flags?
	checksumFlags = gssapi.ContextFlagInteg | gssapi.ContextFlagConf
)

type mechanism struct {
	conf Config
	kc   *krbconf.Config
}

type stateMachine struct {
	client   *client.Client
	key      types.EncryptionKey
	verified bool
}

func Mechanism(config Config) (sasl.Mechanism, error) {
	// todo : validate config.

	kc := krbconf.New()
	kc.LibDefaults.DefaultRealm = config.Realm
	// todo : figure out which settings are required and fix.
	encTypes := []int32{etypeID.AES256_CTS_HMAC_SHA1_96, etypeID.AES128_CTS_HMAC_SHA1_96}
	kc.LibDefaults.PermittedEnctypeIDs = encTypes
	kc.LibDefaults.DefaultTGSEnctypeIDs = encTypes
	kc.LibDefaults.DefaultTktEnctypeIDs = encTypes
	kc.Realms = []krbconf.Realm{{
		Realm: config.Realm,
		KDC:   []string{config.KDCAddr}, // todo : multiple?
	}}

	return mechanism{conf: config, kc: kc}, nil
}

func (m mechanism) Name() string {
	return "GSSAPI"
}

func (m mechanism) Start(ctx context.Context) (sess sasl.StateMachine, ir []byte, err error) {
	cl := client.NewWithPassword(
		m.conf.Username,
		m.conf.Realm,
		m.conf.Password,
		m.kc,
	)

	if err := cl.Login(); err != nil {
		return nil, nil, err
	}

	ticket, sessKey, err := cl.GetServiceTicket(m.conf.Service)
	if err != nil {
		cl.Destroy()
		return nil, nil, err
	}

	// all of the interactions with the KDC are complete.  now on to
	// authentication with the broker.
	s := stateMachine{
		client: cl,
		key:    sessKey,
	}

	// if unable to create the initial request, then release resources.
	b, err := initialRequest(cl, ticket, sessKey)
	if err != nil {
		s.Close()
		return nil, nil, err
	}

	return &s, b, nil
}

func (s *stateMachine) Next(_ context.Context, challenge []byte) (done bool, response []byte, err error) {
	// if the challenge/response cycle has completed, then return that we're done.
	if s.verified {
		return true, nil, nil
	}
	// otherwise, parse the response and check validity.
	s.verified, response, err = verify(challenge, s.key)
	return false, response, err
}

func (s *stateMachine) Close() error {
	if s.client != nil {
		s.client.Destroy()
	}
	return nil
}

func initialRequest(client *client.Client, ticket messages.Ticket, sessKey types.EncryptionKey) ([]byte, error) {
	auth, err := types.NewAuthenticator(client.Credentials.Domain(), client.Credentials.CName())
	if err != nil {
		return nil, err
	}

	// calculate the authenticator checksum.
	// bytes [4,20) are used for channel binding information and so are left
	// blank.  see https://tools.ietf.org/html/rfc4121#section-4.1.1
	checksum := make([]byte, 24)
	binary.LittleEndian.PutUint32(checksum, 16 /*constant value per RFC*/)
	binary.LittleEndian.PutUint32(checksum[20:], uint32(checksumFlags))
	auth.Cksum = types.Checksum{
		CksumType: chksumtype.GSSAPI,
		Checksum:  checksum,
	}

	req, err := messages.NewAPReq(ticket, sessKey, auth)
	if err != nil {
		return nil, err
	}
	reqBytes, err := req.Marshal()
	if err != nil {
		return nil, err
	}

	oidBytes, err := asn1.Marshal(gssapi.OIDKRB5)
	if err != nil {
		return nil, err
	}

	// work backward to calculate the size of the packet.  the final packet will
	// be laid out as:
	//
	// header:
	// 1 byte  - sequence byte (always 0x60)
	// 1-2 bytes - variable int length of token
	//
	// token:
	// variable - OID bytes
	// 2 bytes - gssapi tag (always 256)
	// variable - ap req
	//
	// see https://tools.ietf.org/html/rfc2743#section-3.1
	sz := len(reqBytes)
	sz += 2
	sz += len(oidBytes)
	tokenSz := asn1tools.MarshalLengthBytes(sz)
	sz += len(tokenSz)
	sz += 1

	// then write out the packet in order.
	out := bytes.NewBuffer(make([]byte, 0, sz))
	out.WriteByte(0x60)                              // gssapi tag
	out.Write(tokenSz)                               // varint length of token
	out.Write(oidBytes)                              // oid bytes
	binary.Write(out, binary.BigEndian, uint16(256)) // gssapi token type
	out.Write(reqBytes)

	return out.Bytes(), nil
}

func verify(challenge []byte, key types.EncryptionKey) (bool, []byte, error) {
	req := gssapi.WrapToken{}
	if err := req.Unmarshal(challenge, true); err != nil {
		return false, nil, err
	}
	valid, err := req.Verify(key, keyusage.GSSAPI_ACCEPTOR_SEAL)
	if !valid {
		return false, nil, err
	}

	resp, err := gssapi.NewInitiatorWrapToken(req.Payload, key)
	if err != nil {
		return false, nil, err
	}

	respBytes, err := resp.Marshal()
	if err != nil {
		return false, nil, err
	}

	return true, respBytes, nil
}
