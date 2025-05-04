package consumer

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/segmentio/kafka-go/protocol"
)

const MaxVersionSupported = 3

type Subscription struct {
	Version         int16            `kafka:"min=v0,max=v3"`
	Topics          []string         `kafka:"min=v0,max=v3"`
	UserData        []byte           `kafka:"min=v0,max=v3,nullable"`
	OwnedPartitions []TopicPartition `kafka:"min=v1,max=v3"`
	GenerationID    int32            `kafka:"min=v2,max=v3"`
	RackID          string           `kafka:"min=v3,max=v3,nullable"`
}

type subscriptionBackwardsCompat struct {
	Version  int16    `kafka:"min=v0,max=v1"`
	Topics   []string `kafka:"min=v0,max=v1"`
	UserData []byte   `kafka:"min=v0,max=v1,nullable"`
}

func (s *subscriptionBackwardsCompat) FromBytes(b []byte) error {
	// This type is only intended to maintain backwards compatibility with
	// this library and support other clients in the wild sending
	// version 1 supscription data without OwnedPartitionsy
	return protocol.Unmarshal(b, 1, s)
}

func (s *Subscription) FromBytes(b []byte) error {
	if len(b) < 2 {
		return io.ErrUnexpectedEOF
	}
	version := readInt16(b[0:2])
	err := protocol.Unmarshal(b, version, s)
	if err != nil && version >= 1 && errors.Is(err, io.ErrUnexpectedEOF) {
		var sub subscriptionBackwardsCompat
		if err = sub.FromBytes(b); err != nil {
			return err
		}
		s.Version = sub.Version
		s.Topics = sub.Topics
		s.UserData = sub.UserData
		return nil

	}

	return err
}

func (s *Subscription) Bytes() ([]byte, error) {
	return protocol.Marshal(s.Version, *s)
}

type Assignment struct {
	Version            int16            `kafka:"min=v0,max=v3"`
	AssignedPartitions []TopicPartition `kafka:"min=v0,max=v3"`
	UserData           []byte           `kafka:"min=v0,max=v3,nullable"`
}

func (a *Assignment) FromBytes(b []byte) error {
	if len(b) < 2 {
		return io.ErrUnexpectedEOF
	}
	version := readInt16(b[0:2])
	return protocol.Unmarshal(b, version, a)
}

func (a *Assignment) Bytes() ([]byte, error) {
	return protocol.Marshal(a.Version, *a)
}

type TopicPartition struct {
	Topic      string  `kafka:"min=v0,max=v3"`
	Partitions []int32 `kafka:"min=v0,max=v3"`
}

func readInt16(b []byte) int16 {
	return int16(binary.BigEndian.Uint16(b))
}
