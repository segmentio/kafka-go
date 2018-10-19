package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestSaramaCompatibility(t *testing.T) {
	var (
		// sample data from github.com/Shopify/sarama
		//
		// See consumer_group_members_test.go
		//
		groupMemberMetadata = []byte{
			0, 1, // Version
			0, 0, 0, 2, // Topic array length
			0, 3, 'o', 'n', 'e', // Topic one
			0, 3, 't', 'w', 'o', // Topic two
			0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
		}
		groupMemberAssignment = []byte{
			0, 1, // Version
			0, 0, 0, 1, // Topic array length
			0, 3, 'o', 'n', 'e', // Topic one
			0, 0, 0, 3, // Topic one, partition array length
			0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 4, // 0, 2, 4
			0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
		}
	)

	t.Run("verify metadata", func(t *testing.T) {
		var item groupMetadata
		remain, err := (&item).readFrom(bufio.NewReader(bytes.NewReader(groupMemberMetadata)), len(groupMemberMetadata))
		if err != nil {
			t.Fatalf("bad err: %v", err)
		}
		if remain != 0 {
			t.Fatalf("expected 0; got %v", remain)
		}

		if v := item.Version; v != 1 {
			t.Errorf("expected Version 1; got %v", v)
		}
		if v := item.Topics; !reflect.DeepEqual([]string{"one", "two"}, v) {
			t.Errorf(`expected {"one", "two"}; got %v`, v)
		}
		if v := item.UserData; !reflect.DeepEqual([]byte{0x01, 0x02, 0x03}, v) {
			t.Errorf("expected []byte{0x01, 0x02, 0x03}; got %v", v)
		}
	})

	t.Run("verify assignments", func(t *testing.T) {
		var item groupAssignment
		remain, err := (&item).readFrom(bufio.NewReader(bytes.NewReader(groupMemberAssignment)), len(groupMemberAssignment))
		if err != nil {
			t.Fatalf("bad err: %v", err)
		}
		if remain != 0 {
			t.Fatalf("expected 0; got %v", remain)
		}

		if v := item.Version; v != 1 {
			t.Errorf("expected Version 1; got %v", v)
		}
		if v := item.Topics; !reflect.DeepEqual(map[string][]int32{"one": {0, 2, 4}}, v) {
			t.Errorf(`expected map[string][]int32{"one": {0, 2, 4}}; got %v`, v)
		}
		if v := item.UserData; !reflect.DeepEqual([]byte{0x01, 0x02, 0x03}, v) {
			t.Errorf("expected []byte{0x01, 0x02, 0x03}; got %v", v)
		}
	})
}

func TestMemberMetadata(t *testing.T) {
	item := groupMetadata{
		Version:  1,
		Topics:   []string{"a", "b"},
		UserData: []byte(`blah`),
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found groupMetadata
	remain, err := (&found).readFrom(bufio.NewReader(buf), buf.Len())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if remain != 0 {
		t.Errorf("expected 0 remain, got %v", remain)
		t.FailNow()
	}
	if !reflect.DeepEqual(item, found) {
		t.Error("expected item and found to be the same")
		t.FailNow()
	}
}

func TestJoinGroupResponseV1(t *testing.T) {
	item := joinGroupResponseV1{
		ErrorCode:     2,
		GenerationID:  3,
		GroupProtocol: "a",
		LeaderID:      "b",
		MemberID:      "c",
		Members: []joinGroupResponseMemberV1{
			{
				MemberID:       "d",
				MemberMetadata: []byte("blah"),
			},
		},
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found joinGroupResponseV1
	remain, err := (&found).readFrom(bufio.NewReader(buf), buf.Len())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if remain != 0 {
		t.Errorf("expected 0 remain, got %v", remain)
		t.FailNow()
	}
	if !reflect.DeepEqual(item, found) {
		t.Error("expected item and found to be the same")
		t.FailNow()
	}
}
