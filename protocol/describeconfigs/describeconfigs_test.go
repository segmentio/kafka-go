package describeconfigs

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/stretchr/testify/require"
)

func TestResponse_Merge(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		r := &Response{}

		r1 := &Response{
			Resources: []ResponseResource{
				{ResourceName: "r1"},
			},
		}
		r2 := &Response{
			Resources: []ResponseResource{
				{ResourceName: "r2"},
			},
		}

		got, err := r.Merge([]protocol.Message{&Request{}}, []interface{}{r1, r2})
		if err != nil {
			t.Fatal(err)
		}

		want := &Response{
			Resources: []ResponseResource{
				{ResourceName: "r1"},
				{ResourceName: "r2"},
			},
		}

		if !reflect.DeepEqual(want, got) {
			t.Fatalf("wanted response: \n%+v, got \n%+v", want, got)
		}
	})

	t.Run("with errors", func(t *testing.T) {
		r := &Response{}

		r1 := &Response{
			Resources: []ResponseResource{
				{ResourceName: "r1"},
			},
		}

		_, err := r.Merge([]protocol.Message{&Request{}}, []interface{}{r1, io.EOF})
		if !errors.Is(err, io.EOF) {
			t.Fatalf("wanted err io.EOF, got %v", err)
		}
	})

	t.Run("panic with unexpected type", func(t *testing.T) {
		defer func() {
			msg := recover()
			require.Equal(t, "BUG: result must be a message or an error but not string", fmt.Sprintf("%s", msg))
		}()
		r := &Response{}

		r1 := &Response{
			Resources: []ResponseResource{
				{ResourceName: "r1"},
			},
		}

		_, _ = r.Merge([]protocol.Message{&Request{}}, []interface{}{r1, "how did a string got here"})
		t.Fatal("did not panic")
	})
}
