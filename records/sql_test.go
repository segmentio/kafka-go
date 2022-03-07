package records_test

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/segmentio/kafka-go/records"
)

func TestIndexDB(t *testing.T) {
	testIndex(t, func() (records.Index, func(), error) {
		index, err := records.OpenIndex("sqlite3", "file:test.db?cache=shared&mode=memory")
		if err != nil {
			return nil, nil, err
		}
		return index, func() { index.Close() }, nil
	})
}
