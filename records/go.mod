module github.com/segmentio/kafka-go/records

go 1.18

require (
	github.com/mattn/go-sqlite3 v1.14.12
	github.com/segmentio/datastructures/v2 v2.6.0
	github.com/segmentio/kafka-go v0.4.29
)

require (
	github.com/klauspost/compress v1.14.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.14 // indirect
	golang.org/x/net v0.0.0-20201021035429-f5854403a974 // indirect
)

replace github.com/segmentio/kafka-go => ../
