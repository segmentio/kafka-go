module github.com/segmentio/kafka-go

go 1.15

require (
	github.com/klauspost/compress v1.15.9
	github.com/pierrec/lz4/v4 v4.1.15
	github.com/stretchr/testify v1.8.0
	github.com/xdg-go/scram v1.1.2
	golang.org/x/net v0.17.0
)

retract [v0.4.36, v0.4.37]
