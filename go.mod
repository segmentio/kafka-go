module github.com/segmentio/kafka-go

go 1.18

require (
	github.com/klauspost/compress v1.17.4
	github.com/pierrec/lz4/v4 v4.1.19
	github.com/stretchr/testify v1.8.4
	github.com/xdg-go/scram v1.1.2
	golang.org/x/net v0.19.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	golang.org/x/text v0.14.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract [v0.4.36, v0.4.37]
