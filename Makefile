test:
	KAFKA_SKIP_NETTEST=1 \
	KAFKA_VERSION=2.3.1 \
	go test -v -race -cover -count=1 ./...

docker:
	docker compose up -d
