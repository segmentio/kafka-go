test:
	KAFKA_SKIP_NETTEST=1 \
	KAFKA_VERSION=0.11.0.1 \
	go test -race -cover ./...

docker:
	docker-compose up -d
