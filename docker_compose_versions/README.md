# Bitnami Kafka

This document outlines how to create a docker-compose file for a specific Bitnami Kafka version.


## Steps to create docker-compose

- Refer to [docker-hub Bitnami Kafka tags](https://hub.docker.com/r/bitnami/kafka/tags) and sort by NEWEST to locate the image preferred, for example: `2.7.0`
- There is documentation in the (main branch)[https://github.com/bitnami/containers/blob/main/bitnami/kafka/README.md] for environment config setup information. Refer to the `Notable Changes` section.
- Sometimes there is a need to understand how the set up is being done. To locate the appropriate Kafka release in the repo [bitnami/containers](https://github.com/bitnami/containers), go through the [kafka commit history](https://github.com/bitnami/containers/commits/main/bitnami/kafka).
- Once a commit is located, Refer to README.md, Dockerfile, entrypoint and various init scripts to understand the environment variables to config server.properties mapping conventions. Alternatively, you can spin up the required Kafka image and refer the mapping inside the container.
- Ensure you follow the environment variable conventions in your docker-compose. Without proper environment variables, the Kafka cluster cannot start or can start with undesired configs. For example, Since Kafka version 2.3, all server.properties docker-compose environment configs start with `KAFKA_CFG_<config_with_underscore>`
- Older versions of Bitnami Kafka have different conventions and limited docker-compose environment variables exposed for configs needed in server.properties


In kafka-go, for all the test cases to succeed, Kafka cluster should have following server.properties along with a relevant kafka_jaas.conf mentioned in the KAFKA_OPTS. Goal is to ensure that the docker-compose file generates below server.properties.


server.properties
```
advertised.host.name=localhost
advertised.listeners=PLAINTEXT://localhost:9092,SASL_PLAINTEXT://localhost:9093
advertised.port=9092
auto.create.topics.enable=true
broker.id=1
delete.topic.enable=true
group.initial.rebalance.delay.ms=0
listeners=PLAINTEXT://:9092,SASL_PLAINTEXT://:9093
log.dirs=/kafka/kafka-logs-1d5951569d78
log.retention.check.interval.ms=300000
log.retention.hours=168
log.segment.bytes=1073741824
message.max.bytes=200000000
num.io.threads=8
num.network.threads=3
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
port=9092
sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
socket.send.buffer.bytes=102400
transaction.state.log.min.isr=1
transaction.state.log.replication.factor=1
zookeeper.connect=zookeeper:2181
zookeeper.connection.timeout.ms=6000
```


## run docker-compose and test cases

run docker-compose
```
# docker-compose -f ./docker_compose_versions/docker-compose-<kafka_version>.yml up -d
```


run test cases
```
# go clean -cache; KAFKA_SKIP_NETTEST=1 KAFKA_VERSION=<a.b.c> go test -race -cover ./...;
```


## Various Bitnami Kafka version issues observed in circleci


### Kafka v101, v111, v201, v211 and v221


In kafka-go repo, all the tests require sasl.enabled.mechanisms as PLAIN,SCRAM-SHA-256,SCRAM-SHA-512 for the Kafka cluster.


It has been observed for Kafka v101, v111, v201, v211 and v221 which are used in the circleci for build have issues with SCRAM.


There is no way to override the config sasl.enabled.mechanisms causing Kafka cluster to start up as PLAIN.


There has been some attempts made to override sasl.enabled.mechanisms 
- Modified entrypoint in docker-compose to append the server.properties with relevant configs sasl.enabled.mechanisms before running entrypoint.sh. This resulted in failures for Kafka v101, v111, v201, v211 and v221. Once Kafka server starts, server.properties gets appended with default value of sasl.enabled.mechanisms  there by cluster to start with out PLAIN,SCRAM-SHA-256,SCRAM-SHA-512
- Mounted a docker-compose volume for server.propeties. However, This also resulted in failures for Kafka v101, v111, v201, v211 and v221. Once Kafka server starts, server.properties gets appended with default value of sasl.enabled.mechanisms there by cluster to start with out PLAIN,SCRAM-SHA-256,SCRAM-SHA-512


NOTE: 
- Kafka v101, v111, v201, v211 and v221 have no docker-compose files since we need SCRAM for kafka-go test cases to succeed. 
- There is no Bitnami Kafka image for v222 hence testing has been performed on v221


### Kafka v231

In Bitnami Kafka v2.3, all server.properties docker-compose environment configs start with `KAFKA_CFG_<config_with_underscore>`. However, it is not picking the custom populated kafka_jaas.conf.


After a lot of debugging, it has been noticed that there aren't enough privileges to create the kafka_jaas.conf. Hence the environment variables below need to be added in docker-compose to generate the kafka_jaas.conf. This issue is not noticed after kafka v2.3


```
KAFKA_INTER_BROKER_USER: adminplain
KAFKA_INTER_BROKER_PASSWORD: admin-secret
KAFKA_BROKER_USER: adminplain
KAFKA_BROKER_PASSWORD: admin-secret
```

There is a docker-compose file `docker-compose-231.yml` in the folder `kafka-go/docker_compose_versions` for reference.


## References


For user reference, please find the some of the older kafka versions commits from the [kafka commit history](https://github.com/bitnami/containers/commits/main/bitnami/kafka). For Kafka versions with no commit history, data is populated with the latest version available for the tag.


### Kafka v010: docker-compose reference: `kafka-go/docker_compose_versions/docker-compose-010.yml`
- [tag](https://hub.docker.com/r/bitnami/kafka/tags?page=1&ordering=last_updated&name=0.10.2.1)
- [kafka commit](https://github.com/bitnami/containers/tree/c4240f0525916a418245c7ef46d9534a7a212c92/bitnami/kafka)


### Kafka v011: docker-compose reference: `kafka-go/docker_compose_versions/docker-compose-011.yml`
- [tag](https://hub.docker.com/r/bitnami/kafka/tags?page=1&ordering=last_updated&name=0.11.0)
- [kafka commit](https://github.com/bitnami/containers/tree/7724adf655e4ca9aac69d606d41ad329ef31eeca/bitnami/kafka)


### Kafka v101: docker-compose reference: N/A
- [tag](https://hub.docker.com/r/bitnami/kafka/tags?page=1&ordering=last_updated&name=1.0.1)
- [kafka commit](https://github.com/bitnami/containers/tree/44cc8f4c43ead6edebd3758c8df878f4f9da82c2/bitnami/kafka)


### Kafka v111: docker-compose reference: N/A
- [tag](https://hub.docker.com/r/bitnami/kafka/tags?page=1&ordering=last_updated&name=1.1.1)
- [kafka commit](https://github.com/bitnami/containers/tree/cb593dc98c2eb7a39f2792641e741d395dbe50e7/bitnami/kafka)


### Kafka v201: docker-compose reference: N/A
- [tag](https://hub.docker.com/r/bitnami/kafka/tags?page=1&ordering=last_updated&name=2.0.1)
- [kafka commit](https://github.com/bitnami/containers/tree/9ff8763df265c87c8b59f8d7ff0cf69299d636c9/bitnami/kafka)


### Kafka v211: docker-compose reference: N/A
- [tag](https://hub.docker.com/r/bitnami/kafka/tags?page=1&ordering=last_updated&name=2.1.1)
- [kafka commit](https://github.com/bitnami/containers/tree/d3a9d40afc2b7e7de53486538a63084c1a565d43/bitnami/kafka)


### Kafka v221: docker-compose reference: N/A
- [tag](https://hub.docker.com/r/bitnami/kafka/tags?page=1&ordering=last_updated&name=2.2.1)
- [kafka commit](https://github.com/bitnami/containers/tree/f132ef830d1ba9b78392ec4619174b4640c276c9/bitnami/kafka)


### Kafka v231: docker-compose reference: `kafka-go/docker_compose_versions/docker-compose-231.yml`
- [tag](https://hub.docker.com/r/bitnami/kafka/tags?page=1&ordering=last_updated&name=2.3.1)
- [kafka commit](https://github.com/bitnami/containers/tree/ae572036b5281456b0086345fec0bdb74f7cf3a3/bitnami/kafka)

