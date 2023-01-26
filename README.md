## Build Project

./mvnw clean install -DskipTests=true

## Download KAKFA

https://downloads.apache.org/kafka/3.3.2/kafka-3.3.2-src.tgz

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

## List topics with in kafka

bin/kafka-topics.sh --bootstrap-server=localhost:9092 --list

## Topic Details

bin/kafka-topics.sh --bootstrap-server=localhost:9092 --describe --topic my-kafka-topic