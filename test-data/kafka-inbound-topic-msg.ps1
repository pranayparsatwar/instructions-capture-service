$ErrorActionPreference = "Stop"

# Publishes a sample inbound trade message to the local Kafka container/topic.
'{"account":"87654321","security":"AAP456","type":"SELL","amount":50}' |
  docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic instructions.inbound

