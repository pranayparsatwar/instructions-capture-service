param(
  [switch]$NoFollowLogs
)

$ErrorActionPreference = "Stop"

# Remove old kafka container if it exists.
# Ignore failures when container does not exist.
$existingKafka = docker ps -a --filter "name=^kafka$" --format "{{.Names}}"
if ($existingKafka -eq "kafka") {
  docker rm -f kafka | Out-Null
}

$clusterId = docker run --rm apache/kafka:3.9.1 /opt/kafka/bin/kafka-storage.sh random-uuid

docker run -d --name kafka `
  -p 9092:9092 `
  -e CLUSTER_ID=$clusterId `
  -e KAFKA_NODE_ID=1 `
  -e KAFKA_PROCESS_ROLES=broker,controller `
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 `
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 `
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT `
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 `
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER `
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT `
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 `
  apache/kafka:3.9.1

if (-not $NoFollowLogs) {
  docker logs -f kafka
}

