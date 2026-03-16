# instructions-capture-service

Reactive Spring Boot service to capture trade instructions, validate/transform them, and publish processed output to Kafka.

## Pre-requisites

- Java 21
- Maven (or use the included Maven Wrapper)
- Docker (for local Kafka and/or app image execution)

### Kafka for local development

Kafka should be running in Docker for local process execution.

- For local app run, use Spring profile: `localstack`
- For app run as Docker image with Kafka in Docker network, use Spring profile: `dockerstack`

PowerShell scripts are available at the project root to simplify local Kafka lifecycle:

- Start Kafka: `start-kafka-local.ps1`
- Stop Kafka: `stop-kafka-local.ps1`

### Remote Kafka setup

If connecting to a remote Kafka cluster, update:

- `src/main/resources/application-remotestack.yaml`

Set the required Kafka connection details (for example, bootstrap servers and any additional security/auth configuration your environment requires), then run with profile `remotestack`.

## Spring profiles

- `localstack` -> Kafka at `localhost:9092`
- `dockerstack` -> Kafka at `kafka:9092`
- `remotestack` -> user-managed remote Kafka settings

## Data validations

1. **Account Number**
   - Must contain only digits
   - Must be exactly 8 characters
2. **Trade Type**
   - Case-insensitive allowed values: `BUY`, `SELL`, `B`, `S`

## Process documentation

OpenAPI documentation is available at:

- Swagger UI: http://localhost:8080/swagger-ui/index.html
- API Docs (OpenAPI JSON): http://localhost:8080/v3/api-docs

## Build and run

### Run locally

```powershell
Set-Location "C:\workspace\instructions-capture-service"
.\start-kafka-local.ps1
.\mvnw.cmd spring-boot:run -Dspring-boot.run.profiles=localstack
```

### Build JAR

```powershell
Set-Location "C:\workspace\instructions-capture-service"
.\mvnw.cmd clean package
```

### Build and run Docker image

```powershell
Set-Location "C:\workspace\instructions-capture-service"
docker build -t instructions-capture-service:latest .
docker run --rm -p 8080:8080 -e SPRING_PROFILES_ACTIVE=dockerstack instructions-capture-service:latest
```

## Testing

1. Test data is available under `./test-data`.
2. APIs can be tested using Swagger UI: http://localhost:8080/swagger-ui/index.html

Run automated tests:

```powershell
Set-Location "C:\workspace\instructions-capture-service"
.\mvnw.cmd test
```

## Useful test-data script

A helper script is available to publish a sample inbound trade message:

- `test-data/kafka-inbound-topic-msg.ps1`

Run it:

```powershell
Set-Location "C:\workspace\instructions-capture-service"
.\test-data\kafka-inbound-topic-msg.ps1
```

## NOTE:

The code is intentionally simple and focused on demonstrating the core functionality of capturing, validating, transforming, and publishing trade instructions. In a production environment, additional considerations such as error handling, security, monitoring, and scalability would need to be addressed.