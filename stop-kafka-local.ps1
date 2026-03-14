$ErrorActionPreference = "Stop"

# Stop and remove the local kafka container if it exists.
# Ignore failures when container does not exist.
try {
  docker rm -f kafka | Out-Null
  Write-Host "Kafka container removed."
} catch {
  Write-Host "No kafka container found (or remove failed)."
}

