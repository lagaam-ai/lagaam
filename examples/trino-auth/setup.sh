#!/usr/bin/env bash
# Make the self-signed cert the trino-auth profile serves. Then:
#   docker compose -f examples/docker-compose.yml --profile trino-auth up -d
# Login: user lagaam, password lagaam-test-password, on https://localhost:8443.
# password.db holds that password's bcrypt hash ($2y$ is the prefix Trino reads).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p tls
if [[ ! -f tls/trino.pem ]]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
    -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    -keyout tls/key.pem -out tls/cert.pem 2>/dev/null
  # Trino wants key and cert in one PEM; clients verify against cert.pem alone.
  cat tls/key.pem tls/cert.pem > tls/trino.pem
  chmod 644 tls/*.pem
fi
echo "TRINO_VERIFY=$(pwd)/tls/cert.pem"
