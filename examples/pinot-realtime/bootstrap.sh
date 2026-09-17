#!/usr/bin/env bash
# Bootstrap the pinot-realtime profile: two topics, three tables, sample rows.
#
#   docker compose --profile pinot-realtime up -d
#   examples/pinot-realtime/bootstrap.sh
#
# Idempotent: a topic or table that already exists is left alone, so a re-run
# after a partial failure finishes the job rather than doubling the data.
set -euo pipefail

CONTROLLER="${PINOT_REALTIME_CONTROLLER:-http://localhost:9001}"
KAFKA="${LAGAAM_KAFKA_CONTAINER:-lagaam-kafka}"
PINOT="${LAGAAM_PINOT_REALTIME_CONTAINER:-lagaam-pinot-realtime}"
# The name Pinot reaches Kafka by: the compose service, not the container.
KAFKA_HOST="${LAGAAM_KAFKA_HOST:-kafka}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROWS="${U12_AIRLINE_ROWS:-600}"

wait_for_controller() {
  for _ in $(seq 1 60); do
    if curl -sf "${CONTROLLER}/health" >/dev/null; then return 0; fi
    sleep 2
  done
  echo "controller ${CONTROLLER} never answered /health" >&2
  exit 1
}

create_topic() {
  docker exec "${KAFKA}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${KAFKA_HOST}:9092" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1
}

table_exists() {
  curl -sf "${CONTROLLER}/tables/$1" | grep -q '"tableName"'
}

schema_exists() {
  curl -sf -o /dev/null "${CONTROLLER}/schemas/$1"
}

post_table() {
  local name="$1" schema="$2" table="$3"
  if table_exists "${name}"; then
    echo "table ${name} exists — left alone"
    return 0
  fi
  # The STREAM quickstart posts its own airlineStats schema while this runs,
  # so an already-present schema is a race, not an error.
  if schema_exists "${name}"; then
    echo "schema ${name} exists — left alone"
  else
    curl -sf -X POST "${CONTROLLER}/schemas" \
      -H 'Content-Type: application/json' --data-binary "@${HERE}/${schema}" >/dev/null
  fi
  curl -sf -X POST "${CONTROLLER}/tables" \
    -H 'Content-Type: application/json' --data-binary "@${HERE}/${table}" >/dev/null
  echo "table ${name} created"
}

# The rows already in a topic, so a re-run tops it up to ROWS rather than
# appending another ROWS on top of them.
topic_rows() {
  docker exec "${KAFKA}" /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server "${KAFKA_HOST}:9092" --topic "$1" \
    | awk -F: '{total += $3} END {print total + 0}'
}

wait_for_controller
create_topic flights-realtime 1
create_topic u12-upsert 2

post_table airlineStats airlineStats-schema.json airlineStats-table.json
post_table u12upsert    u12upsert-schema.json    u12upsert-table.json
post_table u12plain     u12plain-schema.json     u12plain-table.json

# The flight feed: the image's own sample data, capped so the table seals a
# predictable number of 100-row segments and still leaves one consuming.
if [ "$(topic_rows flights-realtime)" -ge "${ROWS}" ]; then
  echo "topic flights-realtime already carries ${ROWS}+ rows — left alone"
else
  docker exec "${PINOT}" bash -lc \
    "head -${ROWS} /opt/pinot/examples/stream/airlineStats/rawdata/airlineStats_data.json \
       > /tmp/u12_feed.json"
  FEED="$(mktemp -t u12_feed)"
  docker cp "${PINOT}:/tmp/u12_feed.json" "${FEED}"
  # docker cp preserves the mode, and Kafka's entrypoint runs as appuser:
  # mktemp's 0600 would reach the container unreadable.
  chmod 644 "${FEED}"
  docker cp "${FEED}" "${KAFKA}:/tmp/u12_feed.json"
  rm -f "${FEED}"
  docker exec "${KAFKA}" bash -lc \
    "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server ${KAFKA_HOST}:9092 \
       --topic flights-realtime < /tmp/u12_feed.json"
  echo "produced ${ROWS} rows to flights-realtime"
fi

# The upsert feed: 100 distinct keys x 6 versions, keyed by pk so both
# partitions get whole keys and the upsert view is the last version of each.
if [ "$(topic_rows u12-upsert)" -ge 600 ]; then
  echo "topic u12-upsert already carries 600+ rows — left alone"
else
  UP_FEED="$(mktemp -t u12up_feed)"
  python3 - > "${UP_FEED}" <<'PY'
import json, time
now = int(time.time() * 1000)
for version in range(6):
    for key in range(100):
        pk = f"key{key:03d}"
        row = {"pk": pk, "label": f"v{version}", "val": version, "ts": now + version}
        print(f"{pk}\t{json.dumps(row)}")
PY
  chmod 644 "${UP_FEED}"
  docker cp "${UP_FEED}" "${KAFKA}:/tmp/u12up_feed.txt"
  rm -f "${UP_FEED}"
  docker exec "${KAFKA}" bash -lc \
    "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server ${KAFKA_HOST}:9092 \
       --topic u12-upsert --property parse.key=true --property key.separator=\$'\t' \
       < /tmp/u12up_feed.txt"
  echo "produced 600 rows (100 keys x 6 versions) to u12-upsert"
fi

echo "bootstrapped — controller ${CONTROLLER}, broker http://localhost:8001"
