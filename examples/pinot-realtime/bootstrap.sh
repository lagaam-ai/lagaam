#!/usr/bin/env bash
# Bootstrap the pinot-realtime profile: four topics, five tables, sample rows.
#
#   docker compose --profile pinot-realtime up -d
#   examples/pinot-realtime/bootstrap.sh
#
# Idempotent: a topic or table that already exists is left alone, so a re-run
# after a partial failure finishes the job rather than doubling the data.
#
# The topics are this profile's own (u12-flights, u12-upsert, u14-multi,
# u14-rep), never the STREAM quickstart's: its flights-realtime is auto-created
# with 10 partitions and its own tables consume from it, so a table pointed
# there gets neither the partition count these tests assume nor a row count
# this script controls.
set -euo pipefail

CONTROLLER="${PINOT_REALTIME_CONTROLLER:-http://localhost:9001}"
KAFKA="${LAGAAM_KAFKA_CONTAINER:-lagaam-kafka}"
PINOT="${LAGAAM_PINOT_REALTIME_CONTAINER:-lagaam-pinot-realtime}"
# The name Pinot reaches Kafka by: the compose service, not the container.
KAFKA_HOST="${LAGAAM_KAFKA_HOST:-kafka}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROWS="${U12_AIRLINE_ROWS:-600}"
UPSERT_ROWS=600
# 1,100 rows at a flush threshold of 100 seal 10 segments and leave one
# consuming per partition — see the feed below for why they land unevenly.
MULTI_ROWS=1100

# A failed run must not leave the feeds — or a response body — behind on the
# host. POST_BODY is set while post_json holds a temp file and cleared after
# it removes one, so the trap never names a path that has been reused.
FEED=""
UP_FEED=""
U14_FEED=""
POST_BODY=""
trap 'rm -f "${FEED-}" "${UP_FEED-}" "${U14_FEED-}" "${POST_BODY-}"' EXIT

wait_for_controller() {
  for _ in $(seq 1 60); do
    if curl -sf "${CONTROLLER}/health" >/dev/null; then return 0; fi
    sleep 2
  done
  echo "controller ${CONTROLLER} never answered /health" >&2
  exit 1
}

# /health answers well before the quickstart's servers have joined, and a
# REALTIME table POSTed at that moment is rejected outright ("No instance
# found with tag: DefaultTenant_REALTIME"). Wait for a server carrying that
# tag — the exact precondition the table POST needs.
realtime_server_count() {
  curl -sf "${CONTROLLER}/tenants/DefaultTenant?type=server" 2>/dev/null \
    | python3 -c 'import sys, json
try:
    payload = json.load(sys.stdin)
except Exception:
    print(0)
else:
    print(len(payload.get("RealtimeServerInstances") or []))' 2>/dev/null \
    || true
}

wait_for_realtime_server() {
  for _ in $(seq 1 90); do
    if [ "$(realtime_server_count)" -gt 0 ]; then return 0; fi
    sleep 2
  done
  echo "no server took the DefaultTenant_REALTIME tag on ${CONTROLLER} within 180s" >&2
  exit 1
}

create_topic() {
  docker exec "${KAFKA}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${KAFKA_HOST}:9092" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1
}

# Only a 404 means absent. A 5xx or a dead controller must not read as "not
# there yet" and send us on to POST a table that may already exist.
resource_exists() {
  local kind="$1" name="$2" code
  if ! code="$(curl -s -o /dev/null -w '%{http_code}' \
                 "${CONTROLLER}/${kind}/${name}")"; then
    echo "GET ${CONTROLLER}/${kind}/${name} failed to connect" >&2
    exit 1
  fi
  case "${code}" in
    2??) return 0 ;;
    404) return 1 ;;
    *)   echo "GET ${CONTROLLER}/${kind}/${name} answered HTTP ${code}" >&2
         exit 1 ;;
  esac
}

table_exists()  { resource_exists tables "$1"; }
schema_exists() { resource_exists schemas "$1"; }

# A POST that loses a race with the STREAM quickstart's own bootstrap comes
# back 409 or with "already exists" in the body; that is success. Anything
# else is a real failure and must not be swallowed.
post_json() {
  local what="$1" url="$2" file="$3" body code
  body="$(mktemp -t u12_post.XXXXXX)"
  POST_BODY="${body}"
  code="$(curl -s -o "${body}" -w '%{http_code}' -X POST "${url}" \
            -H 'Content-Type: application/json' --data-binary "@${file}")" || {
    rm -f "${body}"; POST_BODY=""
    echo "POST ${url} (${what}) failed to connect" >&2
    exit 1
  }
  case "${code}" in
    2??|409) rm -f "${body}"; POST_BODY=""; return 0 ;;
  esac
  if grep -qi 'already exists' "${body}"; then
    rm -f "${body}"; POST_BODY=""
    return 0
  fi
  echo "POST ${url} (${what}) answered HTTP ${code}:" >&2
  cat "${body}" >&2
  rm -f "${body}"; POST_BODY=""
  exit 1
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
    post_json "schema ${name}" "${CONTROLLER}/schemas" "${HERE}/${schema}"
  fi
  post_json "table ${name}" "${CONTROLLER}/tables" "${HERE}/${table}"
  echo "table ${name} created"
}

topic_rows() {
  docker exec "${KAFKA}" /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server "${KAFKA_HOST}:9092" --topic "$1" \
    | awk -F: '{total += $3} END {print total + 0}'
}

# Exactly three outcomes: empty means feed it, full means leave it, and a
# partial topic is a failure. Topping up would put the wrong rows in, and
# producing again would double them — either way the counts the tests assert
# stop meaning anything, so say which topic to delete and stop.
feed_needed() {
  local topic="$1" want="$2" have
  have="$(topic_rows "${topic}")"
  if [ "${have}" -eq 0 ]; then return 0; fi
  if [ "${have}" -ge "${want}" ]; then
    echo "topic ${topic} already carries ${have} rows — left alone"
    return 1
  fi
  echo "topic ${topic} holds ${have} rows, expected 0 or ${want}." >&2
  echo "Delete it and re-run:" >&2
  echo "  docker exec ${KAFKA} /opt/kafka/bin/kafka-topics.sh \\" >&2
  echo "    --bootstrap-server ${KAFKA_HOST}:9092 --delete --topic ${topic}" >&2
  exit 1
}

wait_for_controller
wait_for_realtime_server
create_topic u12-flights 1
# One partition, not two. An upsert table needs its stream partitioned by the
# primary key, and one partition satisfies that trivially — and it keeps both
# u12 tables on a single server, which is what makes them the control: they
# are complete on the bulk metadata call and prove the fan-out costs a
# single-server table nothing. The multi-server case is u14multi/u14rep below.
create_topic u12-upsert 1

# Two partitions and no replica-group pin, which is the point: the quickstart
# runs four servers, so these two tables' sealed segments land on more than
# one of them. GET /segments/{table}/metadata then answers for a single server
# while /tables/{table}/size names them all, and the adapter has to go and
# fetch the rest from the servers that hold them (ADR 0011). u14rep repeats it
# at replication 2, where every segment sits on exactly two servers.
create_topic u14-multi 2
create_topic u14-rep 2

post_table airlineStats airlineStats-schema.json airlineStats-table.json
post_table u12upsert    u12upsert-schema.json    u12upsert-table.json
post_table u12plain     u12plain-schema.json     u12plain-table.json
post_table u14multi     u14multi-schema.json     u14multi-table.json
post_table u14rep       u14rep-schema.json       u14rep-table.json

# The flight feed: the image's own sample data, capped so the table seals a
# predictable number of 100-row segments and still leaves one consuming.
if feed_needed u12-flights "${ROWS}"; then
  docker exec "${PINOT}" bash -lc \
    "head -${ROWS} /opt/pinot/examples/stream/airlineStats/rawdata/airlineStats_data.json \
       > /tmp/u12_feed.json"
  FEED="$(mktemp -t u12_feed.XXXXXX)"
  docker cp "${PINOT}:/tmp/u12_feed.json" "${FEED}"
  # docker cp preserves the mode, and Kafka's entrypoint runs as appuser:
  # mktemp's 0600 would reach the container unreadable.
  chmod 644 "${FEED}"
  docker cp "${FEED}" "${KAFKA}:/tmp/u12_feed.json"
  rm -f "${FEED}"
  FEED=""
  docker exec "${KAFKA}" bash -lc \
    "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server ${KAFKA_HOST}:9092 \
       --topic u12-flights < /tmp/u12_feed.json"
  echo "produced ${ROWS} rows to u12-flights"
fi

# The upsert feed: 100 distinct keys x 6 versions, keyed by pk so every
# version of a key lands in the same partition and the upsert view is the
# last version of each. The topic has one partition, so that is automatic —
# the key is still produced, because it is what upsert partitioning requires.
if feed_needed u12-upsert "${UPSERT_ROWS}"; then
  UP_FEED="$(mktemp -t u12up_feed.XXXXXX)"
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
  UP_FEED=""
  docker exec "${KAFKA}" bash -lc \
    "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server ${KAFKA_HOST}:9092 \
       --topic u12-upsert --property parse.key=true --property key.separator=\$'\t' \
       < /tmp/u12up_feed.txt"
  echo "produced ${UPSERT_ROWS} rows (100 keys x 6 versions) to u12-upsert"
fi

# The multi-server feed, produced to both u14 topics so the two tables hold
# byte-identical data and differ only in replication. 1,100 distinct pks, so
# neither table has a key to prove: these tables exist to be *sized* across
# servers, not to carry key evidence.
#
# The two-then-ten key shape is what makes the split uneven, and it is
# reproducible rather than lucky: Kafka's default partitioner is murmur2 of
# the key, k0 and k1 both hash to partition 1, and x0..x9 split 5/5. So the
# first 500 rows land together and the last 600 spread, giving 360 rows on
# partition 0 and 740 on partition 1 — confirmed against both the running
# topic's offsets and murmur2 computed independently. At 100 rows a segment
# that is 3 sealed on one server and 7 on the other, one consuming each.
u14_feed() {
  U14_FEED="$(mktemp -t u14_feed.XXXXXX)"
  python3 - "${MULTI_ROWS}" > "${U14_FEED}" <<'PY'
import json, sys, time
total = int(sys.argv[1])
now = int(time.time() * 1000)
for index in range(total):
    # Two keys for the first 500 rows, ten for the rest.
    key = f"k{index % 2}" if index < 500 else f"x{index % 10}"
    width = 3 if index < 500 else 4
    row = {
        "pk": f"key{index:0{width}d}",
        "label": f"v{index % 6}",
        "val": index,
        "ts": now + index,
    }
    print(f"{key}\t{json.dumps(row)}")
PY
  chmod 644 "${U14_FEED}"
  docker cp "${U14_FEED}" "${KAFKA}:/tmp/u14_feed.txt"
  rm -f "${U14_FEED}"
  U14_FEED=""
}

produce_u14() {
  local topic="$1"
  docker exec "${KAFKA}" bash -lc \
    "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server ${KAFKA_HOST}:9092 \
       --topic ${topic} --property parse.key=true --property key.separator=\$'\t' \
       < /tmp/u14_feed.txt"
  echo "produced ${MULTI_ROWS} rows to ${topic}"
}

# Built at most once, and only where a topic actually needs it.
for topic in u14-multi u14-rep; do
  if feed_needed "${topic}" "${MULTI_ROWS}"; then
    if [ -z "${U14_FEED_BUILT-}" ]; then
      u14_feed
      U14_FEED_BUILT=1
    fi
    produce_u14 "${topic}"
  fi
done

echo "bootstrapped — controller ${CONTROLLER}, broker http://localhost:8001"
