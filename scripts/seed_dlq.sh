#!/usr/bin/env bash
# Publish malformed messages straight to events.raw so the consumer routes
# them to the dead letter queue, one per failure path in consumer/processor.py.
#
# The API cannot produce these: its Pydantic validation rejects every one at
# the door. They are what a buggy producer writing to Kafka directly would
# send, which is exactly the case the DLQ exists for. Useful for demos and for
# trying scripts/replay_dlq.py against real failures.
#
#   ./scripts/seed_dlq.sh            # one of each failure
#   ./scripts/seed_dlq.sh 3          # three of each
#
# Needs the compose stack running. Messages go through the Kafka container's
# own console producer, so nothing has to be installed on the host.

set -euo pipefail

ROUNDS="${1:-1}"
cd "$(dirname "$0")/.."

uuid() { cat /proc/sys/kernel/random/uuid 2>/dev/null || python3 -c 'import uuid; print(uuid.uuid4())'; }

messages() {
  for _ in $(seq "$ROUNDS"); do
    # missing_fields: no event_id at all.
    echo '{"event_type": "order.created", "source": "legacy-importer", "payload": {"order_id": "ORD-LEGACY-1", "amount": 12.5}}'

    # invalid_event_id: present, but an order number rather than a UUID.
    echo '{"event_id": "ORD-7731", "event_type": "order.created", "source": "legacy-importer", "payload": {"order_id": "ORD-7731"}}'

    # retry_exhausted: every field is present and well formed, but event_type
    # is longer than the 50 character column, so Postgres refuses the insert
    # on every attempt and the consumer gives up after its retries. This is
    # the message that found the session rollback bug: before that fix the
    # consumer crash looped on it instead of recording it here.
    echo "{\"event_id\": \"$(uuid)\", \"event_type\": \"inventory.warehouse_stock_level_recalculated_after_audit\", \"source\": \"inventory-service\", \"payload\": {\"sku\": \"SKU-2210\", \"on_hand\": 41}}"
  done
}

count=$(messages | wc -l)
messages | docker compose exec -T kafka \
  kafka-console-producer --bootstrap-server localhost:29092 --topic events.raw

echo "Published $count malformed message(s) to events.raw."
echo "The first two fail at once; the long event_type fails after the consumer's retries (about 7 s)."
echo "Inspect them with: curl -s localhost:8000/dlq | python3 -m json.tool"
