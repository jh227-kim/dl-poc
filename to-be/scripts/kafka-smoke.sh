#!/bin/bash

# Kafka smoke: create mail.events -> produce 1 JSON -> consume 1 message
# Run from repo root: ./to-be/scripts/kafka-smoke.sh
# Prerequisite: cd to-be && docker compose up -d

# 에러 발생 시 즉시 스크립트 종료
set -e

# 현재 스크립트의 디렉토리를 기준으로 부모 디렉토리(to-be) 감지
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TO_BE_ROOT="$(dirname "$SCRIPT_DIR")"

if [ ! -f "$TO_BE_ROOT/docker-compose.yml" ]; then
    echo "Error: docker-compose.yml not found (expected to-be/ next to scripts/)." >&2
    exit 1
fi

cd "$TO_BE_ROOT"

BOOTSTRAP="kafka:29092"
TOPIC="mail.events"

echo "==> Working directory: $TO_BE_ROOT"

echo "==> Delete topic if exists (then recreate so first consume is predictable)"
# 실패해도 스크립트가 종료되지 않도록 잠시 set +e 설정
set +e
docker compose exec -T kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" >/dev/null 2>&1
set -e
sleep 4

echo "==> Create topic: $TOPIC"
docker compose exec -T kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1

echo "==> List topics"
docker compose exec -T kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --list

# 8자리 랜덤 문자열 생성 (PowerShell의 GUID Substring 대응)
MID=$(cat /proc/sys/kernel/random/uuid | tr -d '-' | cut -c1-8)
PAYLOAD="{\"mail_id\":\"$MID\",\"action\":\"created\"}"

echo "==> Produce: $PAYLOAD"
echo "$PAYLOAD" | docker compose exec -T kafka kafka-console-producer --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"

echo "==> Consume (first message)"
RECEIVED=$(docker compose exec -T kafka kafka-console-consumer --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --from-beginning --max-messages 1 --timeout-ms 20000)

# 공백 제거
RECEIVED_TRIMMED=$(echo "$RECEIVED" | xargs)
echo "   Received: $RECEIVED_TRIMMED"

# 문자열 포함 여부 검사
if [[ "$RECEIVED_TRIMMED" == *"$MID"* ]]; then
    echo "==> OK: mail_id matches"
    exit 0
fi

echo "==> FAIL: expected mail_id not found in payload"
exit 1