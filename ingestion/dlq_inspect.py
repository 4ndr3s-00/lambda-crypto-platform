"""
Inspección rápida de `market.dlq` (US-09): cuántos mensajes hay, por qué
fallaron y cuáles son los últimos.

Uso:
    python -m ingestion.dlq_inspect
    python -m ingestion.dlq_inspect --last 10 --max 50000
"""

import argparse
import json
import os
from collections import Counter
from typing import Any, Dict, Iterable, List, Tuple

LEGACY = "(sin metadatos)"
REASON_WIDTH = 70


def parse_message(raw: bytes) -> Dict[str, Any]:
    """Devuelve el sobre de la DLQ; un mensaje que no es JSON se muestra tal cual, sin perderlo."""
    try:
        message = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return {"original_event": raw.decode("utf-8", errors="replace")}
    return message if isinstance(message, dict) else {"original_event": message}


def summarize(messages: Iterable[Dict[str, Any]], last: int = 5) -> Dict[str, Any]:
    by_component: Counter = Counter()
    by_source: Counter = Counter()
    by_reason: Counter = Counter()
    recent: List[Dict[str, Any]] = []
    total = 0
    for message in messages:
        total += 1
        by_component[message.get("failed_component", LEGACY)] += 1
        by_source[message.get("source", LEGACY)] += 1
        reason = str(message.get("error_reason") or message.get("error") or LEGACY)
        by_reason[reason[:REASON_WIDTH]] += 1
        recent.append(message)
        recent = recent[-last:] if last else []
    return {"total": total, "by_component": by_component, "by_source": by_source,
            "by_reason": by_reason, "recent": recent}


def read_dlq(consumer: Any, topic: str, max_messages: int) -> Tuple[List[Dict[str, Any]], int]:
    """Lee desde el principio hasta el final que tenía el topic al empezar. Devuelve (mensajes, total en el topic)."""
    from kafka import TopicPartition

    partitions = [TopicPartition(topic, p) for p in sorted(consumer.partitions_for_topic(topic) or [])]
    consumer.assign(partitions)
    consumer.seek_to_beginning(*partitions)
    ends = consumer.end_offsets(partitions)
    begins = consumer.beginning_offsets(partitions)
    available = sum(ends[tp] - begins[tp] for tp in partitions)

    messages: List[Dict[str, Any]] = []
    pending = {tp: ends[tp] for tp in partitions if ends[tp] > begins[tp]}
    while pending and len(messages) < max_messages:
        batch = consumer.poll(timeout_ms=1000)
        if not batch:
            break
        for tp, records in batch.items():
            for record in records:
                messages.append(parse_message(record.value))
                if record.offset + 1 >= pending.get(tp, 0):
                    pending.pop(tp, None)
    return messages, available


def render(summary: Dict[str, Any], available: int, topic: str) -> str:
    lines = [f"Topic {topic}: {available} mensajes en total; analizados {summary['total']}"]
    for title, key in (("Por componente", "by_component"), ("Por fuente", "by_source"),
                       ("Por motivo", "by_reason")):
        lines.append(f"\n{title}:")
        for name, count in summary[key].most_common():
            lines.append(f"  {count:>7}  {name}")
    if summary["recent"]:
        lines.append(f"\nÚltimos {len(summary['recent'])}:")
        for m in summary["recent"]:
            lines.append(f"  {m.get('timestamp_utc', '-')}  {m.get('failed_component', LEGACY)}  "
                         f"{str(m.get('error_reason') or m.get('error') or LEGACY)[:REASON_WIDTH]}")
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Inspecciona y cuenta los mensajes de market.dlq")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--topic", default="market.dlq")
    parser.add_argument("--last", type=int, default=5, help="cuántos mensajes recientes mostrar")
    parser.add_argument("--max", type=int, default=10000, help="máximo de mensajes a analizar")
    args = parser.parse_args(argv)

    from kafka import KafkaConsumer

    consumer = KafkaConsumer(bootstrap_servers=args.bootstrap, enable_auto_commit=False)
    try:
        messages, available = read_dlq(consumer, args.topic, args.max)
    finally:
        consumer.close()
    print(render(summarize(messages, args.last), available, args.topic))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
