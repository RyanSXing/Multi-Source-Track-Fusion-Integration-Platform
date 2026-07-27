#!/usr/bin/env python3
"""Replay Detection JSON through Redpanda and measure published track latency."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import subprocess
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FIXTURE = Path(__file__).with_name("fixtures") / "detections.jsonl"


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(quantile * len(ordered)) - 1)]


def load_templates(path: Path) -> list[dict]:
    templates = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not templates:
        raise ValueError("input fixture is empty")
    required = {
        "offsetMs",
        "sourceId",
        "sourceType",
        "latDeg",
        "lonDeg",
        "positionSigmaMeters",
        "groundTruth",
    }
    if any(required - template.keys() for template in templates):
        raise ValueError("input fixture is missing required fields")
    return templates


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def tick_for(value: str, tick_ms: int) -> int:
    observed_ms = int(
        datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000
    )
    return (observed_ms // tick_ms + 1) * tick_ms


def detection_order(detection: dict) -> tuple:
    nullable = lambda value: (value is not None, value or 0)
    return (
        detection["observedAt"],
        detection["sourceType"],
        detection["sourceId"],
        detection["latDeg"],
        detection["lonDeg"],
        nullable(detection.get("altMeters")),
        nullable(detection.get("speedMps")),
        nullable(detection.get("headingDeg")),
        detection["positionSigmaMeters"],
        detection["receivedAt"],
        tuple(sorted(detection.get("attributes", {}).items())),
    )


def cpu_usage(api_url: str) -> float | None:
    with urllib.request.urlopen(
        f"{api_url}/actuator/prometheus", timeout=2
    ) as response:
        for line in response.read().decode().splitlines():
            if line.startswith("process_cpu_usage "):
                return float(line.split()[1]) * 100
    return None


def acceptance(report: dict, args: argparse.Namespace) -> dict:
    failures = []
    if report["deliveryRatio"] < args.min_delivery_ratio:
        failures.append(
            f"delivery ratio {report['deliveryRatio']:.4f} "
            f"is below {args.min_delivery_ratio:.4f}"
        )
    p99 = report["latencyMs"]["p99"]
    if args.max_p99_ms is not None and (p99 is None or p99 > args.max_p99_ms):
        failures.append(f"p99 latency {p99} ms exceeds {args.max_p99_ms} ms")
    reduction = report["duplicateTrackRemoval"]["reductionPercent"]
    if (
        args.min_duplicate_reduction_percent is not None
        and (
            reduction is None
            or reduction < args.min_duplicate_reduction_percent
        )
    ):
        failures.append(
            f"duplicate reduction {reduction}% is below "
            f"{args.min_duplicate_reduction_percent}%"
        )
    integrity = report["groundTruthIntegrity"]
    if integrity["impureTracks"]:
        failures.append(f"{integrity['impureTracks']} tracks mix ground-truth objects")
    if integrity["missingGroundTruthObjects"]:
        failures.append(
            f"{integrity['missingGroundTruthObjects']} ground-truth objects are missing"
        )
    if integrity["fragmentedGroundTruthObjects"] and not args.allow_fragmentation:
        failures.append(
            f"{integrity['fragmentedGroundTruthObjects']} ground-truth objects are fragmented"
        )
    if report["receiverErrors"]:
        failures.append("WebSocket receiver reported errors")
    return {
        "passed": not failures,
        "criteria": {
            "minDeliveryRatio": args.min_delivery_ratio,
            "maxP99Ms": args.max_p99_ms,
            "minDuplicateReductionPercent": args.min_duplicate_reduction_percent,
            "allowFragmentation": args.allow_fragmentation,
        },
        "failures": failures,
    }


def ground_truth_integrity(
    ground_truth: set[str], track_truths: dict[int, set[str]]
) -> dict[str, int]:
    truth_to_tracks = {truth: set() for truth in ground_truth}
    for track_id, truths in track_truths.items():
        for truth in truths:
            truth_to_tracks.setdefault(truth, set()).add(track_id)
    return {
        "impureTracks": sum(len(truths) > 1 for truths in track_truths.values()),
        "missingGroundTruthObjects": sum(
            not track_ids for track_ids in truth_to_tracks.values()
        ),
        "fragmentedGroundTruthObjects": sum(
            len(track_ids) > 1 for track_ids in truth_to_tracks.values()
        ),
    }


def credited_observations(
    detection: dict,
    run_id: str,
    representative_ids: dict[tuple[str, str, str, int], str],
    expected_observations: dict[
        str, tuple[tuple[str, str, str, int], float]
    ],
    tick_ms: int,
) -> list[tuple[str, float]]:
    attributes = detection.get("attributes", {})
    if attributes.get("loadRun") != run_id:
        return []
    group = (
        detection["sourceType"],
        detection["sourceId"],
        attributes.get("groundTruth"),
        tick_for(detection["observedAt"], tick_ms),
    )
    if attributes.get("messageId") != representative_ids.get(group):
        return []
    return [
        (message_id, received_at)
        for message_id, (expected_group, received_at) in expected_observations.items()
        if expected_group == group
    ]


def receive_latencies(
    ws_url: str,
    run_id: str,
    expected_ids: set[str],
    expected_observations: dict[
        str, tuple[tuple[str, str, str, int], float]
    ],
    representative_ids: dict[tuple[str, str, str, int], str],
    tick_ms: int,
    latencies: dict[str, float],
    track_truths: dict[int, set[str]],
    ready: threading.Event,
    stop: threading.Event,
    errors: list[str],
) -> None:
    try:
        import websocket

        socket = websocket.create_connection(ws_url, timeout=1)
        ready.set()
        try:
            while not stop.is_set() and expected_ids - set(latencies):
                try:
                    event = json.loads(socket.recv())
                except websocket.WebSocketTimeoutException:
                    continue
                for track in event.get("tracks", []):
                    truths = {
                        detection.get("attributes", {}).get("groundTruth")
                        for detection in track.get("contributors", [])
                        if detection.get("attributes", {}).get("loadRun") == run_id
                    }
                    truths.discard(None)
                    if truths:
                        track_truths.setdefault(track["trackId"], set()).update(truths)
                    for detection in track.get("contributors", []):
                        published_at = time.time()
                        for expected_id, received_at in credited_observations(
                            detection,
                            run_id,
                            representative_ids,
                            expected_observations,
                            tick_ms,
                        ):
                            if expected_id not in latencies:
                                latencies[expected_id] = max(
                                    0, (published_at - received_at) * 1000
                                )
        finally:
            socket.close()
    except Exception as error:  # surfaced by the main thread
        errors.append(str(error))
        ready.set()


def sample_cpu(
    api_url: str, samples: list[float], stop: threading.Event
) -> None:
    while not stop.wait(0.25):
        try:
            usage = cpu_usage(api_url)
            if usage is not None and math.isfinite(usage):
                samples.append(usage)
        except Exception:
            pass


def run(args: argparse.Namespace) -> dict:
    if (
        args.multiplier <= 0
        or args.cycles < 1
        or args.cycle_ms < 1
        or args.tick_ms < 1
    ):
        raise ValueError("multiplier, cycles, cycle-ms, and tick-ms must be positive")
    if not 0 <= args.min_delivery_ratio <= 1:
        raise ValueError("min-delivery-ratio must be between 0 and 1")
    if args.max_p99_ms is not None and args.max_p99_ms < 0:
        raise ValueError("max-p99-ms cannot be negative")
    if (
        args.min_duplicate_reduction_percent is not None
        and not 0 <= args.min_duplicate_reduction_percent <= 100
    ):
        raise ValueError(
            "min-duplicate-reduction-percent must be between 0 and 100"
        )
    templates = load_templates(args.input)
    run_id = uuid.uuid4().hex[:12]
    scheduled = [
        ((cycle * args.cycle_ms + template["offsetMs"]) / args.multiplier, template)
        for cycle in range(args.cycles)
        for template in templates
    ]
    expected_ids = {f"{run_id}-{index}" for index in range(len(scheduled))}
    expected_observations: dict[
        str, tuple[tuple[str, str, str, int], float]
    ] = {}
    representative_ids: dict[tuple[str, str, str, int], str] = {}
    representative_orders: dict[tuple[str, str, str, int], tuple] = {}
    latencies: dict[str, float] = {}
    track_truths: dict[int, set[str]] = {}
    errors: list[str] = []
    cpu_samples: list[float] = []
    ready = threading.Event()
    stop = threading.Event()
    receiver = threading.Thread(
        target=receive_latencies,
        args=(
            args.ws_url,
            run_id,
            expected_ids,
            expected_observations,
            representative_ids,
            args.tick_ms,
            latencies,
            track_truths,
            ready,
            stop,
            errors,
        ),
        daemon=True,
    )
    sampler = threading.Thread(
        target=sample_cpu, args=(args.api_url, cpu_samples, stop), daemon=True
    )
    receiver.start()
    sampler.start()
    if not ready.wait(5) or errors:
        stop.set()
        raise RuntimeError(
            errors[0] if errors else f"could not connect to {args.ws_url}"
        )

    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        args.compose_service,
        "rpk",
        "topic",
        "produce",
        args.topic,
        "--format",
        "%k %v{json}\\n",
        "--output-format",
        "",
    ]
    producer = subprocess.Popen(
        command,
        cwd=ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    started = time.perf_counter()
    assert producer.stdin is not None
    for index, (offset_ms, template) in enumerate(scheduled):
        delay = started + offset_ms / 1000 - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        timestamp = now_iso()
        message_id = f"{run_id}-{index}"
        record = {
            key: value
            for key, value in template.items()
            if key not in {"offsetMs", "groundTruth"}
        }
        record.update(
            {
                "observedAt": timestamp,
                "receivedAt": timestamp,
                "attributes": {
                    "groundTruth": template["groundTruth"],
                    "loadRun": run_id,
                    "messageId": message_id,
                },
            }
        )
        group = (
            record["sourceType"],
            record["sourceId"],
            template["groundTruth"],
            tick_for(timestamp, args.tick_ms),
        )
        expected_observations[message_id] = (
            group,
            datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp(),
        )
        order = detection_order(record)
        if group not in representative_orders or order > representative_orders[group]:
            representative_orders[group] = order
            representative_ids[group] = message_id
        producer.stdin.write(
            f"{record['sourceType']}:{record['sourceId']} "
            + json.dumps(record, separators=(",", ":"))
            + "\n"
        )
        producer.stdin.flush()
    producer.stdin.close()
    stderr = producer.stderr.read() if producer.stderr else ""
    exit_code = producer.wait(timeout=args.timeout)
    produced_at = time.perf_counter()
    if exit_code:
        stop.set()
        raise RuntimeError(f"rpk producer failed: {stderr.strip()}")

    deadline = time.monotonic() + args.timeout
    while expected_ids - set(latencies) and time.monotonic() < deadline:
        time.sleep(0.1)
    stop.set()
    receiver.join(2)
    sampler.join(2)
    if errors and not latencies:
        raise RuntimeError(errors[0])

    ground_truth = {template["groundTruth"] for template in templates}
    integrity = ground_truth_integrity(ground_truth, track_truths)
    source_types = {template["sourceType"] for template in templates}
    source_specific_tracks = len(ground_truth) * len(source_types)
    reduction = (
        max(0, 1 - len(track_truths) / source_specific_tracks) * 100
        if source_specific_tracks
        else None
    )
    values = list(latencies.values())
    production_seconds = max(produced_at - started, 1e-9)
    try:
        input_name = str(args.input.resolve().relative_to(ROOT))
    except ValueError:
        input_name = str(args.input)
    report = {
        "generatedAt": now_iso(),
        "runId": run_id,
        "config": {
            "input": input_name,
            "multiplier": args.multiplier,
            "cycles": args.cycles,
            "cycleMs": args.cycle_ms,
            "tickMs": args.tick_ms,
            "topic": args.topic,
        },
        "inputMessages": len(scheduled),
        "latencySamples": len(values),
        "deliveryRatio": round(len(values) / len(scheduled), 4),
        "receiverErrors": list(errors),
        "productionDurationMs": round(production_seconds * 1000, 3),
        "producerThroughputMessagesPerSecond": round(
            len(scheduled) / production_seconds, 3
        ),
        "latencyMs": {
            "p50": round(percentile(values, 0.50), 3) if values else None,
            "p95": round(percentile(values, 0.95), 3) if values else None,
            "p99": round(percentile(values, 0.99), 3) if values else None,
            "max": round(max(values), 3) if values else None,
        },
        "processCpuPercent": {
            "average": round(statistics.fmean(cpu_samples), 3)
            if cpu_samples
            else None,
            "max": round(max(cpu_samples), 3) if cpu_samples else None,
            "samples": len(cpu_samples),
        },
        "duplicateTrackRemoval": {
            "groundTruthObjects": len(ground_truth),
            "sourceSpecificTracks": source_specific_tracks,
            "fusedTracks": len(track_truths),
            "reductionPercent": round(reduction, 3)
            if reduction is not None
                else None,
        },
        "groundTruthIntegrity": integrity,
    }
    report["acceptance"] = acceptance(report, args)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--multiplier", type=float, default=1)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--cycle-ms", type=int, default=1200)
    parser.add_argument("--tick-ms", type=int, default=1000)
    parser.add_argument("--topic", default="detections")
    parser.add_argument("--compose-service", default="redpanda")
    parser.add_argument("--api-url", default="http://localhost:8080")
    parser.add_argument("--ws-url", default="ws://localhost:8080/ws/tracks")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--min-delivery-ratio", type=float, default=1)
    parser.add_argument("--max-p99-ms", type=float)
    parser.add_argument("--min-duplicate-reduction-percent", type=float)
    parser.add_argument("--allow-fragmentation", action="store_true")
    parser.add_argument(
        "--report",
        type=Path,
        default=Path(__file__).with_name("reports") / "load-latest.json",
    )
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        assert percentile([4, 1, 3, 2], 0.5) == 2
        assert percentile([4, 1, 3, 2], 0.99) == 4
        assert tick_for("2026-01-02T03:04:05.123Z", 1000) == 1767323046000
        assert len(load_templates(args.input)) == 6
        ordered = load_templates(args.input)[0] | {
            "observedAt": "2026-01-02T03:04:05.123Z",
            "receivedAt": "2026-01-02T03:04:05.123Z",
            "attributes": {"messageId": "id1"},
        }
        assert detection_order(ordered) < detection_order(
            ordered | {"observedAt": "2026-01-02T03:04:05.124Z"}
        )
        assert ground_truth_integrity(
            {"alpha", "bravo", "charlie"},
            {1: {"alpha", "bravo"}, 2: {"alpha"}, 3: {"bravo"}},
        ) == {
            "impureTracks": 1,
            "missingGroundTruthObjects": 1,
            "fragmentedGroundTruthObjects": 2,
        }
        group = ("ADSB", "feed", "alpha", 1767323046000)
        expected = {"id1": (group, 1.0), "id2": (group, 2.0)}
        representative = {group: "id2"}
        assert credited_observations(
            {
                "sourceType": "ADSB",
                "sourceId": "feed",
                "observedAt": "2026-01-02T03:04:05.123Z",
                "attributes": {
                    "loadRun": "run",
                    "groundTruth": "alpha",
                    "messageId": "id1",
                },
            },
            "run",
            representative,
            expected,
            1000,
        ) == []
        assert credited_observations(
            {
                "sourceType": "ADSB",
                "sourceId": "feed",
                "observedAt": "2026-01-02T03:04:05.123Z",
                "attributes": {
                    "loadRun": "run",
                    "groundTruth": "alpha",
                    "messageId": "id2",
                },
            },
            "run",
            representative,
            expected,
            1000,
        ) == [("id1", 1.0), ("id2", 2.0)]
        failed = acceptance(
            {
                "deliveryRatio": 0.5,
                "latencyMs": {"p99": 100},
                "duplicateTrackRemoval": {"reductionPercent": 50},
                "groundTruthIntegrity": {
                    "impureTracks": 0,
                    "missingGroundTruthObjects": 1,
                    "fragmentedGroundTruthObjects": 1,
                },
                "receiverErrors": [],
            },
            args,
        )
        assert not failed["passed"] and len(failed["failures"]) == 3
        print("load harness self-test passed")
        return 0
    report = run(args)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["acceptance"]["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
