"""Summarize privacy-safe usage journals for capacity and token-based cost.

Costs are estimates from an explicitly supplied rate card. GPU-hours must be
measured separately; gateway events alone cannot establish GPU allocation.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def percentile(values: list[int], fraction: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[math.ceil(fraction * len(ordered)) - 1]


def parse_timestamp(value: str) -> datetime:
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("usage timestamps must include a timezone")
    return stamp.astimezone(timezone.utc)


def load_events(paths: list[Path]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    seen: dict[str, dict[str, Any]] = {}
    for path in paths:
        with path.open(encoding="utf-8") as source:
            for number, line in enumerate(source, 1):
                if not line.strip():
                    continue
                event = json.loads(line)
                event_id = event.get("event_id") or f"{path.resolve()}:{number}"
                if event_id in seen:
                    if seen[event_id] != event:
                        raise ValueError(f"{path}:{number}: conflicting duplicate event_id")
                    continue
                seen[event_id] = event
                if not event.get("tenant") or not event.get("model"):
                    raise ValueError(f"{path}:{number}: missing tenant or model")
                parse_timestamp(event["timestamp"])
                events.append(event)
    return events


def report(events: list[dict[str, Any]], rates: dict[str, dict[str, float]] | None = None,
           safe_output_tps: float | None = None, target_load: float = 0.7,
           gpu_hours: float | None = None, gpu_hour_rate: float | None = None,
           cpu_core_hours: float | None = None, cpu_core_hour_rate: float | None = None,
           memory_gib_hours: float | None = None,
           memory_gib_hour_rate: float | None = None,
           business_map: dict[str, str] | None = None,
           capacity_profiles: dict[str, dict[str, float]] | None = None) -> dict[str, Any]:
    if not 0 < target_load <= 1:
        raise ValueError("target_load must be in (0, 1]")
    if safe_output_tps is not None and safe_output_tps <= 0:
        raise ValueError("safe_output_tps must be positive")
    for label, hours, rate in (("GPU", gpu_hours, gpu_hour_rate),
                               ("CPU", cpu_core_hours, cpu_core_hour_rate),
                               ("memory", memory_gib_hours, memory_gib_hour_rate)):
        if (hours is None) != (rate is None):
            raise ValueError(f"{label} hours and rate must be supplied together")
        if hours is not None and (hours < 0 or rate < 0):
            raise ValueError(f"{label} cost inputs must be nonnegative")
    for model_name, profile in (capacity_profiles or {}).items():
        if (not isinstance(profile, dict) or not model_name
                or not isinstance(profile.get("safe_output_tps_per_replica"), (int, float))
                or profile["safe_output_tps_per_replica"] <= 0
                or not isinstance(profile.get("target_load", target_load), (int, float))
                or not 0 < profile.get("target_load", target_load) <= 1):
            raise ValueError(f"invalid capacity profile for {model_name!r}")
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    buckets: dict[int, dict[str, int]] = defaultdict(lambda: {"requests": 0, "output_tokens": 0})
    model_buckets: dict[tuple[str, int], dict[str, int]] = defaultdict(
        lambda: {"requests": 0, "output_tokens": 0})
    all_durations: list[int] = []
    unmetered = 0
    server_errors = 0
    for event in events:
        key = (event["model"], event["tenant"])
        row = rows.setdefault(key, {"model": key[0], "tenant": key[1], "requests": 0,
                                    "metered_requests": 0, "input_tokens": 0,
                                    "output_tokens": 0, "p95_ms": None, "estimated_token_cost": None,
                                    "_durations": []})
        row["requests"] += 1
        server_errors += int(int(event.get("status", 0)) >= 500 or event.get("outcome") == "error")
        duration = event.get("duration_ms")
        if isinstance(duration, (int, float)) and duration > 0:
            row["_durations"].append(int(duration))
            all_durations.append(int(duration))
        minute = int(parse_timestamp(event["timestamp"]).timestamp()) // 60
        buckets[minute]["requests"] += 1
        model_buckets[(event["model"], minute)]["requests"] += 1
        if event.get("metered"):
            prompt = int(event.get("prompt_tokens", 0))
            completion = int(event.get("completion_tokens", 0))
            if prompt < 0 or completion < 0:
                raise ValueError("token counts cannot be negative")
            row["metered_requests"] += 1
            row["input_tokens"] += prompt
            row["output_tokens"] += completion
            buckets[minute]["output_tokens"] += completion
            model_buckets[(event["model"], minute)]["output_tokens"] += completion
        else:
            unmetered += 1
    result_rows = []
    for row in rows.values():
        row["p95_ms"] = percentile(row.pop("_durations"), 0.95)
        price = (rates or {}).get(row["model"])
        if price is not None:
            row["estimated_token_cost"] = round(
                (row["input_tokens"] * price["input_per_million"] +
                 row["output_tokens"] * price["output_per_million"]) / 1_000_000, 6)
        result_rows.append(row)
    peak = max((item["output_tokens"] / 60 for item in buckets.values()), default=0.0)
    recommended = math.ceil(peak / (safe_output_tps * target_load)) if safe_output_tps else None
    token_costs = [row["estimated_token_cost"] for row in result_rows]
    total_token_cost = round(sum(token_costs), 6) if all(cost is not None for cost in token_costs) else None
    business_rows: dict[str, dict[str, Any]] = {}
    for row in result_rows:
        business = (business_map or {}).get(row["tenant"], "unmapped")
        group = business_rows.setdefault(business, {"business": business, "requests": 0,
                                                    "metered_requests": 0, "input_tokens": 0,
                                                    "output_tokens": 0, "estimated_token_cost": 0.0})
        for field in ("requests", "metered_requests", "input_tokens", "output_tokens"):
            group[field] += row[field]
        if row["estimated_token_cost"] is None:
            group["estimated_token_cost"] = None
        elif group["estimated_token_cost"] is not None:
            group["estimated_token_cost"] += row["estimated_token_cost"]
    for group in business_rows.values():
        if group["estimated_token_cost"] is not None:
            group["estimated_token_cost"] = round(group["estimated_token_cost"], 6)
    model_rows = []
    for model_name in sorted({event["model"] for event in events}):
        model_minutes = [item for (name, _), item in model_buckets.items() if name == model_name]
        model_peak = max((item["output_tokens"] / 60 for item in model_minutes), default=0.0)
        profile = (capacity_profiles or {}).get(model_name)
        model_safe = profile["safe_output_tps_per_replica"] if profile else None
        model_load = profile.get("target_load", target_load) if profile else None
        model_rows.append({"model": model_name,
                           "peak_minute_rpm": max((item["requests"] for item in model_minutes), default=0),
                           "peak_minute_output_tps": round(model_peak, 3),
                           "safe_output_tps_per_replica": model_safe,
                           "target_load": model_load,
                           "recommended_replicas_for_observed_peak":
                           math.ceil(model_peak / (model_safe * model_load)) if model_safe else None})
    gpu_cost = round(gpu_hours * gpu_hour_rate, 6) if gpu_hours is not None else None
    cpu_cost = round(cpu_core_hours * cpu_core_hour_rate, 6) if cpu_core_hours is not None else None
    memory_cost = round(memory_gib_hours * memory_gib_hour_rate, 6) if memory_gib_hours is not None else None
    return {
        "request_count": len(events), "unmetered_requests": unmetered,
        "server_error_count": server_errors, "p95_ms": percentile(all_durations, 0.95),
        "peak_minute_rpm": max((item["requests"] for item in buckets.values()), default=0),
        "peak_minute_output_tps": round(peak, 3),
        "safe_output_tps_per_replica": safe_output_tps,
        "target_load": target_load,
        "recommended_replicas_for_observed_peak": recommended,
        "estimated_token_cost": total_token_cost, "measured_gpu_hours_cost": gpu_cost,
        "measured_cpu_core_hours_cost": cpu_cost,
        "measured_memory_gib_hours_cost": memory_cost,
        "by_model_tenant": sorted(result_rows, key=lambda row: (row["model"], row["tenant"])),
        "by_business": sorted(business_rows.values(), key=lambda row: row["business"]),
        "by_model_capacity": model_rows,
        "limitations": ["Only accepted, parsed requests appear in the usage journal; use HTTP metrics for full SLO.",
                        "Unmetered streams and failed requests are not billed by token count.",
                        "Replica estimate uses observed output TPS and an externally measured safe TPS per replica.",
                        "Resource costs require measured allocation-hours; they are not apportioned to tenants without a policy.",
                        "Business allocation requires an operator-owned tenant map; unmapped tenants remain explicit.",
                        "One-minute output TPS is an observed lower bound for burst planning, not an arrival forecast.",
                        "Do not add provider token prices to infrastructure costs if the rates already include infrastructure."],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journals", nargs="+", type=Path)
    parser.add_argument("--rates", type=Path, help="JSON model -> input_per_million/output_per_million rate card")
    parser.add_argument("--business-map", type=Path, help="JSON tenant -> business mapping")
    parser.add_argument("--capacity-profiles", type=Path,
                        help="JSON model -> safe_output_tps_per_replica and target_load")
    parser.add_argument("--safe-output-tps-per-replica", type=float)
    parser.add_argument("--target-load", type=float, default=0.7)
    parser.add_argument("--gpu-hours", type=float)
    parser.add_argument("--gpu-hour-rate", type=float)
    parser.add_argument("--cpu-core-hours", type=float)
    parser.add_argument("--cpu-core-hour-rate", type=float)
    parser.add_argument("--memory-gib-hours", type=float)
    parser.add_argument("--memory-gib-hour-rate", type=float)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    rates = json.loads(args.rates.read_text(encoding="utf-8")) if args.rates else None
    business_map = json.loads(args.business_map.read_text(encoding="utf-8")) if args.business_map else None
    profiles = json.loads(args.capacity_profiles.read_text(encoding="utf-8")) if args.capacity_profiles else None
    if business_map is not None and (not isinstance(business_map, dict)
                                     or any(not isinstance(key, str) or not isinstance(value, str)
                                            or not value for key, value in business_map.items())):
        parser.error("business-map must be a JSON object of tenant -> business strings")
    if profiles is not None and not isinstance(profiles, dict):
        parser.error("capacity-profiles must be a JSON object")
    if rates:
        for price in rates.values():
            if any(price[field] < 0 for field in ("input_per_million", "output_per_million")):
                parser.error("rates must be nonnegative")
    result = report(load_events(args.journals), rates, args.safe_output_tps_per_replica,
                    args.target_load, args.gpu_hours, args.gpu_hour_rate,
                    args.cpu_core_hours, args.cpu_core_hour_rate,
                    args.memory_gib_hours, args.memory_gib_hour_rate, business_map, profiles)
    body = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.write_text(body, encoding="utf-8")
    else:
        print(body, end="")


if __name__ == "__main__":
    main()
