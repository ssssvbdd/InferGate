"""Bounded host/GPU sampling during a benchmark; no prompts or process arguments are saved."""

from __future__ import annotations

import math
import subprocess
import threading
import time
from pathlib import Path


def percentile(values: list[float], percent: int) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percent / 100
    lower, upper = math.floor(position), math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _cpu_counters() -> tuple[int, int] | None:
    path = Path("/proc/stat")
    if not path.exists():
        return None
    with path.open(encoding="ascii") as source:
        values = [int(value) for value in source.readline().split()[1:]]
    if len(values) < 4:
        return None
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    return sum(values), idle


def _memory_available_gib() -> float | None:
    path = Path("/proc/meminfo")
    if not path.exists():
        return None
    for line in path.read_text(encoding="ascii").splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) / 1024 / 1024
    return None


def _gpu_sample(index: int) -> dict[str, float]:
    command = ["nvidia-smi", "--query-gpu=index,memory.used,utilization.gpu,utilization.memory",
               "--format=csv,noheader,nounits"]
    result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=5)
    for line in result.stdout.splitlines():
        columns = [value.strip() for value in line.split(",")]
        if len(columns) == 4 and columns[0] == str(index):
            return {"gpu_memory_used_mib": float(columns[1]),
                    "gpu_utilization_pct": float(columns[2]),
                    "gpu_memory_utilization_pct": float(columns[3])}
    raise RuntimeError(f"GPU {index} absent from nvidia-smi sample")


class ResourceSampler:
    def __init__(self, gpu_index: int | None, interval_seconds: float = 1.0,
                 max_samples: int = 3600) -> None:
        if interval_seconds < 0.2 or max_samples < 1:
            raise ValueError("invalid resource sampling interval or limit")
        self.gpu_index = gpu_index
        self.interval_seconds = interval_seconds
        self.max_samples = max_samples
        self.samples: list[dict] = []
        self.errors: list[str] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("resource sampler already started")
        self._thread = threading.Thread(target=self._run, name="resource-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> dict:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=8)
        summary: dict[str, object] = {"sample_count": len(self.samples),
                                      "interval_seconds": self.interval_seconds,
                                      "errors": self.errors[:5]}
        for field in ("cpu_utilization_pct", "host_memory_available_gib",
                      "gpu_memory_used_mib", "gpu_utilization_pct",
                      "gpu_memory_utilization_pct"):
            values = [row[field] for row in self.samples if field in row]
            summary[field] = {"p50": percentile(values, 50), "p95": percentile(values, 95),
                              "p99": percentile(values, 99), "min": min(values) if values else None,
                              "max": max(values) if values else None}
        return summary

    def _run(self) -> None:
        previous = _cpu_counters()
        while not self._stop.is_set() and len(self.samples) < self.max_samples:
            row: dict[str, float] = {"elapsed_monotonic_seconds": time.monotonic()}
            current = _cpu_counters()
            if previous and current and current[0] > previous[0]:
                busy = (current[0] - previous[0]) - (current[1] - previous[1])
                row["cpu_utilization_pct"] = max(0.0, min(100.0, 100 * busy / (current[0] - previous[0])))
            previous = current
            available = _memory_available_gib()
            if available is not None:
                row["host_memory_available_gib"] = available
            if self.gpu_index is not None:
                try:
                    row.update(_gpu_sample(self.gpu_index))
                except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
                    if len(self.errors) < 5:
                        self.errors.append(type(exc).__name__)
            self.samples.append(row)
            self._stop.wait(self.interval_seconds)
