"""Observe LLM workloads and produce conservative deployment recommendations.

The controller deliberately does not mutate vLLM. Scheduler and KV cache
settings are process-wide and normally require an instance restart, so the
gateway records evidence and exposes a recommendation for an external,
drain-aware reconciler.
"""

from __future__ import annotations

import json
import math
import re
import threading
import time
from collections import Counter, deque
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class OptimizationPolicy:
    long_prompt_tokens: int = 2048
    long_output_tokens: int = 256
    high_in_flight: int = 6
    kv_cache_pressure: float = 0.85
    recommendation_hold_seconds: int = 300
    baseline_profile: str = "bf16-memory-085"
    capacity_profile: str = "bf16-memory-090"
    scheduler_profile: str = "batched-tokens-2048"

    @classmethod
    def from_path(cls, path: str) -> "OptimizationPolicy":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if raw.pop("schema_version", None) != 1:
            raise ValueError("optimizer policy schema_version must be 1")
        if raw.pop("mode", "observe") != "observe":
            raise ValueError("only observe mode is supported")
        policy = cls(**raw)
        policy.validate()
        return policy

    def validate(self) -> None:
        if min(self.long_prompt_tokens, self.long_output_tokens, self.high_in_flight) < 1:
            raise ValueError("optimizer token and concurrency thresholds must be positive")
        if not 0 < self.kv_cache_pressure <= 1:
            raise ValueError("kv_cache_pressure must be in (0, 1]")
        if self.recommendation_hold_seconds < 0:
            raise ValueError("recommendation_hold_seconds must be nonnegative")
        for value in (self.baseline_profile, self.capacity_profile, self.scheduler_profile):
            if not re.fullmatch(r"[a-zA-Z0-9_.-]{1,64}", value):
                raise ValueError("optimizer profile names contain invalid characters")


@dataclass(frozen=True)
class WorkloadFeatures:
    workload_class: str
    load_class: str
    estimated_prompt_tokens: int
    requested_output_tokens: int
    in_flight: int
    stream: bool


@dataclass(frozen=True)
class RuntimeSignals:
    kv_cache_usage: float | None = None
    preemptions_total: float | None = None
    waiting_requests: float | None = None


def _text_token_estimate(value: str) -> int:
    """Estimate tokens without retaining content or loading a tokenizer.

    CJK characters are usually close to one token each. ASCII text is estimated
    at four non-space characters per token. This is a routing/telemetry estimate,
    never a billing value.
    """
    cjk = 0
    ascii_non_space = 0
    other = 0
    for char in value:
        code = ord(char)
        if (0x3400 <= code <= 0x4DBF) or (0x4E00 <= code <= 0x9FFF):
            cjk += 1
        elif code < 128:
            if not char.isspace():
                ascii_non_space += 1
        elif not char.isspace():
            other += 1
    return cjk + other + math.ceil(ascii_non_space / 4)


def _content_tokens(value: object) -> int:
    if isinstance(value, str):
        return _text_token_estimate(value)
    if isinstance(value, list):
        return sum(_content_tokens(item) for item in value)
    if isinstance(value, dict):
        # Count user-visible text fields. Image/audio URLs are deliberately not
        # estimated because their model-side tokenization is modality-specific.
        return sum(_content_tokens(value.get(name)) for name in ("text", "content") if name in value)
    return 0


def analyze_request(body: bytes, route: str, in_flight: int,
                    policy: OptimizationPolicy) -> WorkloadFeatures:
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    if route == "/v1/chat/completions":
        prompt_tokens = _content_tokens(payload.get("messages", []))
    else:
        prompt_tokens = _content_tokens(payload.get("prompt", ""))
    requested = payload.get("max_tokens", payload.get("max_completion_tokens", 0))
    if type(requested) is not int or requested < 0:
        requested = 0
    if prompt_tokens >= policy.long_prompt_tokens:
        workload_class = "prefill"
    elif requested >= policy.long_output_tokens:
        workload_class = "decode"
    elif prompt_tokens or requested:
        workload_class = "balanced"
    else:
        workload_class = "unknown"
    load_class = "high" if in_flight >= policy.high_in_flight else "normal"
    return WorkloadFeatures(workload_class, load_class, prompt_tokens, requested, in_flight,
                            payload.get("stream") is True)


def parse_prometheus_signals(content: str) -> RuntimeSignals:
    values: dict[str, list[float]] = {}
    wanted = {
        "vllm:kv_cache_usage_perc",
        "vllm:num_preemptions_total",
        "vllm:num_requests_waiting",
    }
    for line in content.splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.match(r"([^\s{]+)(?:\{[^}]*\})?\s+([-+0-9.eE]+)$", line)
        if match and match.group(1) in wanted:
            try:
                values.setdefault(match.group(1), []).append(float(match.group(2)))
            except ValueError:
                pass
    return RuntimeSignals(
        kv_cache_usage=max(values.get("vllm:kv_cache_usage_perc", []), default=None),
        preemptions_total=sum(values["vllm:num_preemptions_total"])
        if values.get("vllm:num_preemptions_total") else None,
        waiting_requests=sum(values["vllm:num_requests_waiting"])
        if values.get("vllm:num_requests_waiting") else None,
    )


class OptimizationController:
    """Thread-safe, bounded aggregation and observe-only recommendations."""

    def __init__(self, policy: OptimizationPolicy | None = None):
        self.policy = policy
        self.lock = threading.Lock()
        self.workloads: Counter[str] = Counter()
        self.loads: Counter[str] = Counter()
        self.recent_workloads: deque[str] = deque(maxlen=256)
        self.estimated_prompt_tokens = 0
        self.requested_output_tokens = 0
        self.last_signals = RuntimeSignals()
        self.previous_preemptions: float | None = None
        self.last_scrape_ok = False
        self.capacity_hold_until = 0.0
        self.recommendation_reason = "optimizer disabled"

    @property
    def enabled(self) -> bool:
        return self.policy is not None

    def observe_request(self, body: bytes, route: str, in_flight: int) -> WorkloadFeatures | None:
        if self.policy is None:
            return None
        features = analyze_request(body, route, in_flight, self.policy)
        with self.lock:
            self.workloads[features.workload_class] += 1
            self.loads[features.load_class] += 1
            self.recent_workloads.append(features.workload_class)
            self.estimated_prompt_tokens += features.estimated_prompt_tokens
            self.requested_output_tokens += features.requested_output_tokens
        return features

    def update_runtime(self, text: str | None) -> None:
        if self.policy is None:
            return
        now = time.monotonic()
        with self.lock:
            if text is None:
                self.last_scrape_ok = False
                return
            signals = parse_prometheus_signals(text)
            preempted = (signals.preemptions_total is not None
                         and self.previous_preemptions is not None
                         and signals.preemptions_total > self.previous_preemptions)
            if signals.preemptions_total is not None:
                self.previous_preemptions = signals.preemptions_total
            if preempted:
                self.capacity_hold_until = now + self.policy.recommendation_hold_seconds
                self.recommendation_reason = "vLLM preemption increased"
            elif signals.kv_cache_usage is not None and signals.kv_cache_usage >= self.policy.kv_cache_pressure:
                self.capacity_hold_until = max(
                    self.capacity_hold_until, now + self.policy.recommendation_hold_seconds)
                self.recommendation_reason = "KV cache usage exceeded policy threshold"
            elif now >= self.capacity_hold_until:
                self.recommendation_reason = "no sustained KV capacity pressure observed"
            self.last_signals = signals
            self.last_scrape_ok = True

    def _profile_locked(self) -> str:
        assert self.policy is not None
        return (self.policy.capacity_profile if time.monotonic() < self.capacity_hold_until
                else self.policy.baseline_profile)

    def snapshot(self) -> dict:
        if self.policy is None:
            return {"enabled": False, "mode": "disabled"}
        with self.lock:
            return {
                "enabled": True,
                "mode": "observe",
                "applied": False,
                "recommended_service_profile": self._profile_locked(),
                "recommended_scheduler_profile": self.policy.scheduler_profile,
                "reason": self.recommendation_reason,
                "runtime_scrape_ok": self.last_scrape_ok,
                "runtime_signals": asdict(self.last_signals),
                "workload_counts": dict(sorted(self.workloads.items())),
                "load_counts": dict(sorted(self.loads.items())),
                "recent_workload_counts": dict(sorted(Counter(self.recent_workloads).items())),
                "estimated_prompt_tokens_total": self.estimated_prompt_tokens,
                "requested_output_tokens_total": self.requested_output_tokens,
                "limitations": [
                    "prompt token counts are estimates and are not used for billing",
                    "kernel dispatch remains inside vLLM and is not changed by the gateway",
                    "service profile changes require an external drain-aware reconciler",
                ],
            }

    def render_metrics(self) -> list[str]:
        if self.policy is None:
            return ["gateway_optimizer_enabled 0"]
        with self.lock:
            profile = self._profile_locked()
            lines = [
                "# TYPE gateway_optimizer_enabled gauge",
                "gateway_optimizer_enabled 1",
                "# TYPE gateway_optimizer_observe_only gauge",
                "gateway_optimizer_observe_only 1",
                "# TYPE gateway_optimizer_runtime_scrape_success gauge",
                f"gateway_optimizer_runtime_scrape_success {1 if self.last_scrape_ok else 0}",
                "# TYPE gateway_optimizer_recommended_profile_info gauge",
                f'gateway_optimizer_recommended_profile_info{{profile="{profile}"}} 1',
                "# TYPE gateway_workload_requests_total counter",
            ]
            lines.extend(f'gateway_workload_requests_total{{class="{name}"}} {count}'
                         for name, count in sorted(self.workloads.items()))
            lines.append("# TYPE gateway_load_observations_total counter")
            lines.extend(f'gateway_load_observations_total{{class="{name}"}} {count}'
                         for name, count in sorted(self.loads.items()))
            lines.extend([
                "# TYPE gateway_estimated_prompt_tokens_total counter",
                f"gateway_estimated_prompt_tokens_total {self.estimated_prompt_tokens}",
                "# TYPE gateway_requested_output_tokens_total counter",
                f"gateway_requested_output_tokens_total {self.requested_output_tokens}",
            ])
            if self.last_signals.kv_cache_usage is not None:
                lines.extend([
                    "# TYPE gateway_optimizer_observed_kv_cache_usage gauge",
                    f"gateway_optimizer_observed_kv_cache_usage {self.last_signals.kv_cache_usage}",
                ])
            if self.last_signals.waiting_requests is not None:
                lines.extend([
                    "# TYPE gateway_optimizer_observed_waiting_requests gauge",
                    f"gateway_optimizer_observed_waiting_requests {self.last_signals.waiting_requests}",
                ])
            if self.last_signals.preemptions_total is not None:
                lines.extend([
                    "# TYPE gateway_optimizer_observed_vllm_preemptions_total counter",
                    "gateway_optimizer_observed_vllm_preemptions_total "
                    f"{self.last_signals.preemptions_total}",
                ])
            return lines
