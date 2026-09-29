#!/usr/bin/env python3
"""Internal observe-only control plane for the inference platform."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from urllib.parse import urlsplit

from optimizer import OptimizationController, OptimizationPolicy


@dataclass(frozen=True)
class GatewaySignals:
    workloads: dict[str, float]
    estimated_prompt_tokens: float | None
    requested_output_tokens: float | None


def parse_gateway_metrics(content: str) -> GatewaySignals:
    workloads: dict[str, float] = {}
    estimated = requested = None
    for line in content.splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.match(r'infergate_workload_requests_total\{[^}]*class="([^"]+)"[^}]*\}\s+([-+0-9.eE]+)$', line)
        if match:
            workloads[match.group(1)] = workloads.get(match.group(1), 0) + float(match.group(2))
            continue
        match = re.match(r"infergate_estimated_prompt_tokens_total\s+([-+0-9.eE]+)$", line)
        if match:
            estimated = float(match.group(1))
            continue
        match = re.match(r"infergate_requested_output_tokens_total\s+([-+0-9.eE]+)$", line)
        if match:
            requested = float(match.group(1))
    return GatewaySignals(workloads, estimated, requested)


def scrape(url: str, timeout: float = 2.0) -> str | None:
    try:
        with urlopen(url, timeout=timeout) as response:
            return response.read(2_000_000).decode("utf-8", "replace")
    except (HTTPError, URLError, TimeoutError, OSError):
        return None


class App:
    def __init__(self) -> None:
        policy_path = os.environ.get("OPTIMIZER_POLICY_FILE", "")
        if not policy_path:
            raise ValueError("OPTIMIZER_POLICY_FILE is required")
        self.controller = OptimizationController(OptimizationPolicy.from_path(policy_path))
        self.vllm_metrics_url = os.environ.get("VLLM_METRICS_URL", "http://127.0.0.1:8000/metrics")
        self.gateway_metrics_url = os.environ.get("GATEWAY_METRICS_URL", "http://127.0.0.1:6060/metrics")
        for name, value in (("VLLM_METRICS_URL", self.vllm_metrics_url),
                            ("GATEWAY_METRICS_URL", self.gateway_metrics_url)):
            parsed = urlsplit(value)
            if parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost", "::1"):
                raise ValueError(f"{name} must use a loopback http URL")

    def snapshot(self) -> dict:
        vllm = scrape(self.vllm_metrics_url)
        gateway = scrape(self.gateway_metrics_url)
        self.controller.update_runtime(vllm)
        result = self.controller.snapshot()
        # Request classification belongs to the Go data plane. Remove the
        # controller's legacy in-process counters to keep one metric source.
        for field in ("workload_counts", "load_counts", "recent_workload_counts",
                      "estimated_prompt_tokens_total", "requested_output_tokens_total"):
            result.pop(field, None)
        result["gateway_scrape_ok"] = gateway is not None
        result["gateway_signals"] = asdict(parse_gateway_metrics(gateway or ""))
        result["topology"] = "infergate -> vllm; optimizer is off the request path"
        return result

    def metrics(self) -> bytes:
        snapshot = self.snapshot()
        lines = self.controller.render_metrics()
        lines.append("optimizer_gateway_scrape_success " + ("1" if snapshot["gateway_scrape_ok"] else "0"))
        for name, value in sorted(snapshot["gateway_signals"]["workloads"].items()):
            safe = re.sub(r"[^a-zA-Z0-9_.-]", "_", name)
            lines.append(f'optimizer_observed_gateway_workloads_total{{class="{safe}"}} {value}')
        return ("\n".join(lines) + "\n").encode()


class Handler(BaseHTTPRequestHandler):
    server: "Server"

    def log_message(self, *_args: object) -> None:
        pass

    def do_GET(self) -> None:
        route = urlsplit(self.path).path
        if self.client_address[0] not in ("127.0.0.1", "::1"):
            self._write(403, b'{"error":"local access only"}', "application/json")
            return
        if route == "/healthz":
            self._write(200, b"ok\n")
        elif route == "/readyz":
            snapshot = self.server.app.snapshot()
            ready = snapshot["runtime_scrape_ok"] and snapshot["gateway_scrape_ok"]
            self._write(200 if ready else 503, b"ready\n" if ready else b"unavailable\n")
        elif route == "/optimizer/status":
            self._write(200, json.dumps(self.server.app.snapshot(), ensure_ascii=False,
                                        separators=(",", ":")).encode(), "application/json")
        elif route == "/metrics":
            self._write(200, self.server.app.metrics(), "text/plain; version=0.0.4")
        else:
            self._write(404, b'{"error":"not found"}', "application/json")

    def _write(self, status: int, body: bytes, content_type: str = "text/plain") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], app: App):
        self.app = app
        super().__init__(address, Handler)


def main() -> None:
    app = App()
    host = os.environ.get("LISTEN_HOST", "127.0.0.1")
    port = int(os.environ.get("LISTEN_PORT", "8091"))
    if host not in ("127.0.0.1", "::1"):
        raise ValueError("optimizer must bind to loopback")
    server = Server((host, port), app)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
