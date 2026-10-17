"""Exercise two gateway processes against a mock model and one test Redis.

This test never invokes a GPU. Use a disposable Redis instance because the
gateway writes quota keys and usage stream entries into it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from capacity_report import load_events, report


def free_port() -> int:
    server = ThreadingHTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler)
    port = server.server_port
    server.server_close()
    return port


class MockModel(BaseHTTPRequestHandler):
    started = threading.Event()

    def log_message(self, *_args):
        pass

    def do_GET(self):
        if self.path != "/health":
            self.send_error(404)
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def do_POST(self):
        if self.path != "/v1/chat/completions":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        json.loads(self.rfile.read(length))
        self.started.set()
        time.sleep(0.5)
        body = json.dumps({"id": "mock", "object": "chat.completion", "model": "mock-model",
                           "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"},
                                        "finish_reason": "stop"}],
                           "usage": {"prompt_tokens": 4, "completion_tokens": 2, "total_tokens": 6}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def call(port: int, key: str) -> int:
    req = urllib.request.Request(f"http://127.0.0.1:{port}/v1/chat/completions",
                                 data=json.dumps({"model": "mock-model", "messages": [
                                     {"role": "user", "content": "ping"}], "max_tokens": 4}).encode(),
                                 headers={"Authorization": f"Bearer {key}",
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def wait_ready(port: int, proc: subprocess.Popen) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"gateway {port} exited with {proc.returncode}")
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=1) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(0.1)
    raise RuntimeError(f"gateway {port} did not become ready")


def run(gateway_bin: Path, export_bin: Path, redis_addr: str) -> dict:
    server = ThreadingHTTPServer(("127.0.0.1", 0), MockModel)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    procs: list[subprocess.Popen] = []
    try:
        with tempfile.TemporaryDirectory(prefix="infergate-multireplica-") as temp:
            root = Path(temp)
            key = "integration-" + os.urandom(16).hex()
            (root / "keys.json").write_text(json.dumps({"keys": [{"tenant": "integration",
                "sha256": hashlib.sha256(key.encode()).hexdigest(), "rpm": 2,
                "concurrency": 1}]}), encoding="utf-8")
            (root / "salt.key").write_bytes(os.urandom(32).hex().encode())
            nonce = os.urandom(8).hex()
            stream = f"infergate:usage:{{integration-{nonce}}}:events"
            models = root / "models.yaml"
            models.write_text(f"""models:
  - name: mock-model
    version: default
    model_type: llm
    provider_type: generic
    provider_model: mock-model
    discovery: static
    load_balance: direct
    endpoints:
      - id: mock
        host: 127.0.0.1
        port: {server.server_port}
        scheme: http
        weight: 1
        metadata:
          allow_internal: "true"
    timeout: 5s
    max_retries: 0
""", encoding="utf-8")
            ports = [free_port(), free_port()]
            for index, port in enumerate(ports):
                cfg = root / f"gateway-{index}.yaml"
                cfg.write_text(f"""server:
  addr: 127.0.0.1:{port}
  upstream_timeout: 5s
admin:
  enabled: false
redis:
  enabled: true
  addr: {redis_addr}
metrics:
  enabled: false
auth:
  keys_file: {root / 'keys.json'}
  cache_salt_secret_file: {root / 'salt.key'}
  max_in_flight: 1
  distributed: true
  quota_namespace: integration-{nonce}
  lease_ttl: 15s
usage:
  ledger_file: {root / f'usage-{index}.jsonl'}
  redis_stream: "{stream}"
readiness:
  url: http://127.0.0.1:{server.server_port}/health
  timeout: 1s
""", encoding="utf-8")
                log = (root / f"gateway-{index}.log").open("wb")
                proc = subprocess.Popen([str(gateway_bin), "-conf", str(cfg), "-models", str(models)],
                                        stdout=log, stderr=subprocess.STDOUT)
                log.close()
                procs.append(proc)
            for port, proc in zip(ports, procs):
                wait_ready(port, proc)
            first: list[int] = []
            worker = threading.Thread(target=lambda: first.append(call(ports[0], key)))
            worker.start()
            if not MockModel.started.wait(5):
                raise RuntimeError("mock model did not receive first request")
            rejected_concurrency = call(ports[1], key)
            worker.join(5)
            second = call(ports[1], key)
            rejected_rpm = call(ports[0], key)
            if first != [200] or (rejected_concurrency, second, rejected_rpm) != (429, 200, 429):
                raise AssertionError(f"unexpected statuses {first}, {rejected_concurrency}, {second}, {rejected_rpm}")
            exported = root / "central.jsonl"
            for _ in range(40):
                if exported.exists():
                    exported.unlink()
                result = subprocess.run([str(export_bin), "-redis", redis_addr, "-stream", stream,
                                         "-output", str(exported)], capture_output=True, text=True, timeout=10)
                if result.returncode != 0:
                    raise RuntimeError(result.stderr)
                events = [json.loads(line) for line in exported.read_text(encoding="utf-8").splitlines()]
                if len(events) == 2:
                    break
                time.sleep(0.25)
            else:
                raise AssertionError("central ledger did not receive two events")
            if len({event["event_id"] for event in events}) != 2:
                raise AssertionError("central event IDs are not unique")
            summary = report(load_events([exported]),
                             {"mock-model": {"input_per_million": 1, "output_per_million": 1}},
                             safe_output_tps=1)
            if summary["request_count"] != 2 or summary["by_model_tenant"][0]["output_tokens"] != 4:
                raise AssertionError("capacity report disagrees with central usage stream")
            return {"gateway_ports": ports, "statuses": [first[0], rejected_concurrency,
                    second, rejected_rpm], "central_events": len(events),
                    "reported_output_tokens": summary["by_model_tenant"][0]["output_tokens"]}
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        server.shutdown()
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-bin", required=True, type=Path)
    parser.add_argument("--export-bin", required=True, type=Path)
    parser.add_argument("--redis-addr", default="127.0.0.1:16379")
    args = parser.parse_args()
    print(json.dumps(run(args.gateway_bin, args.export_bin, args.redis_addr), indent=2))


if __name__ == "__main__":
    main()
