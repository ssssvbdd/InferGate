"""Exercise routing, bounded admission, SSE duration and circuit breaking.

Run against a disposable gateway binary. The script creates only loopback
listeners and temporary files; it does not need a GPU or Redis.
"""

from __future__ import annotations

import argparse
import concurrent.futures
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


class MockModel(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_GET(self):
        self.send_response(503 if self.server.fail else 200)
        self.end_headers()

    def do_POST(self):
        if self.path != "/v1/chat/completions":
            self.send_error(404)
            return
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        time.sleep(self.server.delay)
        if self.server.fail:
            self.send_error(503)
            return
        if payload.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self.wfile.write(b'data: {"choices":[]}\n\n')
            self.wfile.flush()
            time.sleep(0.8)
            try:
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            return
        body = json.dumps({"id": self.server.endpoint_id, "model": "mock-model",
                           "choices": [{"message": {"role": "assistant", "content": "ok"}}],
                           "usage": {"prompt_tokens": 1, "completion_tokens": 1,
                                     "total_tokens": 2}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass


def free_port() -> int:
    server = ThreadingHTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler)
    port = server.server_port
    server.server_close()
    return port


def call(port: int, key: str, *, stream: bool = False) -> tuple[int, bytes]:
    payload = json.dumps({"model": "mock-model", "messages": [
        {"role": "user", "content": "ping"}], "max_tokens": 4,
        "stream": stream}).encode()
    request = urllib.request.Request(f"http://127.0.0.1:{port}/v1/chat/completions",
                                     data=payload, headers={"Authorization": f"Bearer {key}",
                                                            "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def wait_ready(port: int, proc: subprocess.Popen) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"gateway exited: {proc.returncode}")
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=1):
                return
        except (urllib.error.URLError, TimeoutError):
            time.sleep(0.1)
    raise RuntimeError("gateway did not become ready")


def run(gateway_bin: Path) -> dict:
    servers = []
    threads = []
    for endpoint_id in ("a", "b"):
        server = ThreadingHTTPServer(("127.0.0.1", 0), MockModel)
        server.endpoint_id = endpoint_id
        server.fail = False
        server.delay = 0.0
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append(server)
        threads.append(thread)
    proc = None
    try:
        with tempfile.TemporaryDirectory(prefix="infergate-resilience-") as temp:
            root = Path(temp)
            key = "test-" + os.urandom(16).hex()
            (root / "keys.json").write_text(json.dumps({"keys": [{"tenant": "resilience",
                "sha256": hashlib.sha256(key.encode()).hexdigest(), "rpm": 100,
                "concurrency": 16}]}), encoding="utf-8")
            (root / "salt.key").write_bytes(os.urandom(32).hex().encode())
            port = free_port()
            (root / "gateway.yaml").write_text(f"""server:
  addr: 127.0.0.1:{port}
  upstream_timeout: 100ms
admin: {{enabled: false}}
metrics: {{enabled: false}}
auth:
  keys_file: {root / 'keys.json'}
  cache_salt_secret_file: {root / 'salt.key'}
  max_in_flight: 16
usage:
  ledger_file: {root / 'usage.jsonl'}
readiness:
  urls:
    - http://127.0.0.1:{servers[0].server_port}/health
    - http://127.0.0.1:{servers[1].server_port}/health
""", encoding="utf-8")
            (root / "models.yaml").write_text(f"""models:
  - name: mock-model
    provider_type: generic
    provider_model: mock-model
    discovery: static
    load_balance: round_robin
    endpoints:
      - id: a
        host: 127.0.0.1
        port: {servers[0].server_port}
        scheme: http
        max_in_flight: 1
        metadata: {{allow_internal: "true"}}
      - id: b
        host: 127.0.0.1
        port: {servers[1].server_port}
        scheme: http
        max_in_flight: 1
        metadata: {{allow_internal: "true"}}
    timeout: 500ms
    stream_idle_timeout: 1s
    max_retries: 0
    max_in_flight: 1
    max_waiting: 1
    wait_timeout: 100ms
""", encoding="utf-8")
            with (root / "gateway.log").open("wb") as log:
                proc = subprocess.Popen([str(gateway_bin), "-conf", str(root / "gateway.yaml"),
                                         "-models", str(root / "models.yaml")], stdout=log,
                                        stderr=subprocess.STDOUT)
            wait_ready(port, proc)

            ids = []
            for _ in range(6):
                status, body = call(port, key)
                assert status == 200, (status, body[:200])
                ids.append(json.loads(body)["id"])
            assert ids.count("a") == ids.count("b") == 3, ids

            start = time.monotonic()
            status, body = call(port, key, stream=True)
            elapsed = time.monotonic() - start
            assert status == 200 and b"data: [DONE]" in body and elapsed > 0.7, (status, elapsed)

            disconnect_payload = json.dumps({"model": "mock-model", "messages": [
                {"role": "user", "content": "disconnect"}], "max_tokens": 4,
                "stream": True}).encode()
            disconnect_request = urllib.request.Request(
                f"http://127.0.0.1:{port}/v1/chat/completions", data=disconnect_payload,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
            with urllib.request.urlopen(disconnect_request, timeout=5) as response:
                assert response.readline().startswith(b"data: ")
            disconnected_at = time.monotonic()
            while True:
                status, _ = call(port, key)
                if status == 200:
                    break
                if time.monotonic() - disconnected_at > 2:
                    raise AssertionError("client disconnect did not release admission")
                time.sleep(0.05)
            disconnect_release_ms = round((time.monotonic() - disconnected_at) * 1000)

            for server in servers:
                server.delay = 0.4
            barrier = threading.Barrier(4)
            def burst_call():
                barrier.wait()
                return call(port, key)[0]
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                burst = list(pool.map(lambda _: burst_call(), range(4)))
            assert 200 in burst and 503 in burst, burst
            for server in servers:
                server.delay = 0.0

            servers[0].fail = True
            for _ in range(12):
                call(port, key)
            healthy_statuses = [call(port, key)[0] for _ in range(4)]
            assert healthy_statuses == [200] * 4, healthy_statuses
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=2) as response:
                assert response.status == 200
            servers[1].fail = True
            statuses = [call(port, key)[0] for _ in range(7)]
            assert statuses[-1] == 503, statuses
            return {"round_robin": ids, "stream_elapsed_ms": round(elapsed * 1000),
                    "disconnect_release_ms": disconnect_release_ms,
                    "burst_statuses": burst, "one_upstream_statuses": healthy_statuses,
                    "all_failed_last_status": statuses[-1]}
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for server in servers:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-bin", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.gateway_bin), indent=2))
