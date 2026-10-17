#!/usr/bin/env python3
"""Run a bounded, authenticated acceptance check for an OpenAI chat model.

No prompt, response body, or API key is written to the report. This is an
availability/protocol check; quality and load gates belong to perf-lab.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from model_delivery import DeliveryError, load_manifest


class AcceptanceError(RuntimeError):
    pass


def _url(base: str, path: str) -> str:
    return base.rstrip("/") + path


def _get(url: str, timeout: int, key: str | None = None) -> tuple[int, bytes]:
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    try:
        with urlopen(Request(url, headers=headers), timeout=timeout) as response:
            return response.status, response.read(1024 * 1024)
    except HTTPError as exc:
        return exc.code, b""
    except (URLError, TimeoutError, OSError) as exc:
        raise AcceptanceError(f"GET {url} could not connect: {type(exc).__name__}") from exc


def _post(url: str, key: str, body: dict, timeout: int, *, stream: bool) -> tuple[int, object]:
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    request = Request(url, data=data, headers={"Authorization": f"Bearer {key}",
                                           "Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                return response.status, None
            if not stream:
                return response.status, json.load(response)
            seen_text = False
            seen_done = False
            event_count = 0
            for line in response:
                line = line.strip()
                if not line.startswith(b"data:"):
                    continue
                payload = line[5:].strip()
                if payload == b"[DONE]":
                    seen_done = True
                    break
                event_count += 1
                event = json.loads(payload)
                for choice in event.get("choices", []):
                    delta = choice.get("delta") or {}
                    if delta.get("content") or delta.get("reasoning_content"):
                        seen_text = True
            return response.status, {"events": event_count, "has_output": seen_text,
                                     "done": seen_done}
    except HTTPError as exc:
        return exc.code, None
    except (URLError, TimeoutError, OSError, ValueError) as exc:
        raise AcceptanceError(f"POST {url} failed: {type(exc).__name__}") from exc


def run_acceptance(manifest: dict, key: str) -> dict:
    if manifest["service"]["protocol"] != "openai-chat":
        raise AcceptanceError("this acceptance runner currently supports openai-chat only")
    service = manifest["service"]
    model = manifest["model"]["name"]
    timeout = manifest["acceptance"]["timeout_seconds"]
    started = time.monotonic()
    checks: dict[str, object] = {}

    upstream_health, _ = _get(_url(service["upstream_url"], service["health_path"]), timeout)
    checks["upstream_health"] = upstream_health
    if upstream_health != 200:
        raise AcceptanceError(f"upstream health returned {upstream_health}")
    gateway_health, _ = _get(_url(service["gateway_url"], "/healthz"), timeout)
    checks["gateway_health"] = gateway_health
    if gateway_health != 200:
        raise AcceptanceError(f"gateway health returned {gateway_health}")
    gateway_ready, _ = _get(_url(service["gateway_url"], "/readyz"), timeout)
    checks["gateway_ready"] = gateway_ready
    if gateway_ready != 200:
        raise AcceptanceError(f"gateway readiness returned {gateway_ready}")

    models_url = _url(service["gateway_url"], service["models_path"])
    unauth_status, _ = _get(models_url, timeout)
    checks["unauthenticated_models"] = unauth_status
    if unauth_status != 401:
        raise AcceptanceError(f"unauthenticated models returned {unauth_status}, expected 401")
    models_status, models_body = _get(models_url, timeout, key)
    checks["authenticated_models"] = models_status
    if models_status != 200:
        raise AcceptanceError(f"authenticated models returned {models_status}")
    try:
        listed = {item["id"] for item in json.loads(models_body).get("data", [])}
    except (ValueError, TypeError, KeyError) as exc:
        raise AcceptanceError("models response has an invalid shape") from exc
    if model not in listed:
        raise AcceptanceError(f"model {model} is absent from gateway /v1/models")

    endpoint = _url(service["gateway_url"], service["inference_path"])
    body = {"model": model, "messages": [{"role": "user", "content": manifest["acceptance"]["prompt"]}],
            "max_tokens": manifest["acceptance"]["max_tokens"], "temperature": 0,
            **manifest["acceptance"]["request_options"]}
    status, result = _post(endpoint, key, {**body, "stream": False}, timeout, stream=False)
    checks["non_stream"] = status
    if status != 200:
        raise AcceptanceError(f"non-stream request returned {status}")
    try:
        message = result["choices"][0]["message"]
        has_output = bool(message.get("content") or message.get("reasoning_content"))
    except (TypeError, KeyError, IndexError) as exc:
        raise AcceptanceError("non-stream response has an invalid shape") from exc
    if not has_output:
        raise AcceptanceError("non-stream response has no assistant output")
    if service["streaming"]:
        status, stream_result = _post(endpoint, key, {**body, "stream": True}, timeout, stream=True)
        checks["stream"] = status
        if status != 200 or not stream_result["done"] or not stream_result["has_output"]:
            raise AcceptanceError("streaming response did not complete with assistant output")
        checks["stream_events"] = stream_result["events"]

    return {"status": "passed", "model": model, "checks": checks,
            "elapsed_seconds": round(time.monotonic() - started, 3)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--key-file", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    report = {"timestamp_utc": datetime.now(timezone.utc).isoformat(),
              "manifest_sha256": None, "status": "failed"}
    try:
        manifest = load_manifest(args.manifest)
        report["manifest_sha256"] = hashlib.sha256(args.manifest.read_bytes()).hexdigest()
        key = args.key_file.read_text(encoding="utf-8").strip()
        if not key:
            raise AcceptanceError("key file is empty")
        report.update(run_acceptance(manifest, key))
    except (AcceptanceError, DeliveryError, OSError) as exc:
        report["error"] = str(exc)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
