#!/usr/bin/env python3
"""Run a bounded, repeatable smoke load against the authenticated gateway."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def run_one(index: int, base_url: str, key: str, model: str, max_tokens: int) -> dict:
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": f"请用一句中文说明推理服务的用途。编号 {index}"}],
        "temperature": 0,
        "max_tokens": max_tokens,
        "chat_template_kwargs": {"enable_thinking": False},
    }
    request = Request(
        base_url.rstrip("/") + "/v1/chat/completions",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
    )
    started = time.monotonic()
    try:
        with urlopen(request, timeout=120) as response:
            result = json.load(response)
            return {
                "index": index,
                "status": response.status,
                "latency_seconds": round(time.monotonic() - started, 3),
                "output_tokens": result.get("usage", {}).get("completion_tokens", 0),
                "answer_nonempty": bool(result["choices"][0]["message"].get("content")),
            }
    except (HTTPError, URLError, TimeoutError, ValueError, KeyError) as error:
        return {
            "index": index,
            "status": getattr(error, "code", 0),
            "latency_seconds": round(time.monotonic() - started, 3),
            "error": type(error).__name__,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--key-file", type=Path, required=True)
    parser.add_argument("--model", default="local-model")
    parser.add_argument("--requests", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--max-tokens", type=int, default=96)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1 or args.max_tokens < 1:
        parser.error("requests, concurrency and max-tokens must be positive")
    key = args.key_file.read_text(encoding="utf-8").strip()
    if not key:
        parser.error("key file is empty")
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(run_one, i, args.base_url, key, args.model, args.max_tokens)
                   for i in range(args.requests)]
        results = sorted((future.result() for future in as_completed(futures)), key=lambda item: item["index"])
    elapsed = time.monotonic() - started
    good = [item for item in results if item.get("status") == 200 and item.get("answer_nonempty")]
    latencies = sorted(item["latency_seconds"] for item in good)
    output_tokens = sum(item["output_tokens"] for item in good)
    report = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "model": args.model,
        "request_count": args.requests,
        "concurrency": args.concurrency,
        "success_count": len(good),
        "elapsed_seconds": round(elapsed, 3),
        "output_tokens": output_tokens,
        "output_tokens_per_second": round(output_tokens / elapsed, 2),
        "latency_p50_seconds": round(statistics.median(latencies), 3) if latencies else None,
        "latency_p95_seconds": latencies[max(0, int(len(latencies) * 0.95 + 0.999) - 1)] if latencies else None,
        "results": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key != "results"}, ensure_ascii=False, indent=2))
    raise SystemExit(0 if len(good) == args.requests else 1)


if __name__ == "__main__":
    main()
