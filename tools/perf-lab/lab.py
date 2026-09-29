#!/usr/bin/env python3
"""Small, reproducible vLLM/LMCache inference experiment runner.

The benchmark client uses only the Python standard library. vLLM and LMCache
are required only on the Linux host that runs ``serve``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
from importlib import metadata
import json
import math
import os
from pathlib import Path
import random
import signal
import socket
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


MODES = ("baseline", "prefix", "lmcache")
WORD_POOL = ("amber", "bridge", "cobalt", "delta", "ember", "forest",
             "garden", "harbor", "island", "juniper", "kernel", "lantern",
             "meadow", "nebula", "orange", "planet", "quartz", "river",
             "silver", "timber", "violet", "willow")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)


def gpu_snapshot() -> list[dict]:
    cmd = ["nvidia-smi", "--query-gpu=index,uuid,name,memory.total,memory.free,utilization.gpu",
           "--format=csv,noheader,nounits"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=10)
    gpus = []
    for line in result.stdout.splitlines():
        fields = [field.strip() for field in line.split(",", 5)]
        if len(fields) != 6:
            raise RuntimeError(f"Unexpected nvidia-smi GPU row: {line!r}")
        index, uuid, name, total, free, utilization = fields
        gpus.append({"index": int(index), "uuid": uuid, "name": name,
                     "total_mib": int(total), "free_mib": int(free),
                     "utilization_pct": int(utilization)})
    return gpus


def compute_processes() -> list[dict]:
    cmd = ["nvidia-smi", "--query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory",
           "--format=csv,noheader,nounits"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=10)
    processes = []
    for line in result.stdout.splitlines():
        if not line.strip() or "No running processes" in line:
            continue
        fields = [field.strip() for field in line.split(",", 3)]
        if len(fields) != 4:
            raise RuntimeError(f"Unexpected nvidia-smi process row: {line!r}")
        processes.append({"gpu_uuid": fields[0], "pid": fields[1],
                          "name": fields[2], "used_mib": fields[3]})
    return processes


def selected_gpu(index: int) -> dict:
    matches = [gpu for gpu in gpu_snapshot() if gpu["index"] == index]
    if not matches:
        raise RuntimeError(f"GPU {index} not found")
    return matches[0]


def ensure_idle_gpu(index: int, min_free_mib: int, max_util_pct: int) -> dict:
    gpu = selected_gpu(index)
    processes = [p for p in compute_processes() if p["gpu_uuid"] == gpu["uuid"]]
    problems = []
    if gpu["free_mib"] < min_free_mib:
        problems.append(f"free VRAM {gpu['free_mib']} MiB < {min_free_mib} MiB")
    if gpu["utilization_pct"] > max_util_pct:
        problems.append(f"utilization {gpu['utilization_pct']}% > {max_util_pct}%")
    if processes:
        problems.append(f"{len(processes)} compute process(es) already running")
    if problems:
        raise RuntimeError(f"GPU {index} is busy: " + "; ".join(problems))
    return gpu


def available_host_gib() -> float | None:
    meminfo = Path("/proc/meminfo")
    if not meminfo.exists():
        return None
    for line in meminfo.read_text(encoding="ascii").splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) / 1024 / 1024
    return None


def port_is_free(host: str, port: int) -> bool:
    with socket.socket() as sock:
        try:
            sock.bind((host, port))
            return True
        except OSError:
            return False


def http_json(url: str, payload: dict | None = None, timeout: int = 30) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = Request(url, data=data, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=timeout) as response:
        return json.load(response)


def available_model(url: str) -> str:
    data = http_json(url.rstrip("/") + "/v1/models")
    models = data.get("data") or []
    if not models or not models[0].get("id"):
        raise RuntimeError("/v1/models returned no model id")
    return models[0]["id"]


def metrics_lines(url: str) -> list[str]:
    try:
        with urlopen(url.rstrip("/") + "/metrics", timeout=5) as response:
            lines = response.read(2_000_000).decode("utf-8", "replace").splitlines()
        return [line for line in lines if not line.startswith("#") and
                ("prefix_cache" in line.lower() or "lmcache" in line.lower())][:150]
    except (HTTPError, URLError, TimeoutError):
        return []


def make_workload(seed: int, prefixes: int, suffixes: int,
                  prefix_words: int, evict_count: int) -> tuple[list[dict], list[dict], list[dict]]:
    if min(prefixes, suffixes, prefix_words) < 1 or evict_count < 0:
        raise ValueError("prefixes, suffixes and prefix_words must be positive; evict_count >= 0")
    rng = random.Random(seed)
    cold, warm, eviction = [], [], []
    for i in range(prefixes):
        # The unique marker is at the start so prefix-cache block hashes diverge.
        body = " ".join(rng.choices(WORD_POOL, k=prefix_words))
        shared = f"DOCUMENT_{seed}_{i}: {body}\nQuestion: "
        for j in range(suffixes):
            item = {"id": f"p{i}-s{j}", "prompt": shared +
                    f"Give one concise fact about document {i}, variant {j}."}
            (cold if j == 0 else warm).append(item)
    for i in range(evict_count):
        body = " ".join(rng.choices(WORD_POOL, k=prefix_words))
        eviction.append({"id": f"evict-{i}", "prompt":
                         f"UNRELATED_{seed}_{i}: {body}\nSummarize in one word."})
    return cold, eviction, warm


def send_stream(url: str, model: str, item: dict, max_tokens: int, timeout: int) -> dict:
    endpoint = url.rstrip("/") + "/v1/chat/completions"
    payload = {"model": model, "messages": [{"role": "user", "content": item["prompt"]}],
               "max_tokens": max_tokens, "temperature": 0, "stream": True,
               "stream_options": {"include_usage": True}}
    req = Request(endpoint, data=json.dumps(payload).encode("utf-8"),
                  headers={"Content-Type": "application/json", "Accept": "text/event-stream"})
    start = time.perf_counter()
    first_content = None
    usage = {}
    pieces = []
    try:
        with urlopen(req, timeout=timeout) as response:
            for raw in response:
                line = raw.decode("utf-8", "replace").strip()
                if not line.startswith("data:"):
                    continue
                value = line[5:].strip()
                if value == "[DONE]":
                    break
                event = json.loads(value)
                if event.get("error"):
                    raise RuntimeError(str(event["error"]))
                usage = event.get("usage") or usage
                for choice in event.get("choices") or []:
                    content = (choice.get("delta") or {}).get("content")
                    if content:
                        if first_content is None:
                            first_content = time.perf_counter()
                        pieces.append(content)
        end = time.perf_counter()
        if first_content is None:
            raise RuntimeError("No streamed content received")
        output_tokens = usage.get("completion_tokens")
        output_text = "".join(pieces)
        return {"id": item["id"], "ok": True, "ttft_s": first_content - start,
                "latency_s": end - start, "input_tokens": usage.get("prompt_tokens"),
                "output_tokens": output_tokens, "output_chars": len(output_text),
                "output_sha256": hashlib.sha256(output_text.encode("utf-8")).hexdigest(),
                "tpot_s": ((end - first_content) / (output_tokens - 1)
                           if isinstance(output_tokens, int) and output_tokens > 1 else None)}
    except Exception as exc:
        return {"id": item["id"], "ok": False, "error": str(exc),
                "latency_s": time.perf_counter() - start}


def phase(url: str, model: str, items: list[dict], max_tokens: int,
          timeout: int, concurrency: int) -> tuple[list[dict], float]:
    started = time.perf_counter()
    if concurrency == 1:
        rows = [send_stream(url, model, item, max_tokens, timeout) for item in items]
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            rows = list(pool.map(lambda item: send_stream(url, model, item, max_tokens, timeout), items))
    return rows, time.perf_counter() - started


def percentile(values: list[float], percent: int) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percent / 100
    low, high = math.floor(position), math.ceil(position)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def summarize(rows: list[dict], duration_s: float | None = None) -> dict:
    good = [row for row in rows if row["ok"]]
    ttfts = [row["ttft_s"] for row in good]
    latencies = [row["latency_s"] for row in good]
    tpots = [row["tpot_s"] for row in good if row.get("tpot_s") is not None]
    input_tokens = sum(row.get("input_tokens") or 0 for row in good)
    output_tokens = sum(row.get("output_tokens") or 0 for row in good)
    return {"requests": len(rows), "success": len(good), "failed": len(rows) - len(good),
            "duration_s": duration_s,
            "ttft_mean_s": statistics.mean(ttfts) if ttfts else None,
            "ttft_p50_s": percentile(ttfts, 50), "ttft_p95_s": percentile(ttfts, 95),
            "latency_p95_s": percentile(latencies, 95),
            "tpot_mean_s": statistics.mean(tpots) if tpots else None,
            "input_tokens": input_tokens, "output_tokens": output_tokens,
            "input_tokens_per_s": input_tokens / duration_s if duration_s else None,
            "output_tokens_per_s": output_tokens / duration_s if duration_s else None}


def package_version(name: str) -> str | None:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return None


def cmd_doctor(args: argparse.Namespace) -> None:
    gpu = selected_gpu(args.gpu_index)
    processes = [p for p in compute_processes() if p["gpu_uuid"] == gpu["uuid"]]
    print(json.dumps({"gpu": gpu, "compute_processes": processes,
                      "host_available_gib": available_host_gib(),
                      "python_version": sys.version.split()[0],
                      "vllm_version": package_version("vllm"),
                      "lmcache_version": package_version("lmcache")}, indent=2))
    ensure_idle_gpu(args.gpu_index, args.min_free_mib, args.max_util_pct)
    print("PASS: selected GPU is currently idle. Recheck immediately before launching.")


def cmd_serve(args: argparse.Namespace) -> None:
    if sys.platform != "linux":
        raise RuntimeError("serve requires Linux with NVIDIA drivers; benchmark client works anywhere")
    if not 0 < args.gpu_memory_utilization <= 0.7:
        raise ValueError("gpu-memory-utilization must be > 0 and <= 0.7 on a shared server")
    if not 0 < args.l1_size_gb <= 8:
        raise ValueError("l1-size-gb must be > 0 and <= 8 on a shared server")
    if not 1 <= args.max_num_seqs <= 8 or args.max_model_len < 128:
        raise ValueError("max-num-seqs must be 1..8 and max-model-len >= 128")
    gpu = ensure_idle_gpu(args.gpu_index, args.min_free_mib, args.max_util_pct)
    if args.mode == "lmcache":
        available = available_host_gib()
        if available is not None and available < args.l1_size_gb * 2:
            raise RuntimeError(f"Host available memory {available:.1f} GiB is too low for L1={args.l1_size_gb} GiB")
    for port in ([args.port, args.lmcache_port] if args.mode == "lmcache" else [args.port]):
        if not port_is_free("127.0.0.1", port):
            raise RuntimeError(f"localhost port {port} is already in use")
    env = os.environ.copy()
    env["CUDA_VISIBLE_DEVICES"] = gpu["uuid"]
    cmd = ["vllm", "serve", args.model, "--host", "127.0.0.1", "--port", str(args.port),
           "--gpu-memory-utilization", str(args.gpu_memory_utilization),
           "--max-model-len", str(args.max_model_len), "--max-num-seqs", str(args.max_num_seqs),
           "--enable-prefix-caching" if args.mode != "baseline" else "--no-enable-prefix-caching"]
    if args.mode == "lmcache":
        transfer = {"kv_connector": "LMCacheMPConnector", "kv_role": "kv_both",
                    "kv_connector_extra_config": {"lmcache.mp.host": "tcp://127.0.0.1",
                                                  "lmcache.mp.port": args.lmcache_port}}
        cmd += ["--disable-hybrid-kv-cache-manager", "--kv-transfer-config", json.dumps(transfer)]
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    children = []
    handles = []

    def stop_children() -> None:
        for child in reversed(children):
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        for child in reversed(children):
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait()
        for handle in handles:
            handle.close()

    def on_signal(_signum, _frame):
        raise KeyboardInterrupt

    old_term = signal.signal(signal.SIGTERM, on_signal)
    manifest = Path(args.server_meta)
    try:
        if args.mode == "lmcache":
            cache_log = log_dir / f"{stamp}-lmcache.log"
            handle = cache_log.open("w", encoding="utf-8")
            handles.append(handle)
            child = subprocess.Popen(["lmcache", "server", "--host", "127.0.0.1", "--port",
                                      str(args.lmcache_port), "--l1-size-gb", str(args.l1_size_gb)],
                                     stdout=handle, stderr=subprocess.STDOUT, env=env,
                                     start_new_session=True)
            children.append(child)
            time.sleep(2)
            if child.poll() is not None:
                raise RuntimeError(f"LMCache server exited; inspect {cache_log}")
        vllm_log = log_dir / f"{stamp}-{args.mode}-vllm.log"
        handle = vllm_log.open("w", encoding="utf-8")
        handles.append(handle)
        child = subprocess.Popen(cmd, stdout=handle, stderr=subprocess.STDOUT, env=env,
                                 start_new_session=True)
        children.append(child)
        url = f"http://127.0.0.1:{args.port}"
        print(f"GPU {gpu['index']} ({gpu['uuid']}); mode={args.mode}; waiting for {url}", flush=True)
        deadline = time.monotonic() + args.startup_timeout
        while time.monotonic() < deadline:
            if child.poll() is not None:
                raise RuntimeError(f"vLLM exited; inspect {vllm_log}")
            try:
                available_model(url)
                break
            except (URLError, HTTPError, TimeoutError, ValueError, RuntimeError):
                time.sleep(2)
        else:
            raise RuntimeError(f"vLLM did not become ready; inspect {vllm_log}")
        served_model = available_model(url)
        write_json(manifest, {"mode": args.mode, "model_argument": args.model,
                              "served_model": served_model, "url": url,
                              "gpu": gpu, "vllm_version": package_version("vllm"),
                              "lmcache_version": package_version("lmcache"),
                              "vllm_command": cmd, "gpu_memory_utilization": args.gpu_memory_utilization,
                              "max_model_len": args.max_model_len, "max_num_seqs": args.max_num_seqs,
                              "l1_size_gb": args.l1_size_gb if args.mode == "lmcache" else None,
                              "vllm_log": str(vllm_log), "started_at": utc_now()})
        print(f"READY {url}; run 'python3 lab.py run --mode {args.mode} --gpu-index {args.gpu_index}'", flush=True)
        print("Press Ctrl+C to stop both services.", flush=True)
        while child.poll() is None:
            time.sleep(1)
        raise RuntimeError(f"vLLM exited; inspect {vllm_log}")
    except KeyboardInterrupt:
        print("Stopping services.")
    finally:
        if manifest.exists():
            manifest.unlink()
        stop_children()
        signal.signal(signal.SIGTERM, old_term)


def cmd_run(args: argparse.Namespace) -> None:
    if not 1 <= args.concurrency <= 4:
        raise ValueError("concurrency must be 1..4 on a shared server")
    if args.max_tokens < 1 or args.timeout < 1:
        raise ValueError("max-tokens and timeout must be positive")
    model = available_model(args.url)
    meta_path = Path(args.server_meta)
    server_meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else None
    if server_meta is None and not args.allow_unmanaged:
        raise RuntimeError(f"No active server metadata at {meta_path}; start with 'serve' or pass --allow-unmanaged")
    if server_meta is not None:
        if server_meta["mode"] != args.mode or server_meta["served_model"] != model:
            raise RuntimeError("Run mode/model does not match the active server metadata")
        if server_meta["url"].rstrip("/") != args.url.rstrip("/"):
            raise RuntimeError("Run URL does not match the active server metadata")
        if args.gpu_index is not None and server_meta["gpu"]["index"] != args.gpu_index:
            raise RuntimeError("Run GPU does not match the active server metadata")
    cold, eviction, warm = make_workload(args.seed, args.prefixes, args.suffixes,
                                          args.prefix_words, args.evict_count)
    if not warm:
        raise ValueError("suffixes must be >= 2 to measure warm reuse")
    workload = {"seed": args.seed, "prefixes": args.prefixes, "suffixes": args.suffixes,
                "prefix_words": args.prefix_words, "evict_count": args.evict_count,
                "max_tokens": args.max_tokens, "concurrency": args.concurrency}
    all_items = cold + eviction + warm
    fingerprint = hashlib.sha256(json.dumps({"model": model, "workload": workload,
                                              "items": all_items}, sort_keys=True).encode()).hexdigest()
    gpu_before = selected_gpu(args.gpu_index) if args.gpu_index is not None else None
    processes_before = (compute_processes() if args.gpu_index is not None else None)
    metric_before = metrics_lines(args.url)
    started = utc_now()
    print(f"Model={model}; cold={len(cold)}, eviction pressure={len(eviction)}, warm={len(warm)}")
    results = {}
    summaries = {}
    metrics_by_phase = {}
    for name, items in (("cold", cold), ("eviction", eviction), ("warm", warm)):
        before_phase = metrics_lines(args.url)
        rows, duration_s = phase(args.url, model, items, args.max_tokens, args.timeout, args.concurrency)
        metrics_by_phase[name] = {"before": before_phase, "after": metrics_lines(args.url)}
        results[name] = rows
        summary = summarize(rows, duration_s)
        summaries[name] = summary
        print(f"{name}: {summary['success']}/{summary['requests']} succeeded; TTFT p50={summary['ttft_p50_s']}")
        if summary["failed"]:
            print("A request failed; stopping this run before subsequent phases.", file=sys.stderr)
            break
    output = {"schema_version": 1, "mode": args.mode, "started_at": started,
              "finished_at": utc_now(), "url": args.url, "model": model,
              "workload": workload, "fingerprint": fingerprint,
              "server_meta": server_meta,
              "gpu_before": gpu_before,
              "gpu_after": selected_gpu(args.gpu_index) if args.gpu_index is not None else None,
              "compute_processes_before": processes_before,
              "compute_processes_after": (compute_processes() if args.gpu_index is not None else None),
              "metrics_before": metric_before, "metrics_after": metrics_lines(args.url),
              "metrics_by_phase": metrics_by_phase,
              "results": results, "summaries": summaries,
              "limitations": ["Shared GPU: another user's workload may change during the run.",
                              "Eviction pressure is not proof that GPU KV blocks were evicted."]}
    write_json(Path(args.output), output)
    print(f"Saved {args.output}")
    if any(row["ok"] is False for rows in results.values() for row in rows):
        raise RuntimeError("One or more requests failed; see saved result JSON")


def cmd_report(args: argparse.Namespace) -> None:
    runs = [json.loads(Path(path).read_text(encoding="utf-8")) for path in args.inputs]
    fingerprints = {run["fingerprint"] for run in runs}
    modes = [run["mode"] for run in runs]
    if len(fingerprints) != 1:
        raise ValueError("Runs use different model, prompts, or workload settings")
    if len(modes) != len(set(modes)):
        raise ValueError("Only one result per mode may appear in a report")
    lines = ["# vLLM + LMCache performance comparison", "",
             f"Workload fingerprint: `{runs[0]['fingerprint']}`", "",
             "| Mode | Warm requests | TTFT p50 (ms) | TTFT p95 (ms) | TPOT mean (ms) | Latency p95 (ms) | Output tok/s |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    def ms(value):
        return "n/a" if value is None else f"{value * 1000:.1f}"
    def rate(value):
        return "n/a" if value is None else f"{value:.1f}"
    for run in runs:
        stat = run.get("summaries", {}).get("warm") or {}
        lines.append(f"| {run['mode']} | {stat.get('success', 0)}/{stat.get('requests', 0)} | "
                     f"{ms(stat.get('ttft_p50_s'))} | {ms(stat.get('ttft_p95_s'))} | "
                     f"{ms(stat.get('tpot_mean_s'))} | {ms(stat.get('latency_p95_s'))} | "
                     f"{rate(stat.get('output_tokens_per_s'))} |")
    lines += ["", "## Interpretation limits", "",
              "- All prompts and settings match by fingerprint; verify model weights and serving flags separately.",
              "- This is a shared server. Record competing GPU processes and repeat runs in randomized mode order.",
              "- Cache eviction is a workload pressure setting, not proof of a KV cache miss. Inspect saved /metrics samples.",
              "- Token lengths are recorded from server usage; `prefix_words` is a word count, not an exact token count."]
    if runs[0]["workload"]["evict_count"] == 0:
        lines.append("- No eviction pressure was used; LMCache versus GPU prefix-cache benefit is not isolated.")
    if any(run.get("server_meta") is None for run in runs):
        lines.append("- At least one run used an unmanaged server; its serving flags were not captured automatically.")
    managed = [run["server_meta"] for run in runs if run.get("server_meta")]
    for field in ("model_argument", "vllm_version", "gpu_memory_utilization", "max_model_len", "max_num_seqs"):
        if len({json.dumps(meta.get(field), sort_keys=True) for meta in managed}) > 1:
            lines.append(f"- Serving configuration differs across runs: `{field}`. Review before comparing.")
    gpu_uuids = {run["gpu_before"]["uuid"] for run in runs if run.get("gpu_before")}
    if len(gpu_uuids) > 1:
        lines.append("- Runs used different physical GPUs; hardware and competing load may differ.")
    if any(any(stat.get("failed", 0) for stat in run.get("summaries", {}).values()) for run in runs):
        lines.append("- At least one run has failed requests; do not use its numbers as a performance conclusion.")
    output = "\n".join(lines) + "\n"
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(output, encoding="utf-8")
    print(output)
    print(f"Saved {args.output}")


def _pct_change(baseline: float | None, candidate: float | None) -> float | None:
    if baseline is None or candidate is None or baseline == 0:
        return None
    return (candidate - baseline) / baseline * 100


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cmd_promote(args: argparse.Namespace) -> None:
    """Create review evidence only; never changes a running service or policy."""
    baseline_path, candidate_path = Path(args.baseline), Path(args.candidate)
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
    if baseline.get("fingerprint") != candidate.get("fingerprint"):
        raise ValueError("Baseline and candidate workload fingerprints differ")
    if baseline.get("model") != candidate.get("model"):
        raise ValueError("Baseline and candidate models differ")

    base_stat = (baseline.get("summaries") or {}).get("warm") or {}
    cand_stat = (candidate.get("summaries") or {}).get("warm") or {}
    ttft_change = _pct_change(base_stat.get("ttft_p95_s"), cand_stat.get("ttft_p95_s"))
    tpot_change = _pct_change(base_stat.get("tpot_mean_s"), cand_stat.get("tpot_mean_s"))
    throughput_change = _pct_change(base_stat.get("output_tokens_per_s"),
                                    cand_stat.get("output_tokens_per_s"))

    base_hashes = {row.get("id"): row.get("output_sha256")
                   for rows in (baseline.get("results") or {}).values()
                   for row in rows if row.get("ok")}
    cand_hashes = {row.get("id"): row.get("output_sha256")
                   for rows in (candidate.get("results") or {}).values()
                   for row in rows if row.get("ok")}
    common_ids = sorted(set(base_hashes) & set(cand_hashes))
    hashes_available = bool(common_ids) and all(base_hashes[item] and cand_hashes[item]
                                                 for item in common_ids)
    output_match = hashes_available and set(base_hashes) == set(cand_hashes) and all(
        base_hashes[item] == cand_hashes[item] for item in common_ids)
    no_failures = all(
        not stat.get("failed", 0)
        for run in (baseline, candidate)
        for stat in (run.get("summaries") or {}).values()
    )

    gates = {
        "same_workload_fingerprint": True,
        "no_failed_requests": no_failures,
        "deterministic_output_hashes_available": hashes_available,
        "deterministic_outputs_match": output_match,
        "throughput_improvement": (throughput_change is not None and
                                   throughput_change >= args.min_throughput_improvement_pct),
        "ttft_regression_within_limit": (ttft_change is not None and
                                         ttft_change <= args.max_ttft_regression_pct),
        "tpot_regression_within_limit": (tpot_change is not None and
                                         tpot_change <= args.max_tpot_regression_pct),
    }
    eligible = all(gates.values())
    manifest = {
        "schema_version": 1,
        "generated_at": utc_now(),
        "status": "eligible_for_review" if eligible else "rejected",
        "auto_apply": False,
        "candidate_profile": args.profile,
        "model": baseline.get("model"),
        "workload_fingerprint": baseline.get("fingerprint"),
        "evidence": {
            "baseline": {"path": str(baseline_path), "sha256": _file_sha256(baseline_path),
                         "mode": baseline.get("mode"), "server_meta": baseline.get("server_meta")},
            "candidate": {"path": str(candidate_path), "sha256": _file_sha256(candidate_path),
                          "mode": candidate.get("mode"), "server_meta": candidate.get("server_meta")},
        },
        "comparison": {
            "warm_ttft_p95_change_pct": ttft_change,
            "warm_tpot_mean_change_pct": tpot_change,
            "warm_output_throughput_change_pct": throughput_change,
            "deterministic_outputs_compared": len(common_ids),
        },
        "thresholds": {
            "min_throughput_improvement_pct": args.min_throughput_improvement_pct,
            "max_ttft_regression_pct": args.max_ttft_regression_pct,
            "max_tpot_regression_pct": args.max_tpot_regression_pct,
        },
        "gates": gates,
        "limitations": [
            "This manifest is review evidence and never changes the serving configuration.",
            "Matching deterministic output hashes are a regression gate, not a business-quality score.",
            "A production release still requires repeated interleaved runs and an application eval set.",
        ],
    }
    write_json(Path(args.output), manifest)
    print(json.dumps({"status": manifest["status"], "gates": gates}, indent=2))
    print(f"Saved {args.output}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    doctor = sub.add_parser("doctor", help="Check that a selected GPU is idle")
    doctor.add_argument("--gpu-index", type=int, required=True)
    doctor.add_argument("--min-free-mib", type=int, default=18000)
    doctor.add_argument("--max-util-pct", type=int, default=10)
    doctor.set_defaults(func=cmd_doctor)
    serve = sub.add_parser("serve", help="Run one serving mode on one idle GPU")
    serve.add_argument("--mode", choices=MODES, required=True)
    serve.add_argument("--model", required=True, help="Local model path or Hugging Face model ID")
    serve.add_argument("--gpu-index", type=int, required=True)
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--lmcache-port", type=int, default=5555)
    serve.add_argument("--l1-size-gb", type=int, default=4)
    serve.add_argument("--gpu-memory-utilization", type=float, default=0.55)
    serve.add_argument("--max-model-len", type=int, default=4096)
    serve.add_argument("--max-num-seqs", type=int, default=4)
    serve.add_argument("--min-free-mib", type=int, default=18000)
    serve.add_argument("--max-util-pct", type=int, default=10)
    serve.add_argument("--startup-timeout", type=int, default=600)
    serve.add_argument("--log-dir", default="results/logs")
    serve.add_argument("--server-meta", default="results/active_server.json")
    serve.set_defaults(func=cmd_serve)
    run = sub.add_parser("run", help="Run identical synthetic cold/warm workloads")
    run.add_argument("--mode", choices=MODES, required=True)
    run.add_argument("--url", default="http://127.0.0.1:8000")
    run.add_argument("--gpu-index", type=int)
    run.add_argument("--output", required=True)
    run.add_argument("--server-meta", default="results/active_server.json")
    run.add_argument("--allow-unmanaged", action="store_true", help="Allow external servers without captured launch flags")
    run.add_argument("--seed", type=int, default=42)
    run.add_argument("--prefixes", type=int, default=4)
    run.add_argument("--suffixes", type=int, default=3)
    run.add_argument("--prefix-words", type=int, default=1000)
    run.add_argument("--evict-count", type=int, default=0)
    run.add_argument("--max-tokens", type=int, default=32)
    run.add_argument("--concurrency", type=int, default=1)
    run.add_argument("--timeout", type=int, default=180)
    run.set_defaults(func=cmd_run)
    report = sub.add_parser("report", help="Compare runs with the same workload fingerprint")
    report.add_argument("inputs", nargs="+", help="One result JSON per mode")
    report.add_argument("--output", default="results/report.md")
    report.set_defaults(func=cmd_report)
    promote = sub.add_parser("promote", help="Create a review-only candidate evidence manifest")
    promote.add_argument("--baseline", required=True)
    promote.add_argument("--candidate", required=True)
    promote.add_argument("--profile", required=True)
    promote.add_argument("--output", default="results/experiment-manifest.json")
    promote.add_argument("--min-throughput-improvement-pct", type=float, default=5.0)
    promote.add_argument("--max-ttft-regression-pct", type=float, default=5.0)
    promote.add_argument("--max-tpot-regression-pct", type=float, default=5.0)
    promote.set_defaults(func=cmd_promote)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
        return 0
    except (ValueError, RuntimeError, OSError, subprocess.CalledProcessError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
