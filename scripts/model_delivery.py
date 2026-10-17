#!/usr/bin/env python3
"""Validate a model handoff and lock/verify its local artifact files.

Only JSON and the Python standard library are needed. `validate` checks a draft
without reading large model files. `lock` streams their hashes once; `verify`
checks every file again before a release candidate is accepted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit


PROJECT_ROOT = Path(__file__).resolve().parents[1]
HEX64 = re.compile(r"^[a-f0-9]{64}$")


class DeliveryError(ValueError):
    pass


def _object(value: object, name: str, keys: set[str]) -> dict:
    if not isinstance(value, dict) or set(value) != keys:
        raise DeliveryError(f"{name} must have exactly: {', '.join(sorted(keys))}")
    return value


def _string(value: object, name: str, nullable: bool = False) -> None:
    if nullable and value is None:
        return
    if not isinstance(value, str) or not value.strip():
        raise DeliveryError(f"{name} must be a non-empty string")


def _positive_int(value: object, name: str, maximum: int | None = None) -> None:
    if type(value) is not int or value < 1 or (maximum is not None and value > maximum):
        raise DeliveryError(f"{name} must be a positive integer")


def _lock_path(value: str) -> Path:
    path = (PROJECT_ROOT / value).resolve()
    if not path.is_relative_to(PROJECT_ROOT) or path.suffix != ".json":
        raise DeliveryError("artifact.lock_file must be a JSON path inside this project")
    return path


def load_manifest(path: Path) -> dict:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DeliveryError(f"cannot read model manifest: {exc}") from exc
    _object(manifest, "manifest", {"schema_version", "status", "model", "artifact",
                                   "runtime", "resources", "processing", "service", "acceptance"})
    if type(manifest["schema_version"]) is not int or manifest["schema_version"] != 1:
        raise DeliveryError("unsupported manifest schema_version")
    if not isinstance(manifest["status"], str) or manifest["status"] not in {"draft", "release_candidate"}:
        raise DeliveryError("status must be draft or release_candidate")

    model = _object(manifest["model"], "model", {"name", "version", "kind", "upstream_name"})
    for key in ("name", "version", "upstream_name"):
        _string(model[key], f"model.{key}")
    if not isinstance(model["kind"], str) or model["kind"] not in {"llm", "embedding", "rerank"}:
        raise DeliveryError("unsupported model.kind")

    artifact = _object(manifest["artifact"], "artifact", {"path", "lock_file"})
    for key in artifact:
        _string(artifact[key], f"artifact.{key}")
    if not PurePosixPath(artifact["path"]).is_absolute():
        raise DeliveryError("artifact.path must be absolute on the model host")
    _lock_path(artifact["lock_file"])

    runtime = _object(manifest["runtime"], "runtime", {"name", "version", "image", "image_digest"})
    if not isinstance(runtime["name"], str) or runtime["name"] not in {"vllm", "triton", "onnxruntime", "pytorch"}:
        raise DeliveryError("unsupported runtime.name")
    _string(runtime["version"], "runtime.version", nullable=True)
    _string(runtime["image"], "runtime.image", nullable=True)
    _string(runtime["image_digest"], "runtime.image_digest", nullable=True)
    if runtime["image_digest"] is not None and not re.fullmatch(r"sha256:[a-f0-9]{64}", runtime["image_digest"]):
        raise DeliveryError("runtime.image_digest must be a sha256 digest")

    resources = _object(manifest["resources"], "resources",
                        {"device", "gpu_count", "cpu_cores", "memory_gib"})
    if not isinstance(resources["device"], str) or resources["device"] not in {"cpu", "gpu"}:
        raise DeliveryError("resources.device must be cpu or gpu")
    if type(resources["gpu_count"]) is not int or resources["gpu_count"] < 0:
        raise DeliveryError("resources.gpu_count must be a non-negative integer")
    if (resources["device"] == "cpu") != (resources["gpu_count"] == 0):
        raise DeliveryError("CPU models need 0 GPUs; GPU models need at least 1")
    if resources["cpu_cores"] is not None:
        _positive_int(resources["cpu_cores"], "resources.cpu_cores")
    if resources["memory_gib"] is not None and (type(resources["memory_gib"]) not in (int, float)
                                                or resources["memory_gib"] <= 0):
        raise DeliveryError("resources.memory_gib must be positive")

    processing = _object(manifest["processing"], "processing",
                         {"preprocessor_version", "postprocessor_version"})
    for key in processing:
        _string(processing[key], f"processing.{key}", nullable=True)

    service = _object(manifest["service"], "service",
                      {"gateway_url", "upstream_url", "health_path", "models_path",
                       "inference_path", "protocol", "streaming"})
    for key in ("gateway_url", "upstream_url"):
        parsed = urlsplit(service[key]) if isinstance(service[key], str) else None
        if (parsed is None or parsed.scheme not in {"http", "https"} or not parsed.netloc
                or parsed.username is not None or parsed.password is not None
                or parsed.path not in {"", "/"} or parsed.query or parsed.fragment):
            raise DeliveryError(f"service.{key} must be an HTTP base URL")
        try:
            if parsed.port is not None and parsed.port < 1:
                raise ValueError("invalid port")
        except ValueError as exc:
            raise DeliveryError(f"service.{key} has an invalid port") from exc
    for key in ("health_path", "models_path", "inference_path"):
        if not isinstance(service[key], str) or not service[key].startswith("/"):
            raise DeliveryError(f"service.{key} must be an absolute URL path")
    if not isinstance(service["protocol"], str) or service["protocol"] not in {"openai-chat", "openai-embeddings", "triton-http", "grpc"}:
        raise DeliveryError("unsupported service.protocol")
    if type(service["streaming"]) is not bool:
        raise DeliveryError("service.streaming must be boolean")
    if service["protocol"] == "openai-chat" and model["kind"] != "llm":
        raise DeliveryError("openai-chat requires an llm model")

    acceptance = _object(manifest["acceptance"], "acceptance",
                         {"prompt", "max_tokens", "timeout_seconds", "request_options"})
    _string(acceptance["prompt"], "acceptance.prompt")
    _positive_int(acceptance["max_tokens"], "acceptance.max_tokens", 256)
    _positive_int(acceptance["timeout_seconds"], "acceptance.timeout_seconds", 300)
    options = acceptance["request_options"]
    if not isinstance(options, dict) or any(key in options for key in
                                            ("model", "messages", "stream", "max_tokens", "cache_salt", "user")):
        raise DeliveryError("acceptance.request_options must not override protected fields")
    return manifest


def _files(root: Path) -> list[Path]:
    if not root.is_dir():
        raise DeliveryError(f"model directory does not exist: {root}")
    paths = sorted(root.rglob("*"))
    if any(path.is_symlink() for path in paths):
        raise DeliveryError("model directory contains symlinks; materialize files before locking")
    files = [path for path in paths if path.is_file()]
    if not files:
        raise DeliveryError("model directory contains no files")
    return files


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _content_digest(files: list[dict]) -> str:
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def make_lock(manifest: dict, *, force: bool = False) -> Path:
    root = Path(manifest["artifact"]["path"]).resolve()
    destination = _lock_path(manifest["artifact"]["lock_file"])
    if destination.exists() and not force:
        raise DeliveryError(f"lock already exists: {destination} (pass --force to replace it)")
    files = [{"path": path.relative_to(root).as_posix(), "size": path.stat().st_size,
              "sha256": _sha256(path)} for path in _files(root)]
    lock = {"schema_version": 1, "model": manifest["model"]["name"],
            "artifact_path": str(root), "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "files": files, "content_sha256": _content_digest(files)}
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".json.tmp")
    try:
        temporary.write_text(json.dumps(lock, indent=2) + "\n", encoding="utf-8")
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def verify_lock(manifest: dict) -> dict:
    root = Path(manifest["artifact"]["path"]).resolve()
    path = _lock_path(manifest["artifact"]["lock_file"])
    try:
        lock = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DeliveryError(f"cannot read artifact lock: {exc}") from exc
    if lock.get("schema_version") != 1 or lock.get("model") != manifest["model"]["name"] or lock.get("artifact_path") != str(root):
        raise DeliveryError("artifact lock does not match this model/path")
    expected = lock.get("files")
    if (not isinstance(expected, list) or not expected or
            not all(isinstance(entry, dict) and isinstance(entry.get("path"), str) for entry in expected) or
            lock.get("content_sha256") != _content_digest(expected)):
        raise DeliveryError("artifact lock has an invalid file list or content digest")
    actual_paths = {path.relative_to(root).as_posix(): path for path in _files(root)}
    locked_paths = [entry.get("path") for entry in expected if isinstance(entry, dict)]
    if len(locked_paths) != len(expected) or set(actual_paths) != set(locked_paths) or len(set(locked_paths)) != len(locked_paths):
        raise DeliveryError("artifact files were added, removed, or duplicated after locking")
    for entry in expected:
        name = entry["path"]
        if not isinstance(name, str) or not HEX64.fullmatch(str(entry.get("sha256", ""))):
            raise DeliveryError(f"invalid lock entry: {name}")
        file = actual_paths[name]
        if file.stat().st_size != entry.get("size") or _sha256(file) != entry["sha256"]:
            raise DeliveryError(f"artifact file changed: {name}")
    return lock


def check_release(manifest: dict) -> dict:
    if manifest["status"] != "release_candidate":
        raise DeliveryError("manifest must be marked release_candidate")
    if manifest["service"]["protocol"] != "openai-chat":
        raise DeliveryError("only openai-chat currently has an automated acceptance gate")
    for section, key in (("runtime", "version"), ("runtime", "image"),
                         ("runtime", "image_digest"),
                         ("resources", "cpu_cores"), ("resources", "memory_gib"),
                         ("processing", "preprocessor_version"),
                         ("processing", "postprocessor_version")):
        if manifest[section][key] is None:
            raise DeliveryError(f"{section}.{key} is required for release")
    return verify_lock(manifest)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("validate", "lock", "verify"):
        command = sub.add_parser(name)
        command.add_argument("--manifest", type=Path, required=True)
        if name == "validate":
            command.add_argument("--release", action="store_true")
        if name == "lock":
            command.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    try:
        manifest = load_manifest(args.manifest)
        if args.command == "lock":
            print(f"locked: {make_lock(manifest, force=args.force)}")
        elif args.command == "verify":
            lock = verify_lock(manifest)
            print(f"verified: {manifest['model']['name']} ({len(lock['files'])} files)")
        elif args.release:
            lock = check_release(manifest)
            print(f"release candidate verified: {lock['content_sha256']}")
        else:
            print(f"draft structure valid: {manifest['model']['name']}")
        return 0
    except DeliveryError as exc:
        parser.exit(1, f"model delivery failed: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
