#!/usr/bin/env python3
"""Create a random InferGate API key and its JSON configuration entry."""

import argparse
import hashlib
import json
import re
import secrets


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tenant", help="letters, digits, underscore and hyphen only")
    parser.add_argument("--rpm", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=2)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,48}", args.tenant):
        parser.error("invalid tenant name")
    if args.rpm < 1 or args.concurrency < 1:
        parser.error("rpm and concurrency must be positive")
    token = secrets.token_urlsafe(32)
    print("API key (copy once):", token)
    print("Add this entry to config/keys.json:")
    print(json.dumps({"tenant": args.tenant, "sha256": hashlib.sha256(token.encode()).hexdigest(),
                      "rpm": args.rpm, "concurrency": args.concurrency}, indent=2))


if __name__ == "__main__":
    main()
