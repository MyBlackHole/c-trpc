#!/usr/bin/env python3
"""Run the RPC V2 TLS × Message-compression comparison in a worktree output directory."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--scenario", choices=("small", "bulk", "mixed"), default="bulk")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--requests", type=int, default=1000)
    args = parser.parse_args()
    if not 1 <= args.trials <= 20 or not 32 <= args.requests <= 1000000:
        parser.error("trials must be 1..20 and requests must be 32..1000000")
    binary = args.binary.resolve(strict=True)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    tls_dir = output_dir / "tls-identity"
    tls_dir.mkdir(parents=True, exist_ok=True)
    certificate = tls_dir / "server.pem"
    private_key = tls_dir / "server-key.pem"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-nodes",
        "-days", "7", "-subj", "/CN=server.example.test",
        "-addext", "subjectAltName=DNS:server.example.test",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,digitalSignature,keyEncipherment",
        "-addext", "extendedKeyUsage=serverAuth",
        "-keyout", str(private_key), "-out", str(certificate)
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    runner = Path(__file__).with_name("run_rpc_bench.py").resolve(strict=True)
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    for tls in (False, True):
        for compression in ("none", "zstd"):
            label = f"{args.scenario}-tls-{int(tls)}-{compression}"
            command = [
                sys.executable, str(runner), "--binary", str(binary),
                "--output", str(output_dir / f"{label}.jsonl"),
                "--label", label, "--trials", str(args.trials),
                "--requests", str(args.requests), "--scenario", args.scenario,
                "--compression", compression,
            ]
            if tls:
                command.extend([
                    "--tls", "--tls-ca", str(certificate),
                    "--tls-cert", str(certificate), "--tls-key", str(private_key)
                ])
            subprocess.run(command, check=True, env=env)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"compression matrix failed: {error}", file=sys.stderr)
        raise SystemExit(1)
