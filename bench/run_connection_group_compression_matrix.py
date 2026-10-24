#!/usr/bin/env python3
"""Run verified TLS × compression diagnostics over public connection-group DATA."""
from __future__ import annotations

import math
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import selectors
import socket
import subprocess
import sys
import threading
import time
from typing import Any
import weakref


SCENARIOS = ("small", "bulk-compressible", "bulk-incompressible", "mixed")
COMPRESSIONS = ("none", "zstd")
MAX_BENCH_ALLOCATION_COUNTER = 1 << 60
_stdout_line_buffers: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def expected_matrix_cases(*, trials: int = 3, requests: int = 1000) -> list[dict[str, Any]]:
    if type(trials) is not int or not 1 <= trials <= 20:
        raise ValueError("trials must be in 1..20")
    if type(requests) is not int or not 32 <= requests <= 100000:
        raise ValueError("requests must be in 32..100000")
    return [
        {"scenario": scenario, "clients": clients, "requests": requests,
         "tls": tls, "compression": compression, "trial": trial}
        for trial in range(1, trials + 1)
        for scenario in SCENARIOS
        for tls in (False, True)
        for compression in COMPRESSIONS
        for clients in (1, 2)
    ]


def _number(row: dict[str, Any], key: str, *, positive: bool = False) -> float:
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{key}: invalid measurement type")
    value = float(value)
    if not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError(f"{key}: measurement must be finite and nonnegative")
    return value


def _integer(row: dict[str, Any], key: str) -> int:
    value = row.get(key)
    if type(value) is not int or value < 0:
        raise ValueError(f"{key}: invalid counter")
    return value


def _allocation_counter(row: dict[str, Any], key: str) -> int:
    value = _integer(row, key)
    if value > MAX_BENCH_ALLOCATION_COUNTER:
        raise ValueError(f"{key}: allocation counter exceeds benchmark bound")
    return value


def validate_case(case: dict[str, Any], clients: list[dict[str, Any]],
                  server: dict[str, Any]) -> None:
    """Reject incomplete or invalid measurements before they enter summaries."""
    expected_clients = case.get("clients")
    requests = case.get("requests")
    if type(expected_clients) is not int or expected_clients not in (1, 2):
        raise ValueError("case has invalid client count")
    if type(requests) is not int or requests <= 0:
        raise ValueError("case has invalid request count")
    if len(clients) != expected_clients:
        raise ValueError("client process count mismatch")

    seen: set[int] = set()
    for row in clients:
        if (row.get("type") != "client_phase" or row.get("schema") != 1 or
                row.get("api_generation") != "connection_group_v2" or
                row.get("phase") != "measure"):
            raise ValueError("invalid client phase/schema")
        client_index = _integer(row, "client_index")
        if client_index >= expected_clients or client_index in seen:
            raise ValueError("duplicate or unknown client index")
        seen.add(client_index)
        attempted = _integer(row, "attempted")
        accepted = _integer(row, "accepted")
        submit_again = _integer(row, "submit_again")
        submit_errors = _integer(row, "submit_errors")
        if attempted != accepted + submit_again + submit_errors:
            raise ValueError("unaccounted client submissions")
        if accepted != requests or submit_errors:
            raise ValueError("client did not accept every requested message")
        elapsed = _number(row, "elapsed_s", positive=True)
        start_ns = _integer(row, "start_ns")
        end_ns = _integer(row, "end_ns")
        if end_ns <= start_ns:
            raise ValueError("client interval is invalid")
        if not math.isclose(elapsed, (end_ns - start_ns) / 1e9,
                            rel_tol=1e-6, abs_tol=1e-9):
            raise ValueError("client interval does not match elapsed time")
        accepted_rps = _number(row, "accepted_rps", positive=True)
        if not math.isclose(accepted_rps, accepted / elapsed, rel_tol=0.02):
            raise ValueError("client throughput does not match its interval")
        for key in ("client_cpu_s", "client_peak_rss_kib"):
            _number(row, key)
        _allocation_counter(row, "client_alloc_calls")
        _allocation_counter(row, "client_alloc_requested_bytes")
    if seen != set(range(expected_clients)):
        raise ValueError("missing client process result")

    if (server.get("type") != "server_phase" or server.get("schema") != 1 or
            server.get("api_generation") != "connection_group_v2" or
            server.get("phase") != "measure"):
        raise ValueError("invalid server phase/schema")
    total_expected = expected_clients * requests
    if (_integer(server, "expected") != total_expected or
            _integer(server, "received") != total_expected):
        raise ValueError("server completion count mismatch")
    if (_integer(server, "verified") != total_expected or
            _integer(server, "invalid") != 0):
        raise ValueError("payload validation failed")
    elapsed = _number(server, "elapsed_s", positive=True)
    receive_rps = _number(server, "receive_rps", positive=True)
    if not math.isclose(receive_rps, total_expected / elapsed, rel_tol=0.02):
        raise ValueError("server throughput does not match its interval")
    p50 = _number(server, "latency_p50_us")
    p99 = _number(server, "latency_p99_us")
    if p50 > p99:
        raise ValueError("unordered server latency percentiles")
    for key in ("server_cpu_s", "server_peak_rss_kib"):
        _number(server, key)
    _allocation_counter(server, "server_alloc_calls")
    _allocation_counter(server, "server_alloc_requested_bytes")


class TcpByteProxy:
    """Loopback TCP forwarder counting successful bytes in each direction."""

    def __init__(self, backend_host: str, backend_port: int):
        self._backend = (backend_host, backend_port)
        self._listener: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._stopping = threading.Event()
        self._lock = threading.Lock()
        self._bytes_client_to_server = 0
        self._bytes_server_to_client = 0
        self._active: set[socket.socket] = set()
        self._connections: list[threading.Thread] = []

    def start(self) -> int:
        if self._listener is not None:
            raise RuntimeError("proxy already started")
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(64)
        listener.settimeout(0.2)
        self._listener = listener
        self._accept_thread = threading.Thread(target=self._accept_loop,
                                               name="group-byte-proxy", daemon=True)
        self._accept_thread.start()
        return listener.getsockname()[1]

    def snapshot(self) -> tuple[int, int]:
        with self._lock:
            return self._bytes_client_to_server, self._bytes_server_to_client

    def close(self) -> None:
        self._stopping.set()
        listener = self._listener
        self._listener = None
        if listener is not None:
            listener.close()
        with self._lock:
            active = tuple(self._active)
        for connection in active:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                connection.close()
            except OSError:
                pass
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=2)
            self._accept_thread = None
        for thread in self._connections:
            thread.join(timeout=2)
        self._connections.clear()

    def _accept_loop(self) -> None:
        listener = self._listener
        assert listener is not None
        while not self._stopping.is_set():
            try:
                client, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                upstream = socket.create_connection(self._backend, timeout=5)
            except OSError:
                client.close()
                continue
            with self._lock:
                self._active.add(client)
                self._active.add(upstream)
            thread = threading.Thread(target=self._relay_pair, args=(client, upstream),
                                      name="group-byte-relay", daemon=True)
            self._connections.append(thread)
            thread.start()

    def _relay_direction(self, source: socket.socket, destination: socket.socket,
                          client_to_server: bool) -> None:
        try:
            while not self._stopping.is_set():
                chunk = source.recv(65536)
                if not chunk:
                    break
                destination.sendall(chunk)
                with self._lock:
                    if client_to_server:
                        self._bytes_client_to_server += len(chunk)
                    else:
                        self._bytes_server_to_client += len(chunk)
        except OSError:
            pass
        finally:
            try:
                destination.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    def _relay_pair(self, client: socket.socket, upstream: socket.socket) -> None:
        client_to_server = threading.Thread(
            target=self._relay_direction, args=(client, upstream, True), daemon=True)
        server_to_client = threading.Thread(
            target=self._relay_direction, args=(upstream, client, False), daemon=True)
        client_to_server.start()
        server_to_client.start()
        client_to_server.join()
        server_to_client.join()
        client.close()
        upstream.close()
        with self._lock:
            self._active.discard(client)
            self._active.discard(upstream)


def _read_json_line(process: subprocess.Popen[str], expected_type: str,
                    timeout_s: float = 30.0,
                    records: list[str] | None = None) -> dict[str, Any]:
    if process.stdout is None:
        raise RuntimeError(f"{expected_type}: stdout pipe is unavailable")
    pending = _stdout_line_buffers.setdefault(process, bytearray())
    deadline = time.monotonic() + timeout_s
    while True:
        newline = pending.find(b"\n")
        if newline >= 0:
            line_bytes = bytes(pending[:newline])
            del pending[:newline + 1]
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            raise TimeoutError(f"{expected_type}: timed out waiting for output")
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            if not selector.select(remaining):
                raise TimeoutError(f"{expected_type}: timed out waiting for output")
        chunk = os.read(process.stdout.fileno(), 65536)
        if not chunk:
            raise RuntimeError(f"{expected_type}: process exited without output")
        pending.extend(chunk)
    try:
        line = line_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        raise RuntimeError(f"{expected_type}: output is not UTF-8") from error
    if records is not None:
        records.append(line)
    try:
        row = json.loads(line)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"{expected_type}: invalid JSON line {line!r}") from error
    if row.get("type") != expected_type:
        raise RuntimeError(f"expected {expected_type}, received {row.get('type')!r}")
    if row.get("schema") != 1 or row.get("api_generation") != "connection_group_v2":
        raise RuntimeError(f"{expected_type}: wrong schema/generation")
    return row


def _write_gate(process: subprocess.Popen[str], gate: str) -> None:
    if process.stdin is None:
        raise RuntimeError("child stdin pipe is unavailable")
    process.stdin.write(gate + "\n")
    process.stdin.flush()


def _release_server_transfers_before_client_shutdown(
        server: subprocess.Popen[str], clients: list[subprocess.Popen[str]],
        records: list[str]) -> None:
    row = _read_json_line(server, "server_release_ready", records=records)
    if row.get("released_transfers") != len(clients):
        raise ValueError("server did not release every DATA transfer")
    for child in clients:
        _write_gate(child, "Q")


def _stop_owned(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _client_command(binary: Path, case: dict[str, Any], index: int,
                    port: int, warmup: int, tls: dict[str, Path] | None) -> list[str]:
    command = [
        str(binary), "client", "--host", "127.0.0.1", "--port", str(port),
        "--index", str(index), "--warmup", str(warmup),
        "--requests", str(case["requests"]), "--scenario", case["scenario"],
        "--compression", case["compression"], "--tls", str(int(case["tls"])),
    ]
    if tls:
        command.extend([
            "--ca", str(tls["ca"]),
            "--client-cert", str(tls["client_cert"]),
            "--client-key", str(tls["client_key"]),
        ])
    return command


def _server_command(binary: Path, case: dict[str, Any], warmup: int,
                    tls: dict[str, Path] | None) -> list[str]:
    command = [
        str(binary), "server", "--port", "0", "--clients", str(case["clients"]),
        "--warmup", str(warmup), "--requests", str(case["requests"]),
        "--scenario", case["scenario"], "--compression", case["compression"],
        "--tls", str(int(case["tls"])),
    ]
    if tls:
        command.extend([
            "--ca", str(tls["ca"]),
            "--server-cert", str(tls["server_cert"]),
            "--server-key", str(tls["server_key"]),
        ])
    return command


def run_case(binary: Path, case: dict[str, Any], warmup: int,
             log_dir: Path, tls: dict[str, Path] | None) -> dict[str, Any]:
    log_dir.mkdir(parents=True, exist_ok=True)
    children: list[subprocess.Popen[str]] = []
    logs = []
    protocol_records: dict[str, list[str]] = {"server": []}
    proxy: TcpByteProxy | None = None
    start_time = time.monotonic()
    try:
        server_log = (log_dir / "server.stderr").open("w", encoding="utf-8")
        logs.append(server_log)
        server_command = _server_command(binary, case, warmup, tls)
        server = subprocess.Popen(
            server_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=server_log, text=True, bufsize=1,
        )
        children.append(server)
        ready = _read_json_line(server, "server_ready",
                                records=protocol_records["server"])
        if ready.get("pid") != server.pid or not 0 < ready.get("port", 0) <= 65535:
            raise ValueError("server readiness identity/port is invalid")

        proxy = TcpByteProxy("127.0.0.1", ready["port"])
        proxy_port = proxy.start()
        clients: list[subprocess.Popen[str]] = []
        client_commands: list[list[str]] = []
        for index in range(case["clients"]):
            client_log = (log_dir / f"client-{index}.stderr").open("w", encoding="utf-8")
            logs.append(client_log)
            command = _client_command(binary, case, index, proxy_port, warmup, tls)
            child = subprocess.Popen(
                command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=client_log, text=True, bufsize=1,
            )
            children.append(child)
            clients.append(child)
            client_commands.append(command)
            protocol_records[f"client-{index}"] = []

        client_ready = []
        for index, child in enumerate(clients):
            row = _read_json_line(
                child, "client_ready",
                records=protocol_records[f"client-{index}"])
            if row.get("pid") != child.pid:
                raise ValueError("client readiness PID mismatch")
            client_ready.append(row)
        for child in clients:
            _write_gate(child, "W")
        for index, child in enumerate(clients):
            phase_ready = _read_json_line(
                child, "client_phase_ready",
                records=protocol_records[f"client-{index}"])
            if phase_ready.get("phase") != "warmup_sent":
                raise ValueError("client warmup phase did not finish")
        server_warmup = _read_json_line(
            server, "server_phase_ready", records=protocol_records["server"])
        if server_warmup.get("phase") != "warmup_done":
            raise ValueError("server warmup phase did not finish")

        bytes_before = proxy.snapshot()
        for child in clients:
            _write_gate(child, "M")
        client_phases = [
            _read_json_line(child, "client_phase",
                            records=protocol_records[f"client-{index}"])
            for index, child in enumerate(clients)
        ]
        server_phase = _read_json_line(
            server, "server_phase", timeout_s=120.0,
            records=protocol_records["server"])
        bytes_after = proxy.snapshot()
        tcp_client_to_server = bytes_after[0] - bytes_before[0]
        tcp_server_to_client = bytes_after[1] - bytes_before[1]
        total_messages = case["clients"] * case["requests"]
        server_phase["tcp_client_to_server_bytes"] = tcp_client_to_server
        server_phase["tcp_server_to_client_bytes"] = tcp_server_to_client
        server_phase["tcp_total_bytes"] = tcp_client_to_server + tcp_server_to_client
        server_phase["tcp_bytes_per_message"] = (
            (tcp_client_to_server + tcp_server_to_client) / total_messages
        )

        starts = [row["start_ns"] for row in client_phases]
        ends = [row["end_ns"] for row in client_phases]
        if len(client_phases) == 2 and max(starts) >= min(ends):
            raise ValueError("two-client measurement intervals did not overlap")
        validate_case(case, client_phases, server_phase)

        _release_server_transfers_before_client_shutdown(
            server, clients, protocol_records["server"])
        for index, child in enumerate(clients):
            remaining, _ = child.communicate(timeout=30)
            protocol_records[f"client-{index}"].extend(
                line for line in remaining.splitlines() if line)
            if child.returncode != 0:
                raise RuntimeError(f"client {index} exited {child.returncode}")
            (log_dir / f"client-{index}.stdout-rest").write_text(remaining,
                                                               encoding="utf-8")
        _write_gate(server, "Q")
        server_remaining, _ = server.communicate(timeout=30)
        protocol_records["server"].extend(
            line for line in server_remaining.splitlines() if line)
        if server.returncode != 0:
            raise RuntimeError(f"server exited {server.returncode}")
        server_exit_lines = [line for line in server_remaining.splitlines() if line]
        if len(server_exit_lines) != 1:
            raise ValueError("server exit record is missing or duplicated")
        server_exit = json.loads(server_exit_lines[0])
        if (server_exit.get("type") != "server_exit" or
                server_exit.get("schema") != 1 or
                server_exit.get("api_generation") != "connection_group_v2" or
                server_exit.get("drain_status") != 0 or
                server_exit.get("received") != warmup * case["clients"] + total_messages or
                server_exit.get("verified") != warmup * case["clients"] + total_messages or
                server_exit.get("invalid") != 0):
            raise ValueError("server did not verify all warmup and measured DATA")
        return {
            "type": "case", "schema": 1,
            "api_generation": "connection_group_v2",
            "case": case,
            "server_command": server_command,
            "client_commands": client_commands,
            "server_ready": ready,
            "client_ready": client_ready,
            "server_exit": server_exit,
            "clients": client_phases,
            "server": server_phase,
            "runner_elapsed_s": time.monotonic() - start_time,
        }
    finally:
        (log_dir / "protocol.jsonl").write_text(
            "".join(f"{name}\t{line}\n"
                    for name, records in protocol_records.items()
                    for line in records),
            encoding="utf-8")
        if proxy is not None:
            to_server, to_client = proxy.snapshot()
            (log_dir / "tcp-bytes.json").write_text(
                json.dumps({"client_to_server": to_server,
                            "server_to_client": to_client}, sort_keys=True) + "\n",
                encoding="utf-8")
        for child in reversed(children):
            _stop_owned(child)
            for pipe in (child.stdin, child.stdout):
                if pipe is not None:
                    pipe.close()
        if proxy is not None:
            proxy.close()
        for log in logs:
            log.close()


def _openssl(command: list[str], env: dict[str, str]) -> None:
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL,
                   stderr=subprocess.PIPE, env=env)


def generate_tls_identity(directory: Path, env: dict[str, str]) -> dict[str, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    ca_key = directory / "ca-key.pem"
    ca_cert = directory / "ca.pem"
    server_key = directory / "server-key.pem"
    server_csr = directory / "server.csr"
    server_cert = directory / "server.pem"
    client_key = directory / "client-key.pem"
    client_csr = directory / "client.csr"
    client_cert = directory / "client.pem"
    server_ext = directory / "server.ext"
    client_ext = directory / "client.ext"
    _openssl([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-sha256",
        "-nodes", "-days", "2", "-subj", "/CN=c-trpc-group-bench-ca",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign",
        "-keyout", str(ca_key), "-out", str(ca_cert),
    ], env)
    for key, csr, common_name in (
        (server_key, server_csr, "server.example.test"),
        (client_key, client_csr, "connection-group-bench-client"),
    ):
        _openssl([
            "openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes",
            "-sha256", "-subj", f"/CN={common_name}",
            "-keyout", str(key), "-out", str(csr),
        ], env)
    server_ext.write_text(
        "subjectAltName=DNS:server.example.test,"
        "URI:spiffe://c-trpc.example/server\n"
        "basicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\n", encoding="ascii")
    client_ext.write_text(
        "subjectAltName=URI:spiffe://c-trpc.example/bench\n"
        "basicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature\n"
        "extendedKeyUsage=clientAuth\n", encoding="ascii")
    for csr, certificate, extension in (
        (server_csr, server_cert, server_ext),
        (client_csr, client_cert, client_ext),
    ):
        _openssl([
            "openssl", "x509", "-req", "-sha256", "-days", "2",
            "-in", str(csr), "-CA", str(ca_cert), "-CAkey", str(ca_key),
            "-CAcreateserial", "-extfile", str(extension),
            "-out", str(certificate),
        ], env)
    ca_key.unlink()
    server_csr.unlink()
    client_csr.unlink()
    return {"ca": ca_cert, "server_cert": server_cert,
            "server_key": server_key, "client_cert": client_cert,
            "client_key": client_key}


def _metadata(binary: Path) -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[1]
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo_root,
                              capture_output=True, text=True, check=False)
    cpu_info = Path("/proc/cpuinfo")
    cpu_text = cpu_info.read_text(encoding="utf-8") if cpu_info.exists() else ""
    cpu_model = next((line.split(":", 1)[1].strip()
                      for line in cpu_text.splitlines()
                      if line.startswith("model name")), platform.machine())
    quota = Path("/sys/fs/cgroup/cpu.max")
    return {
        "type": "metadata", "schema": 1,
        "api_generation": "connection_group_v2",
        "binary": str(binary),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "git_revision": revision.stdout.strip() if revision.returncode == 0 else None,
        "platform": platform.platform(), "cpu_model": cpu_model,
        "affinity": sorted(os.sched_getaffinity(0))
                    if hasattr(os, "sched_getaffinity") else None,
        "cgroup_cpu_max": quota.read_text().strip() if quota.exists() else None,
        "clock": "CLOCK_MONOTONIC",
        "transport": "TCP IPv4 loopback through byte-counting proxy",
        "tcp_bytes": "TCP stream payload; includes TLS/control, excludes TCP/IP headers and retransmits",
        "latency": "client monotonic timestamp in message to complete server callback; same-host clock",
        "client_resources": "process CPU/allocation delta during measured sends; lifecycle peak RSS",
        "server_resources": "process CPU/allocation delta during measured receive; lifecycle peak RSS",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=100)
    parser.add_argument("--scenario", choices=("all", *SCENARIOS), default="all")
    parser.add_argument("--clients", choices=("all", "1", "2"), default="all")
    parser.add_argument("--smoke", action="store_true",
                        help="run small/one-client 32-request TLS × compression correctness cases")
    args = parser.parse_args()
    if args.smoke:
        args.trials, args.requests, args.warmup = 1, 32, 8
        selected_scenarios = ("small",)
        selected_clients = (1,)
    else:
        if not 1 <= args.trials <= 20 or not 32 <= args.requests <= 100000:
            parser.error("trials must be 1..20 and requests 32..100000")
        if not 0 <= args.warmup <= 100000:
            parser.error("warmup must be 0..100000")
        selected_scenarios = SCENARIOS if args.scenario == "all" else (args.scenario,)
        selected_clients = (1, 2) if args.clients == "all" else (int(args.clients),)
    binary = args.binary.resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error("--binary must name an executable file")
    repo_root = Path(__file__).resolve().parents[1]
    build_root = (repo_root / "build").resolve()
    output = args.output.resolve()
    try:
        output.relative_to(build_root)
    except ValueError:
        parser.error("--output must be under this worktree's build/ directory")
    output.parent.mkdir(parents=True, exist_ok=True)
    output_dir = output.parent
    identity_dir = output_dir / f"{output.stem}-tls-identity"
    log_root = output_dir / f"{output.stem}-logs"
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    repo_tmp = build_root / "tmp"
    xmake_tmp = build_root / "xmake-tmp"
    repo_tmp.mkdir(parents=True, exist_ok=True)
    xmake_tmp.mkdir(parents=True, exist_ok=True)
    env["TMPDIR"] = str(repo_tmp)
    tls = generate_tls_identity(identity_dir, env)

    cases = [case for case in expected_matrix_cases(
        trials=args.trials, requests=args.requests)
        if case["scenario"] in selected_scenarios and
        case["clients"] in selected_clients]
    with output.open("w", encoding="utf-8") as result_file:
        result_file.write(json.dumps(_metadata(binary), sort_keys=True) + "\n")
        result_file.flush()
        for number, case in enumerate(cases, start=1):
            case_tls = tls if case["tls"] else None
            label = (f"{number:03d}-{case['scenario']}-clients-{case['clients']}"
                     f"-tls-{int(case['tls'])}-{case['compression']}-trial-{case['trial']}")
            try:
                row = run_case(binary, case, args.warmup,
                               log_root / label, case_tls)
                row["label"] = label
                result_file.write(json.dumps(row, sort_keys=True) + "\n")
                result_file.flush()
                server = row["server"]
                print(f"{label}: received={server['received']}/{server['expected']} "
                      f"rps={server['receive_rps']:.1f} "
                      f"p50={server['latency_p50_us']:.1f}us "
                      f"p99={server['latency_p99_us']:.1f}us "
                      f"tcp={server['tcp_total_bytes']}B")
            except Exception as error:
                result_file.write(json.dumps({
                    "type": "failure", "schema": 1,
                    "api_generation": "connection_group_v2",
                    "case": case, "error": str(error), "logs": str(log_root / label),
                }, sort_keys=True) + "\n")
                result_file.flush()
                raise
    print(f"completed {len(cases)} connection-group matrix cases; results: {output}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, subprocess.CalledProcessError, TimeoutError, RuntimeError,
            ValueError) as error:
        print(f"connection-group matrix failed: {error}", file=sys.stderr)
        raise SystemExit(1)
