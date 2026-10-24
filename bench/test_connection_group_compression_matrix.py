from __future__ import annotations

import math
import os
import socket
import threading
import unittest

import run_connection_group_compression_matrix as matrix


class ShutdownProtocolTests(unittest.TestCase):
    def test_reader_preserves_two_records_written_to_one_pipe(self) -> None:
        records = [
            '{"type":"server_release_ready","schema":1,'
            '"api_generation":"connection_group_v2",'
            '"released_transfers":1}\n',
            '{"type":"server_exit","schema":1,'
            '"api_generation":"connection_group_v2"}\n',
        ]
        read_fd, write_fd = os.pipe()
        os.write(write_fd, "".join(records).encode("utf-8"))

        class BufferedStdout:
            def __init__(self, fd: int) -> None:
                self.fd = fd
                self.pending = bytearray()

            def fileno(self) -> int:
                return self.fd

            def readline(self) -> str:
                while b"\n" not in self.pending:
                    chunk = os.read(self.fd, 4096)
                    if not chunk:
                        break
                    self.pending.extend(chunk)
                newline = self.pending.find(b"\n")
                if newline < 0:
                    line = bytes(self.pending)
                    self.pending.clear()
                else:
                    line = bytes(self.pending[:newline + 1])
                    del self.pending[:newline + 1]
                return line.decode("utf-8")

            def close(self) -> None:
                os.close(self.fd)

        class Process:
            def __init__(self) -> None:
                self.stdout = BufferedStdout(read_fd)

        observed: list[str] = []
        process = Process()
        try:
            matrix._read_json_line(process, "server_release_ready", records=observed)
            matrix._read_json_line(process, "server_exit", timeout_s=0.01,
                                   records=observed)
        finally:
            process.stdout.close()
            os.close(write_fd)
        self.assertEqual(observed, [record.rstrip("\n") for record in records])

    def test_server_releases_transfers_before_client_shutdown(self) -> None:
        record = (
            '{"type":"server_release_ready","schema":1,'
            '"api_generation":"connection_group_v2",'
            '"released_transfers":1}\n'
        )
        read_fd, write_fd = os.pipe()
        os.write(write_fd, record.encode("utf-8"))
        os.close(write_fd)

        class Child:
            def __init__(self, stdout=None) -> None:
                self.stdout = stdout
                self.stdin = self
                self.gates: list[str] = []

            def write(self, value: str) -> int:
                self.gates.append(value)
                return len(value)

            def flush(self) -> None:
                return None

        server = Child(os.fdopen(read_fd, "r", encoding="utf-8"))
        client = Child()
        records: list[str] = []

        matrix._release_server_transfers_before_client_shutdown(
            server, [client], records)

        self.assertEqual(records, [record.rstrip("\n")])
        self.assertEqual(client.gates, ["Q\n"])
        server.stdout.close()


class ResultContractTests(unittest.TestCase):
    def test_matrix_covers_transport_mode_payload_and_client_count(self) -> None:
        cases = matrix.expected_matrix_cases(trials=2, requests=32)
        self.assertEqual(len(cases), 4 * 4 * 2 * 2)
        self.assertEqual(
            {case["scenario"] for case in cases},
            {"small", "bulk-compressible", "bulk-incompressible", "mixed"},
        )
        self.assertEqual({case["tls"] for case in cases}, {False, True})
        self.assertEqual({case["compression"] for case in cases}, {"none", "zstd"})
        self.assertEqual({case["clients"] for case in cases}, {1, 2})
        self.assertEqual({case["trial"] for case in cases}, {1, 2})

    def test_valid_case_requires_exact_client_and_server_completion(self) -> None:
        case = {
            "scenario": "small", "clients": 1, "requests": 2,
            "tls": False, "compression": "none", "trial": 1,
        }
        client = {
            "type": "client_phase", "schema": 1,
            "api_generation": "connection_group_v2", "phase": "measure",
            "client_index": 0, "attempted": 3, "accepted": 2,
            "submit_again": 1, "submit_errors": 0,
            "start_ns": 1000000000, "end_ns": 1500000000,
            "elapsed_s": 0.5, "accepted_rps": 4.0,
            "client_cpu_s": 0.1, "client_peak_rss_kib": 12000,
            "client_alloc_calls": 10, "client_alloc_requested_bytes": 1000,
        }
        server = {
            "type": "server_phase", "schema": 1,
            "api_generation": "connection_group_v2", "phase": "measure",
            "expected": 2, "received": 2, "verified": 2, "invalid": 0,
            "start_ns": 1000000000, "end_ns": 1450000000,
            "elapsed_s": 0.45, "receive_rps": 4.444,
            "latency_p50_us": 1000.0, "latency_p99_us": 2000.0,
            "server_cpu_s": 0.2, "server_peak_rss_kib": 13000,
            "server_alloc_calls": 20, "server_alloc_requested_bytes": 2000,
        }
        matrix.validate_case(case, [client], server)

    def test_case_rejects_lost_or_corrupt_messages(self) -> None:
        case = {
            "scenario": "small", "clients": 1, "requests": 2,
            "tls": False, "compression": "none", "trial": 1,
        }
        client = {
            "type": "client_phase", "schema": 1,
            "api_generation": "connection_group_v2", "phase": "measure",
            "client_index": 0, "attempted": 2, "accepted": 2,
            "submit_again": 0, "submit_errors": 0,
            "start_ns": 1000000000, "end_ns": 1500000000,
            "elapsed_s": 0.5, "accepted_rps": 4.0,
            "client_cpu_s": 0.1, "client_peak_rss_kib": 12000,
            "client_alloc_calls": 10, "client_alloc_requested_bytes": 1000,
        }
        server = {
            "type": "server_phase", "schema": 1,
            "api_generation": "connection_group_v2", "phase": "measure",
            "expected": 2, "received": 1, "verified": 1, "invalid": 0,
            "start_ns": 1000000000, "end_ns": 1450000000,
            "elapsed_s": 0.45, "receive_rps": 2.222,
            "latency_p50_us": 1000.0, "latency_p99_us": 2000.0,
            "server_cpu_s": 0.2, "server_peak_rss_kib": 13000,
            "server_alloc_calls": 20, "server_alloc_requested_bytes": 2000,
        }
        with self.assertRaisesRegex(ValueError, "server completion"):
            matrix.validate_case(case, [client], server)
        server["received"] = 2
        server["verified"] = 1
        with self.assertRaisesRegex(ValueError, "payload validation"):
            matrix.validate_case(case, [client], server)

    def test_case_rejects_non_finite_measurements(self) -> None:
        case = {
            "scenario": "small", "clients": 1, "requests": 1,
            "tls": False, "compression": "none", "trial": 1,
        }
        client = {
            "type": "client_phase", "schema": 1,
            "api_generation": "connection_group_v2", "phase": "measure",
            "client_index": 0, "attempted": 1, "accepted": 1,
            "submit_again": 0, "submit_errors": 0,
            "start_ns": 1000000000, "end_ns": 1500000000,
            "elapsed_s": math.inf, "accepted_rps": 1.0,
            "client_cpu_s": 0.1, "client_peak_rss_kib": 12000,
            "client_alloc_calls": 10, "client_alloc_requested_bytes": 1000,
        }
        server = {
            "type": "server_phase", "schema": 1,
            "api_generation": "connection_group_v2", "phase": "measure",
            "expected": 1, "received": 1, "verified": 1, "invalid": 0,
            "start_ns": 1000000000, "end_ns": 1450000000,
            "elapsed_s": 0.45, "receive_rps": 2.222,
            "latency_p50_us": 1000.0, "latency_p99_us": 2000.0,
            "server_cpu_s": 0.2, "server_peak_rss_kib": 13000,
            "server_alloc_calls": 20, "server_alloc_requested_bytes": 2000,
        }
        with self.assertRaisesRegex(ValueError, "finite"):
            matrix.validate_case(case, [client], server)

    def test_case_rejects_wrapped_allocation_counters(self) -> None:
        case = {
            "scenario": "small", "clients": 1, "requests": 1,
            "tls": False, "compression": "none", "trial": 1,
        }
        client = {
            "type": "client_phase", "schema": 1,
            "api_generation": "connection_group_v2", "phase": "measure",
            "client_index": 0, "attempted": 1, "accepted": 1,
            "submit_again": 0, "submit_errors": 0,
            "start_ns": 1000000000, "end_ns": 1500000000,
            "elapsed_s": 0.5, "accepted_rps": 2.0,
            "client_cpu_s": 0.1, "client_peak_rss_kib": 12000,
            "client_alloc_calls": 10, "client_alloc_requested_bytes": 1000,
        }
        server = {
            "type": "server_phase", "schema": 1,
            "api_generation": "connection_group_v2", "phase": "measure",
            "expected": 1, "received": 1, "verified": 1, "invalid": 0,
            "start_ns": 1000000000, "end_ns": 1450000000,
            "elapsed_s": 0.45, "receive_rps": 2.222,
            "latency_p50_us": 1000.0, "latency_p99_us": 2000.0,
            "server_cpu_s": 0.2, "server_peak_rss_kib": 13000,
            "server_alloc_calls": (1 << 64) - 36,
            "server_alloc_requested_bytes": (1 << 64) - 229456,
        }
        with self.assertRaisesRegex(ValueError, "allocation counter"):
            matrix.validate_case(case, [client], server)


class TcpByteProxyTests(unittest.TestCase):
    def test_counts_both_tcp_directions(self) -> None:
        backend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        backend.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        backend.bind(("127.0.0.1", 0))
        backend.listen(1)
        backend_port = backend.getsockname()[1]
        server_errors: list[BaseException] = []

        def echo_once() -> None:
            try:
                connection, _ = backend.accept()
                with connection:
                    request = connection.recv(64)
                    connection.sendall(request[::-1])
            except BaseException as error:  # propagated in the test thread
                server_errors.append(error)

        server_thread = threading.Thread(target=echo_once)
        server_thread.start()
        proxy = matrix.TcpByteProxy("127.0.0.1", backend_port)
        try:
            proxy_port = proxy.start()
            with socket.create_connection(("127.0.0.1", proxy_port), timeout=2) as client:
                client.sendall(b"group-data")
                self.assertEqual(client.recv(64), b"atad-puorg")
            server_thread.join(timeout=2)
            self.assertFalse(server_thread.is_alive())
            self.assertEqual(server_errors, [])
            self.assertEqual(proxy.snapshot(), (10, 10))
        finally:
            proxy.close()
            backend.close()
            server_thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
