import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import export_rpc_v2_csv


class RpcV2CsvExportTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[1] / "build" / "r12-csv-export-tests"
        root.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=root)
        self.directory = Path(self.temporary.name)
        self.binary = self.directory / "bench_rpc"
        self.binary.write_bytes(b"release-binary-fixture")
        self.input = self.directory / "run.jsonl"
        self.output = self.directory / "run.csv"

    def tearDown(self):
        self.temporary.cleanup()

    def _counter(self, attempted):
        return {
            "attempted": attempted,
            "accepted": attempted,
            "ok": attempted,
            "completed": attempted,
            "submit_again": 0,
            "submit_errors": 0,
            "deadlines": 0,
            "rpc_errors": 0,
            "invalid_responses": 0,
            "ok_rps": float(attempted),
            "payload_MiB_s": 0.25,
            "ok_p50_us": 10.0,
            "ok_p99_us": 20.0,
            "accepted_p99_us": 20.0,
        }

    def _phase(self, phase_name, scenario, attempted):
        empty = self._counter(0)
        small = self._counter(attempted)
        return {
            "type": "phase",
            "schema": 2,
            "api_generation": "rpc_v2",
            "phase": phase_name,
            "scenario": scenario,
            "window": 8,
            "start_ns": 100,
            "end_ns": 200,
            "elapsed_s": 0.1,
            "client_cpu_s": 0.05,
            "client_peak_rss_kib": 4096,
            "all": self._counter(attempted),
            "small": small,
            "bulk": dict(empty),
            "slow": dict(empty),
        }

    def _records(self):
        metadata = {
            "type": "metadata",
            "schema": 2,
            "api_generation": "rpc_v2",
            "label": "test-v2",
            "git_revision": "a" * 40,
            "binary": str(self.binary),
            "binary_sha256": hashlib.sha256(self.binary.read_bytes()).hexdigest(),
            "platform": "test-platform",
            "cpu_model": "test-cpu",
            "affinity": [0, 1],
            "cgroup_cpu_max": None,
            "clock": "CLOCK_MONOTONIC",
            "transport": "TCP IPv4 loopback",
            "load_model": "closed-loop",
            "latency": "terminal callback",
            "goodput": "verified application bytes",
            "server_resources": "lifetime counters",
        }
        server_exit = {
            "type": "server_exit",
            "schema": 2,
            "api_generation": "rpc_v2",
            "drain_status": 0,
            "cpu_s": 1.0,
            "peak_rss_kib": 4096,
            "rpc_calls_started": 3,
            "rpc_calls_finished": 3,
            "rpc_executor_threads": 4,
            "rpc_task_queue_capacity": 16,
            "reassembly_pool_capacity": 8,
            "peers_ready_total": 1,
            "peers_reaped_total": 1,
        }
        case = {
            "type": "case",
            "schema": 2,
            "api_generation": "rpc_v2",
            "case": {
                "scenario": "pressure",
                "window": 8,
                "clients": 1,
                "bulk_bytes": 65536,
                "warmup": 10,
                "requests": 2,
                "trial": 1,
            },
            "clients": [[
                self._phase("measure", "pressure", 2),
                self._phase("recovery", "small", 1),
            ]],
            "server_exit": server_exit,
            "runner_elapsed_s": 0.5,
        }
        return [metadata, case]

    def _write_input(self, records):
        self.input.write_text("".join(json.dumps(row) + "\n" for row in records))

    def test_exports_each_phase_with_v2_provenance_and_accounting(self):
        self._write_input(self._records())

        count = export_rpc_v2_csv.export_jsonl(
            self.input, self.output, self.binary
        )

        with self.output.open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual(count, 2)
        self.assertEqual([row["phase"] for row in rows], ["measure", "recovery"])
        self.assertEqual(rows[1]["scenario"], "pressure")
        self.assertEqual(rows[1]["phase_scenario"], "small")
        self.assertEqual(rows[0]["schema"], "2")
        self.assertEqual(rows[0]["api_generation"], "rpc_v2")
        self.assertEqual(rows[0]["git_revision"], "a" * 40)
        self.assertEqual(rows[0]["all_attempted"], "2")
        self.assertEqual(rows[0]["server_rpc_calls_finished"], "3")
        self.assertNotIn("server_rpc_message_pool", rows[0])
        self.assertNotIn(b"\r", self.output.read_bytes())

    def test_rejects_wrong_generation(self):
        records = self._records()
        records[1]["api_generation"] = "rpc_v1"
        self._write_input(records)
        with self.assertRaisesRegex(ValueError, "api_generation"):
            export_rpc_v2_csv.export_jsonl(self.input, self.output, self.binary)

    def test_rejects_binary_hash_mismatch(self):
        records = self._records()
        records[0]["binary_sha256"] = "0" * 64
        self._write_input(records)
        with self.assertRaisesRegex(ValueError, "binary_sha256"):
            export_rpc_v2_csv.export_jsonl(self.input, self.output, self.binary)

    def test_rejects_category_accounting_mismatch(self):
        records = self._records()
        records[1]["clients"][0][0]["small"]["attempted"] = 1
        self._write_input(records)
        with self.assertRaisesRegex(ValueError, "bucket accounting"):
            export_rpc_v2_csv.export_jsonl(self.input, self.output, self.binary)


if __name__ == "__main__":
    unittest.main()
