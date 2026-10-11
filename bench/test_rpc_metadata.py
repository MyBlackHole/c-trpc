"""共享元数据及每个运行入口的负载模式回归；不启动真实负载。"""
import importlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from run_rpc_bench import metadata


class StopBeforeLoad(Exception):
    pass


class MetadataTests(unittest.TestCase):
    def test_default_and_explicit_modes(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "binary"
            binary.write_bytes(b"metadata fixture")
            default = metadata(binary, "test")
            closed = metadata(binary, "test", load_mode="closed")
            opened = metadata(binary, "test", load_mode="open")
            self.assertEqual(default["load_model"], closed["load_model"])
            self.assertIn("closed-loop", closed["load_model"])
            self.assertNotIn("open-loop", closed["load_model"])
            self.assertIn("open-loop", opened["load_model"])
            self.assertNotIn("closed-loop", opened["load_model"])
            for key in default:
                if key != "load_model":
                    self.assertEqual(default[key], opened[key])

    def test_unknown_modes_do_not_silently_fall_back(self):
        for mode in ("", "OPEN", "mixed", None, True, 1, []):
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                metadata(Path("missing-binary"), "test", load_mode=mode)

    def test_each_runner_writes_correct_metadata_before_workload(self):
        runners = (
            ("run_rpc_bench", "run_case", "closed"),
            ("run_rpc_capacity", "run_case", "open"),
            ("run_rpc_scalability", "run_multisource_case", "open"),
            ("run_rpc_headroom", "run_profile_case", "open"),
            ("run_rpc_repeatability", "run_profile_case", "open"),
            ("run_rpc_fanout", "run_profile_case", "open"),
            ("run_rpc_generator_slots", "run_profile_case", "open"),
        )
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "binary"
            binary.write_text("not executed\n")
            binary.chmod(0o700)
            for name, target, mode in runners:
                with self.subTest(runner=name):
                    module = importlib.import_module(name)
                    output = Path(directory) / (name + ".jsonl")
                    args = [name, "--binary", str(binary), "--output", str(output)]
                    # 首次场景调用前主动停止：覆盖真实参数/元数据路径，禁止负载。
                    with patch("sys.argv", args), patch.object(
                            module, target, side_effect=StopBeforeLoad):
                        with self.assertRaises(StopBeforeLoad):
                            module.main()
                    row = json.loads(output.read_text().splitlines()[0])
                    if mode == "closed":
                        self.assertIn("closed-loop", row["load_model"])
                        self.assertNotIn("fixed-rate", row["load_model"])
                    else:
                        self.assertIn("fixed-rate", row["load_model"])
                        self.assertNotIn("closed-loop", row["load_model"])


if __name__ == "__main__":
    unittest.main()
