#!/usr/bin/env python3
"""只验证 CI 对子进程退出的判定，不调用或修改服务端运行时。"""
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


SHUTDOWN_SCRIPT = Path(__file__).resolve().with_name("echo_smoke_shutdown.sh")
SERVER = """
import os
from pathlib import Path
import signal
import sys

mode, ready_path = sys.argv[1:]
def stop(signo, frame):
    sys.exit(int(mode))
signal.signal(signal.SIGTERM, signal.SIG_IGN if mode == "hang" else stop)
Path(ready_path).write_text(str(os.getpid()))
while True:
    signal.pause()
"""
RUNNER = r"""
set -euo pipefail
source "$1"
"$2" -c "$3" "$4" "$5" >"$6" 2>&1 &
server_pid=$!
trap 'kill -KILL "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true' EXIT
for attempt in $(seq 1 100); do
    if [ -s "$5" ]; then break; fi
    if ! kill -0 "$server_pid" 2>/dev/null; then exit 1; fi
    sleep 0.01
done
test -s "$5"
echo_smoke_shutdown "$server_pid" "$6" 1s
trap - EXIT
printf 'shutdown_checked\n'
"""


class EchoSmokeShutdownTests(unittest.TestCase):
    def run_server(self, mode):
        with tempfile.TemporaryDirectory(prefix="echo-shutdown-") as directory:
            ready = Path(directory) / "ready"
            log = Path(directory) / "server.log"
            started = time.monotonic()
            try:
                result = subprocess.run(
                    ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c",
                     RUNNER, "echo-shutdown-test", str(SHUTDOWN_SCRIPT),
                     sys.executable, SERVER, mode, str(ready), str(log)],
                    capture_output=True, text=True, timeout=5,
                )
                self.assertTrue(ready.is_file(), result.stderr)
                # 包括超时失败路径：shell 返回时不能遗留替身进程。
                with self.assertRaises(ProcessLookupError):
                    os.kill(int(ready.read_text()), 0)
                return result, time.monotonic() - started
            finally:
                # 测试本身失败时也清理替身，避免影响后续测试。
                if ready.is_file():
                    try:
                        os.kill(int(ready.read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_zero_exit_passes(self):
        result, _ = self.run_server("0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "shutdown_checked\n")

    def test_nonzero_exit_fails(self):
        result, _ = self.run_server("23")
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertIn("echo_shutdown_status=23", result.stderr)
        self.assertNotIn("shutdown_checked", result.stdout)

    def test_hang_fails_with_bounded_wait(self):
        result, elapsed = self.run_server("hang")
        self.assertEqual(result.returncode, 124, result.stderr)
        self.assertIn("echo_shutdown_status=124", result.stderr)
        self.assertNotIn("shutdown_checked", result.stdout)
        self.assertLess(elapsed, 5)


if __name__ == "__main__":
    unittest.main()
