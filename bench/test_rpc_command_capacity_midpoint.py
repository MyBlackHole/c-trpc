"""Tests for the repeated command-capacity ladder."""

import unittest

from run_rpc_repeatability import PROFILE_SPECS


class CommandCapacityMidpointTests(unittest.TestCase):
    def test_midpoint_changes_only_command_capacity(self):
        base = PROFILE_SPECS["ceiling_headroom"]
        midpoint = PROFILE_SPECS["command_mid_headroom"]
        ceiling = PROFILE_SPECS["command_ceiling_headroom"]
        for key in ("rx_buffer_count", "executor_queue",
                    "control_tx_item_count"):
            self.assertEqual(midpoint[key], base[key])
            self.assertEqual(ceiling[key], base[key])
        self.assertEqual(base["command_capacity"], 4096)
        self.assertEqual(midpoint["command_capacity"], 8192)
        self.assertEqual(ceiling["command_capacity"], 16384)


if __name__ == "__main__":
    unittest.main()
