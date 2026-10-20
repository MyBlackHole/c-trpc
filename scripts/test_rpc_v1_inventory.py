#!/usr/bin/env python3
"""退役清单生成器的正反例；拒绝把注释和 V2 当作 V1 引用。"""
import tempfile
import unittest
from pathlib import Path

from rpc_v1_inventory import audit, parse_nm, remove_comments_and_literals


class InventoryTests(unittest.TestCase):
    def test_client_facade_headers_have_no_v1_endpoint_references(self):
        root = Path(__file__).resolve().parents[1]
        symbols = audit(root)["symbols"]
        client_files = {
            "src/client.c",
            "src/facade_legacy_internal.h",
            "src/facade_tuning_internal.h",
            "src/facade_diagnostics_internal.h",
        }
        client_names = {
            name for name, entry in symbols.items()
            if (name.startswith("tr_client_legacy_") or
                name in {"tr_client_create_with_tuning",
                         "tr_client_get_rpc_stats"} or
                name.startswith("tr_rpc_endpoint_"))
            if any(use["path"] in client_files
                   for use in entry["source_uses"])
        }
        self.assertEqual(client_names, set())

    def test_comments_literals_and_v2_are_excluded(self):
        src = """/* tr_rpc_endpoint_create() */
const char *p = "tr_client_legacy_create";
tr_rpc_v2_client_start(foo);
tr_rpc_record_endpoint_create(bar);
// tr_rpc_call_finish(handle, 0)
tr_rpc_call_finish(call, 0);
"""
        clean = remove_comments_and_literals(src)
        self.assertNotIn("tr_rpc_endpoint_create", clean)
        self.assertNotIn("tr_client_legacy_create", clean)
        self.assertIn("tr_rpc_call_finish(call, 0)", clean)

    def test_scans_all_layers_and_preserves_line_numbers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for folder in ("src/rpc", "src/runtime", "tests"):
                (root / folder).mkdir(parents=True)
            (root / "src/rpc/rpc.c").write_text(
                "int tr_rpc_call_cancel(void);\n", encoding="utf-8"
            )
            (root / "src/runtime/runtime.c").write_text(
                "\ntr_rpc_executor_group_start(group);\n", encoding="utf-8"
            )
            (root / "tests/test_rpc.c").write_text(
                "tr_rpc_call_cancel(call);\n", encoding="utf-8"
            )
            (root / "xmake.lua").write_text(
                'add_ldflags("-Wl,--wrap=tr_rpc_executor_group_start")\n',
                encoding="utf-8",
            )
            result = audit(root)
            self.assertEqual(result["c_family_files_scanned"], 3)
            a = result["symbols"]["tr_rpc_call_cancel"]
            self.assertEqual(len(a["source_uses"]), 2)
            b = result["symbols"]["tr_rpc_executor_group_start"]
            self.assertEqual(b["xmake_lines"], [1])
            self.assertEqual(b["source_uses"][0]["lines"], [2])

    def test_nm_per_object_refs(self):
        text = (
            "build/libtrcore.a:rpc.c.o:0000000000000000 T "
            "tr_rpc_endpoint_create\n"
            "build/libtrcore.a:runtime.c.o:                 U "
            "tr_rpc_executor_group_start\n"
            "build/libtrcore.a:rpc_v2.c.o:0000000 T "
            "tr_rpc_v2_call_start\n"
        )
        entries = parse_nm(text)
        self.assertEqual(entries["tr_rpc_endpoint_create"],
                         ["build/libtrcore.a:rpc.c.o"])
        self.assertEqual(entries["tr_rpc_executor_group_start"],
                         ["build/libtrcore.a:runtime.c.o"])
        self.assertNotIn("tr_rpc_v2_call_start", entries)


if __name__ == "__main__":
    unittest.main()
