#!/usr/bin/env python3
"""退役清单生成器的正反例；拒绝把注释和 V2 当作 V1 引用。"""
import tempfile
import unittest
from pathlib import Path

from rpc_v1_inventory import (
    SERVER_FACADE_RETIREMENT,
    audit,
    denied_symbols,
    parse_nm,
    remove_comments_and_literals,
)


CLIENT_FACADE_FILES = {
    "src/client.c",
    "src/facade_legacy_internal.h",
    "src/facade_tuning_internal.h",
    "src/facade_diagnostics_internal.h",
}


def client_endpoint_references(symbols):
    return {
        name for name, entry in symbols.items()
        if (name.startswith("tr_client_legacy_") or
            name in {"tr_client_create_with_tuning",
                     "tr_client_get_rpc_stats", "tr_rpc_endpoint"} or
            name.startswith("tr_rpc_endpoint_"))
        if any(use["path"] in CLIENT_FACADE_FILES
               for use in entry["source_uses"])
    }


class InventoryTests(unittest.TestCase):
    @staticmethod
    def workspace_tempdir():
        root = Path(__file__).resolve().parents[1] / "build" / "r12-inventory-tests"
        root.mkdir(parents=True, exist_ok=True)
        return tempfile.TemporaryDirectory(dir=root)

    def test_client_facade_headers_have_no_v1_endpoint_references(self):
        root = Path(__file__).resolve().parents[1]
        symbols = audit(root)["symbols"]
        self.assertEqual(client_endpoint_references(symbols), set())

    def test_bare_v1_endpoint_type_is_a_legacy_reference(self):
        with self.workspace_tempdir() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            (root / "src/client.c").write_text(
                "struct tr_rpc_endpoint *rpc;\n"
                "struct tr_rpc_executor_group *group;\n",
                encoding="utf-8",
            )
            (root / "xmake.lua").write_text("", encoding="utf-8")

            symbols = audit(root)["symbols"]

        self.assertIn("tr_rpc_endpoint",
                      client_endpoint_references(symbols))
        self.assertIn("tr_rpc_executor_group", symbols)

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
        with self.workspace_tempdir() as tmp:
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

    def test_server_facade_tuning_and_legacy_source_are_denied(self):
        with self.workspace_tempdir() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            (root / "src/server.c").write_text(
                "tr_server_create_with_tuning(config, tuning, out);\n"
                "tr_server_legacy_create(config, out);\n"
                "tr_server_legacy_register_method(server, method);\n",
                encoding="utf-8",
            )
            (root / "xmake.lua").write_text("", encoding="utf-8")
            symbols = audit(root)["symbols"]

        expected = {
            "tr_server_create_with_tuning",
            "tr_server_legacy_create",
            "tr_server_legacy_register_method",
        }
        self.assertTrue(expected.issubset(symbols))
        self.assertEqual(denied_symbols(symbols, expected), sorted(expected))
        self.assertTrue(all(SERVER_FACADE_RETIREMENT.fullmatch(name)
                            for name in expected))

    def test_runtime_executor_names_are_denied_without_banning_raw_or_v2(self):
        with self.workspace_tempdir() as temporary:
            root = Path(temporary)
            (root / "src/runtime").mkdir(parents=True)
            (root / "src/runtime/runtime.c").write_text(
                "struct tr_runtime_rpc_executor_config config;\n"
                "tr_runtime_shard_rpc_executor(shard);\n"
                "tr_runtime_legacy_rpc_execution_start(execution);\n"
                "tr_rpc_executor_group_create(1, 1, 1, &group);\n"
                "struct tr_rpc_endpoint_result result;\n",
                encoding="utf-8",
            )
            (root / "xmake.lua").write_text("", encoding="utf-8")
            symbols = audit(root)["symbols"]

        runtime_deny = {
            "tr_runtime_rpc_executor_config",
            "tr_runtime_shard_rpc_executor",
            "tr_runtime_legacy_rpc_execution_start",
        }
        self.assertTrue(runtime_deny.issubset(symbols))
        self.assertEqual(denied_symbols(symbols, runtime_deny),
                         sorted(runtime_deny))
        self.assertIn("tr_rpc_executor_group_create", symbols)
        self.assertIn("tr_rpc_endpoint_result", symbols)
        self.assertTrue(runtime_deny.isdisjoint(
            {"tr_rpc_executor_group_create", "tr_rpc_endpoint_result"}))

    def test_archive_deny_is_narrower_than_raw_v1_endpoint_inventory(self):
        archive = parse_nm(
            "libtrcore.a:server.c.o:0000000000000000 T "
            "tr_server_create_with_tuning\n"
            "libtrcore.a:server.c.o:0000000000000000 T "
            "tr_server_legacy_create\n"
            "libtrcore.a:rpc.c.o:0000000000000000 T "
            "tr_rpc_endpoint_create\n"
        )
        server_deny = {
            "tr_server_create_with_tuning",
            "tr_server_legacy_create",
        }
        self.assertEqual(denied_symbols(archive, server_deny),
                         sorted(server_deny))
        self.assertIn("tr_rpc_endpoint_create", archive)
        self.assertTrue(SERVER_FACADE_RETIREMENT.fullmatch(
            "tr_server_legacy_create"))
        self.assertIsNone(SERVER_FACADE_RETIREMENT.fullmatch(
            "tr_rpc_endpoint_create"))

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
