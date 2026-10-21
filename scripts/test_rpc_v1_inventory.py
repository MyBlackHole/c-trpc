#!/usr/bin/env python3
"""退役清单生成器的正反例；拒绝把注释和 V2 当作 V1 引用。"""
import tempfile
import unittest
from pathlib import Path

from rpc_v1_inventory import (
    V1_CORE_DENY,
    SERVER_FACADE_RETIREMENT,
    audit,
    denied_symbols,
    parse_nm,
    remove_comments_and_literals,
    remove_xmake_symbol_guard,
    validate_archive_inventory,
)


CLIENT_FACADE_FILES = {
    "src/client.c",
    "src/facade_legacy_internal.h",
    "src/facade_tuning_internal.h",
    "src/facade_diagnostics_internal.h",
}
V1_ENDPOINT_DENY = {
    "tr_rpc_endpoint", "tr_rpc_endpoint_config", "tr_rpc_endpoint_stats",
    "tr_rpc_endpoint_create", "tr_rpc_endpoint_destroy",
    "tr_rpc_endpoint_create_with_executor_group",
    "tr_rpc_endpoint_destroy_with_stats", "tr_rpc_endpoint_get_stats",
    "tr_rpc_endpoint_get_semantic_stats",
    "tr_rpc_endpoint_detach_for_finalize",
    "tr_rpc_endpoint_arm_detached_finalizer",
    "tr_rpc_endpoint_release_detached_owner",
    "tr_rpc_endpoint_finalize_detached_with_stats",
    "tr_rpc_endpoint_flush", "tr_rpc_endpoint_flush_on_owner",
}
CLIENT_FACADE_DENY = {
    "tr_client_legacy_create", "tr_client_legacy_register_method",
    "tr_client_legacy_unary_call", "tr_client_legacy_unary_call_ex",
    "tr_client_legacy_call_start", "tr_client_legacy_call_start_ex",
    "tr_client_create_with_tuning", "tr_client_get_rpc_stats",
}


def client_endpoint_references(symbols):
    return {
        name for name, entry in symbols.items()
        if name in CLIENT_FACADE_DENY | V1_ENDPOINT_DENY
        if any(use["path"] in CLIENT_FACADE_FILES
               for use in entry["source_uses"])
    }


class InventoryTests(unittest.TestCase):
    @staticmethod
    def workspace_tempdir():
        root = Path(__file__).resolve().parents[1] / "build" / "r14-inventory-tests"
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

    def test_exact_v1_core_symbols_and_types_are_denied(self):
        with self.workspace_tempdir() as temporary:
            root = Path(temporary)
            (root / "src/rpc").mkdir(parents=True)
            (root / "src/rpc/legacy.c").write_text(
                "struct tr_rpc_call_handle call;\n"
                "enum tr_rpc_wire_type wire_type;\n"
                "struct tr_rpc_interceptor interceptor;\n"
                "tr_rpc_call_finish(call, TR_RPC_STATUS_OK);\n"
                "TR_RPC_CALL_PRIVATE_WORDS private_words;\n"
                "tr_rpc_endpoint_create(channel, config, &endpoint);\n"
                "tr_rpc_executor_group_start(group);\n"
                "tr_rpc_register_stream_method(endpoint, method, handlers, arg);\n"
                "tr_rpc_message_release(&message);\n",
                encoding="utf-8",
            )
            (root / "xmake.lua").write_text("", encoding="utf-8")
            symbols = audit(root)["symbols"]

        expected = {
            "tr_rpc_call_handle",
            "tr_rpc_wire_type",
            "tr_rpc_interceptor",
            "tr_rpc_call_finish",
            "tr_rpc_endpoint_create",
            "tr_rpc_executor_group_start",
            "tr_rpc_register_stream_method",
            "tr_rpc_message_release",
            "TR_RPC_CALL_PRIVATE_WORDS",
        }
        self.assertTrue(expected.issubset(V1_CORE_DENY))
        self.assertTrue(expected.issubset(symbols))
        self.assertEqual(denied_symbols(symbols, expected), sorted(expected))

    def test_v2_endpoint_record_and_raw_codec_names_are_allowed(self):
        with self.workspace_tempdir() as temporary:
            root = Path(temporary)
            (root / "src/rpc").mkdir(parents=True)
            (root / "src/rpc/v2.c").write_text(
                "struct tr_rpc_endpoint_result result;\n"
                "tr_rpc_endpoint_terminal_fn terminal;\n"
                "tr_rpc_endpoint_progress_fn progress;\n"
                "struct tr_rpc_bytes raw_bytes;\n"
                "tr_rpc_record_endpoint_create(endpoint);\n"
                "tr_rpc_record_endpoint_destroy(endpoint);\n"
                "tr_rpc_raw_encode(input, dst, capacity, written);\n"
                "tr_rpc_raw_decode(src, len, out);\n",
                encoding="utf-8",
            )
            (root / "xmake.lua").write_text("", encoding="utf-8")
            symbols = audit(root)["symbols"]

        allowed = {
            "tr_rpc_endpoint_result",
            "tr_rpc_endpoint_terminal_fn",
            "tr_rpc_endpoint_progress_fn",
            "tr_rpc_bytes",
            "tr_rpc_record_endpoint_create",
            "tr_rpc_record_endpoint_destroy",
            "tr_rpc_raw_encode",
            "tr_rpc_raw_decode",
        }
        self.assertTrue(allowed.isdisjoint(symbols))
        self.assertEqual(denied_symbols(symbols, allowed), [])

        endpoint_uses = {
            name: {"source_uses": [{"path": "src/client.c"}]}
            for name in allowed
        }
        self.assertEqual(client_endpoint_references(endpoint_uses), set())

    def test_archive_contract_requires_codec_and_rejects_v1_closure(self):
        valid_members = ["rpc_record_client.c.o", "rpc_record_endpoint.c.o",
                         "rpc_codec.c.o", "client.c.o"]
        valid_definitions = {
            "tr_rpc_raw_encode": ["libtrcore.a:rpc_codec.c.o"],
            "tr_rpc_raw_decode": ["libtrcore.a:rpc_codec.c.o"],
            "tr_rpc_endpoint_result": ["libtrcore.a:rpc_record_endpoint.c.o"],
            "tr_rpc_record_endpoint_create": [
                "libtrcore.a:rpc_record_endpoint.c.o"],
        }
        self.assertEqual(validate_archive_inventory(
            valid_members, valid_definitions, {}), [])

        invalid_members = ["rpc.c.o", "rpc_wire.c.o", "client.c.o"]
        invalid_definitions = {
            "tr_rpc_raw_encode": ["libtrcore.a:rpc.c.o"],
            "tr_rpc_call_finish": ["libtrcore.a:rpc.c.o"],
        }
        invalid_undefined = {
            "tr_rpc_wire_decode": ["libtrcore.a:rpc.c.o"],
            "tr_rpc_executor_group_start": ["libtrcore.a:runtime.c.o"],
        }
        problems = validate_archive_inventory(
            invalid_members, invalid_definitions, invalid_undefined)
        self.assertTrue(any("rpc.c.o" in problem for problem in problems))
        self.assertTrue(any("rpc_wire.c.o" in problem for problem in problems))
        self.assertTrue(any("rpc_codec.c.o" in problem for problem in problems))
        self.assertTrue(any("tr_rpc_call_finish" in problem
                            for problem in problems))
        self.assertTrue(any("tr_rpc_wire_decode" in problem
                            for problem in problems))
        self.assertTrue(any("rpc_codec.c.o" in problem and
                            "tr_rpc_raw_encode" in problem
                            for problem in problems))

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

    def test_xmake_v1_symbol_assertion_table_is_not_a_build_reference(self):
        text = (
            "target(\"test\")\n"
            "after_link(function (target)\n"
            "    local v1_symbols = {\n"
            "        tr_rpc_call_finish = true,\n"
            "    }\n"
            "end)\n"
            "add_ldflags(\"-Wl,--wrap=tr_rpc_call_finish\")\n"
        )
        clean = remove_xmake_symbol_guard(text)
        self.assertNotIn("tr_rpc_call_finish = true", clean)
        self.assertIn("--wrap=tr_rpc_call_finish", clean)

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
        self.assertTrue(expected.issubset(SERVER_FACADE_RETIREMENT))

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
        self.assertNotIn("tr_rpc_endpoint_result", symbols)
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
        self.assertIn("tr_server_legacy_create", SERVER_FACADE_RETIREMENT)
        self.assertNotIn("tr_rpc_endpoint_create", SERVER_FACADE_RETIREMENT)

    def test_unknown_legacy_prefix_names_are_not_classified(self):
        self.assertEqual(client_endpoint_references({
            "tr_client_legacy_future": {
                "source_uses": [{"path": "src/client.c"}],
            },
        }), set())
        with self.workspace_tempdir() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            (root / "src/future.c").write_text(
                "tr_client_legacy_future();\n"
                "tr_server_legacy_future();\n"
                "tr_runtime_legacy_rpc_execution_future();\n",
                encoding="utf-8",
            )
            (root / "xmake.lua").write_text("", encoding="utf-8")
            symbols = audit(root)["symbols"]

        self.assertEqual(symbols, {})

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
