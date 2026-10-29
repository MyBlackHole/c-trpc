"""独立语义实验：检查候选的结构演进，不测量 Python 性能。"""
import copy
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import sys
import codec_model as model
from codec_model import Layout, Node, Limits, Reject, encode, decode

if not __debug__:
    raise RuntimeError("ASSERTIONS_REQUIRED")

ROOT = Path(__file__).resolve().parent
COUNTS = {}
CONTROL_FORMATS = (os.environ["CTM_SHAPE_ONLY"],) if os.environ.get("CTM_SHAPE_ONLY") else ("compact", "block")
assert all(f in ("compact", "block") for f in CONTROL_FORMATS)


def count(name, n=1):
    COUNTS[name] = COUNTS.get(name, 0) + n


def rejects(code, function, *args, **kwargs):
    try:
        function(*args, **kwargs)
    except Reject as e:
        assert e.args == (code,), (code, e.args)
        count("rejection_checks")
        return
    raise AssertionError(("EXPECTED_REJECTION", code))


def semantic(node, reader=2):
    return (node.sequence, node.payload, node.values, dict(node.options),
            semantic(node.child, reader) if node.child else None,
            node.counter if reader == 2 else None,
            node.token if reader == 2 else None)


def version(node, revision):
    out = copy.deepcopy(node)
    if revision == 1:
        out.counter = out.token = None
    if out.child:
        out.child = version(out.child, revision)
    return out


def nested(depth):
    layout, node = Layout(payload=True, array=True), Node(sequence=depth, payload=b"leaf", values=(0, 1))
    for i in range(depth - 1):
        layout = Layout(payload=True, array=True, child=layout)
        node = Node(sequence=i, payload=b"branch", values=(2, 3), child=node)
    return layout, node


def fixtures():
    cases = [("scalar_absent", Layout(), Node())]
    for n in (0, 7, 0xffffffffffffffff):
        cases.append(("scalar_" + str(n), Layout(), Node(sequence=n)))
    cases.append(("bytes_absent", Layout(payload=True), Node(sequence=7)))
    for n in (0, 1, 127, 128, 4096, 65536):
        cases.append(("bytes_" + str(n), Layout(payload=True), Node(sequence=7, payload=b"a" * n)))
    cases.append(("array_absent", Layout(array=True), Node(sequence=7)))
    for n in (0, 1, 31, 128, 1024):
        cases.append(("array_" + str(n), Layout(array=True), Node(sequence=7, values=tuple(0xffffffff if i % 2 else i for i in range(n)))))
    for depth in (1, 2, 4, 8):
        layout, node = nested(depth)
        cases.append(("nested_" + str(depth), layout, node))
    for n in (0, 1, 4, 16, 24, 25, 32):
        cases.append(("sparse_" + str(n), Layout(slots=32), Node(sequence=7, options={i: i for i in range(n)})))
    # 新字段 presence 的四种组合在整组输入中轮换；各层同样参与。
    for index, (name, layout, node) in enumerate(cases):
        current, depth = node, 0
        while current is not None:
            choice = (index + depth) % 4
            current.counter = 0 if choice == 1 else 91 if choice == 3 else None
            current.token = b"" if choice == 2 else b"future" if choice == 3 else None
            current, depth = current.child, depth + 1
        yield name, layout, node


def roundtrips(cases):
    sizes, vectors = [], []
    for name, layout, newer in cases:
        for fmt in ("compact", "block"):
            for writer in (1, 2):
                original = version(newer, writer)
                wire = encode(fmt, layout, original, writer)
                sizes.append(dict(case=name, format=fmt, writer=writer, body_bytes=len(wire) - 4))
                vectors.append(dict(case=name, format=fmt, writer=writer,
                                    size=len(wire), sha256=hashlib.sha256(wire).hexdigest(),
                                    hex=wire.hex() if len(wire) <= 512 else None))
                for reader in (1, 2):
                    parsed = decode(fmt, layout, wire, reader)
                    assert semantic(parsed, reader) == semantic(original, reader)
                    rewritten = encode(fmt, layout, parsed, reader)
                    assert semantic(decode(fmt, layout, rewritten, 2)) == semantic(original)
                    if fmt == "block" and writer == 1 and reader == 2:
                        current, nodes = original, 0
                        while current is not None:
                            nodes += 1
                            current = current.child
                        assert len(rewritten) == len(wire) + 12 * nodes
                        count("explicit_block_revision_upgrades")
                    else:
                        assert rewritten == wire, (name, fmt, writer, reader)
                    count("writer_reader_roundtrips")
    return sizes, vectors


def known_vectors():
    # 手工写出的最小 v1 字节，独立于编码器构造期记账。
    examples = {
        "compact": "0a00000025080700000000000000",
        "block": "140000001000010001000000000000000700000000000000",
    }
    for fmt, golden in examples.items():
        wire = bytes.fromhex(golden)
        assert encode(fmt, Layout(), Node(sequence=7), 1) == wire
        assert decode(fmt, Layout(), wire, 1).sequence == 7
        absent = decode(fmt, Layout(), encode(fmt, Layout(), Node(), 1), 2)
        zero = decode(fmt, Layout(), encode(fmt, Layout(), Node(sequence=0), 1), 2)
        assert absent.sequence is None and zero.sequence == 0
        count("handwritten_golden_vectors")
    arrays = {
        "compact": "18000000250807000000000000006e0c0000000001000000ffffffff",
        "block": "2400000010000100050000000000000007000000000000000c0000000000000001000000ffffffff",
    }
    for fmt, golden in arrays.items():
        wire = bytes.fromhex(golden)
        node = Node(sequence=7, values=(0, 1, 0xffffffff))
        assert encode(fmt, Layout(array=True), node, 1) == wire
        assert semantic(decode(fmt, Layout(array=True), wire, 1)) == semantic(node)
        count("handwritten_golden_vectors")
    children = {
        "compact": "0d0000008f010a25080700000000000000",
        "block": "2c0000001000010008000000000000000000000000000000140000001000010001000000000000000700000000000000",
    }
    for fmt, golden in children.items():
        wire = bytes.fromhex(golden)
        layout, node = Layout(child=Layout()), Node(child=Node(sequence=7))
        assert encode(fmt, layout, node, 1) == wire
        assert semantic(decode(fmt, layout, wire, 1)) == semantic(node)
        count("handwritten_golden_vectors")


def rewrite():
    layout = Layout(payload=True, array=True, child=Layout(payload=True), slots=4)
    newer = Node(sequence=7, payload=b"root", values=(0, 1, 0xffffffff),
                 child=Node(sequence=8, payload=b"leaf", counter=91, token=b"child-v2"),
                 options={0: 0, 3: 99}, counter=81, token=b"root-v2")
    for fmt in CONTROL_FORMATS:
        wire = encode(fmt, layout, newer, 2)
        old = decode(fmt, layout, wire, 1)
        old.payload = b"root-grown-by-old-reader"
        old.child.payload = b"leaf-grown-by-old-reader"
        old.options[1] = 42
        del old.options[3]
        rewritten = encode(fmt, layout, old, 1)
        final = decode(fmt, layout, rewritten, 2)
        assert final.counter == 81 and final.token == b"root-v2", "ROOT_UNKNOWN_LOSS"
        assert final.child.counter == 91 and final.child.token == b"child-v2", "NESTED_UNKNOWN_LOSS"
        assert final.payload == old.payload and final.child.payload == old.child.payload
        assert final.options == {0: 0, 1: 42}
        count("nested_variable_rewrites")
        if fmt == "compact":
            assert set(old.unknown) == {5, 63}
            old.unknown[2] = b"L\x00"
        else:
            old.unknown_mask |= 2
        rejects("INVALID", encode, fmt, layout, old, 1)


def budgets():
    layout = Layout(array=True, child=Layout(array=True))
    node = Node(values=(1,) * 6, child=Node(values=(2,) * 6))
    for fmt in CONTROL_FORMATS:
        wire = encode(fmt, layout, node, 1)
        # 每个节点各自不过限，但整棵消息树超过总元素上限。
        rejects("LIMIT", decode, fmt, layout, wire, 1, Limits(elements=10))
        parsed, used = decode(fmt, layout, wire, 1, Limits(elements=12), with_usage=True)
        assert len(parsed.values) + len(parsed.child.values) == used["elements"] == 12
        for key in ("fields", "elements", "work"):
            rejects("LIMIT", decode, fmt, layout, wire, 1, Limits(**{key: used[key] - 1}))
            decode(fmt, layout, wire, 1, Limits(**{key: used[key]}))
            count("exact_budget_boundaries")
        rejects("LIMIT", decode, fmt, layout, wire, 1, Limits(frame=len(wire) - 1))
        decode(fmt, layout, wire, 1, Limits(frame=len(wire)))
        deep_layout, deep = nested(8)
        deep_wire = encode(fmt, deep_layout, deep, 2)
        decode(fmt, deep_layout, deep_wire, 1, Limits(depth=8))
        rejects("LIMIT", decode, fmt, deep_layout, deep_wire, 1, Limits(depth=7))
        # 每层都有新字段的输入核对未知字节额度的累计。
        deep.counter, deep.token = 19, b"unknown-root"
        deep.child.counter, deep.child.token = 29, b"unknown-child"
        future = encode(fmt, deep_layout, deep, 2)
        _, u = decode(fmt, deep_layout, future, 1, with_usage=True)
        assert u["unknown"] > 0
        rejects("LIMIT", decode, fmt, deep_layout, future, 1, Limits(unknown=u["unknown"] - 1))
        decode(fmt, deep_layout, future, 1, Limits(unknown=u["unknown"]))
        count("exact_budget_boundaries", 3)


def malformed():
    def frame(body):
        return len(body).to_bytes(4, "little") + body
    # 非最短/溢出整数、重复或逆序 ID、错 kind、越界长度和不完整 u32 数组。
    bodies = [b"\x80\x00\x00", b"\x81\x80\x80\x80\x10\x00",
              b"%\x08" + b"\0" * 8 + b"%\x08" + b"\0" * 8,
              b"L\x00%\x08" + b"\0" * 8,
              b"!\x08" + b"\0" * 8, b"L\x05abc", b"n\x03abc"]
    layout = Layout(payload=True, array=True)
    for body in bodies:
        rejects("INVALID", decode, "compact", layout, frame(body), 2)
    basic = bytearray(encode("block", Layout(payload=True), Node(payload=b"x"), 1))
    bad = bytearray(basic)
    bad[4:6] = b"\xff\xff"
    rejects("INVALID", decode, "block", Layout(payload=True), bad, 1)
    bad = bytearray(basic)
    bad[6:8] = b"\x03\x00"
    rejects("UNSUPPORTED", decode, "block", Layout(payload=True), bad, 1)
    bad = bytearray(basic)
    bad[8] = 0
    rejects("INVALID", decode, "block", Layout(payload=True), bad, 1)
    # 旧端可有界携带不理解的固定扩展；这不证明未来类型本身合法。
    future_layout = Layout(payload=True)
    future = bytearray(encode("block", future_layout, Node(payload=b"x", counter=9, token=b"y"), 2))
    end = 8 + int.from_bytes(future[4:6], "little")
    future[end:end] = b"xyz"
    future[4:6] = (int.from_bytes(future[4:6], "little") + 3).to_bytes(2, "little")
    future[0:4] = (len(future) - 4).to_bytes(4, "little")
    opaque = decode("block", future_layout, future, 1)
    assert encode("block", future_layout, opaque, 1) == future
    rejects("INVALID", decode, "block", future_layout, future, 2)
    count("opaque_future_layout_not_certified")
    # 字段边界截断可形成合法 optional 子集；伪造长度后不一概要求拒绝。
    for fmt in ("compact", "block"):
        wire = encode(fmt, layout, Node(sequence=7, payload=b"abc", values=(1, 2), counter=9, token=b"future"), 2)
        for cut in range(len(wire)):
            rejects("INVALID", decode, fmt, layout, wire[:cut], 2)
            if cut >= 4:
                forged = frame(wire[4:cut])
                try:
                    parsed = decode(fmt, layout, forged, 2)
                except Reject as e:
                    assert e.args == ("INVALID",)
                    count("internal_truncation_rejected")
                else:
                    assert fmt == "compact"
                    assert encode(fmt, layout, parsed, 2) == forged
                    count("valid_optional_prefixes")


def random_roundtrips():
    rng = random.Random(24520261029)
    for _ in range(128):
        layout = Layout(payload=True, array=True, slots=8)
        node = Node(sequence=rng.randrange(1 << 64) if rng.getrandbits(1) else None,
                    payload=rng.randbytes(rng.randrange(129)) if rng.getrandbits(1) else None,
                    values=tuple(rng.randrange(1 << 32) for _ in range(rng.randrange(33))) if rng.getrandbits(1) else None,
                    options={i: rng.randrange(1 << 64) for i in range(8) if rng.getrandbits(1)},
                    counter=rng.randrange(1 << 64), token=rng.randbytes(rng.randrange(17)))
        for fmt in ("compact", "block"):
            wire = encode(fmt, layout, node, 2)
            older = decode(fmt, layout, wire, 1)
            assert semantic(decode(fmt, layout, encode(fmt, layout, older, 1), 2)) == semantic(node)
            count("seeded_unknown_roundtrips")


def mutation():
    name = os.environ.get("CTM_SHAPE_MUTATION")
    if not name:
        return
    assert name in ("drop_root", "drop_nested", "reset_budget")
    print(json.dumps(dict(event="negative_control_start", mutation=name,
                          formats=CONTROL_FORMATS)), flush=True)
    original = model.decode_node
    def changed(fmt, layout, body, reader, budget, depth):
        if name == "reset_budget" and depth > 1:
            budget = model.Budget(budget.limits)
        out = original(fmt, layout, body, reader, budget, depth)
        if reader == 1 and ((name == "drop_root" and depth == 1) or
                            (name == "drop_nested" and depth > 1)):
            out.unknown.clear()
            out.block_tail, out.suffix, out.unknown_mask, out.writer = b"", b"", 0, 1
        return out
    model.decode_node = changed


def main():
    mutation()
    # 先跑能定位具体缺陷的检查，再扩展数据矩阵。
    rewrite()
    budgets()
    known_vectors()
    malformed()
    cases = list(fixtures())
    sizes, vectors = roundtrips(cases)
    random_roundtrips()
    report = dict(event="shape_probe", status="passed", python=sys.version.split()[0],
                  platform=platform.platform(), fixtures=len(cases), counters=COUNTS,
                  source_sha256={name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                                 for name in ("codec_model.py", "check_shapes.py", "xmake.lua", "run_probe.py")},
                  scope=["throwaway_semantic_model", "not_production_codec", "not_upstream_runtime",
                         "python_allocations", "not_cpu_or_rss_measurement", "acyclic_nested_shapes",
                         "no_rust", "bytes_not_utf8", "no_rpc_or_compression"])
    (ROOT / "results.json").write_text(json.dumps(dict(report=report, sizes=sizes, vectors=vectors), ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
