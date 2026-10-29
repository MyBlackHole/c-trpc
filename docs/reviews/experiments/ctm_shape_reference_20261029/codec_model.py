"""有界形状的可丢弃语义参考；Python 对象分配不代表产品内存合同。"""
from dataclasses import dataclass, field
import struct


class Reject(Exception):
    pass


def need(ok, code="INVALID"):
    if not ok:
        raise Reject(code)


@dataclass(frozen=True)
class Layout:
    payload: bool = False
    array: bool = False
    child: object = None
    slots: int = 0

    def fixed(self):
        return 16 + 8 * self.slots


@dataclass
class Node:
    sequence: object = None
    payload: object = None
    values: object = None
    child: object = None
    options: dict = field(default_factory=dict)
    counter: object = None
    token: object = None
    # 未知字段由格式私有原始片段携带，不能跨候选解释。
    unknown: dict = field(default_factory=dict)
    block_tail: bytes = b""
    suffix: bytes = b""
    unknown_mask: int = 0
    writer: int = 1


@dataclass(frozen=True)
class Limits:
    frame: int = 1048576
    fields: int = 512
    elements: int = 4096
    depth: int = 8
    work: int = 8192
    unknown: int = 65536


class Budget:
    def __init__(self, limits):
        self.limits = limits
        self.used = dict(fields=0, elements=0, work=0, unknown=0)

    def charge(self, name, count):
        need(count >= 0)
        self.used[name] += count
        need(self.used[name] <= getattr(self.limits, name), "LIMIT")

    def node(self, depth):
        need(depth <= self.limits.depth, "LIMIT")
        self.charge("work", 1)

    def fields(self, count):
        self.charge("fields", count)
        self.charge("work", count)

    def elements(self, count):
        self.charge("elements", count)
        self.charge("work", count)


def varint(value):
    need(isinstance(value, int) and 0 <= value <= 0xffffffff)
    out = bytearray()
    while value >= 128:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


class Cursor:
    def __init__(self, data):
        self.data = memoryview(data)
        self.pos = 0

    def take(self, size):
        need(0 <= size <= len(self.data) - self.pos)
        out = self.data[self.pos:self.pos + size]
        self.pos += size
        return out

    def uint(self, size):
        return int.from_bytes(self.take(size), "little")

    def varint(self):
        out = 0
        for i in range(5):
            b = self.uint(1)
            need(i < 4 or b < 16)
            out |= (b & 127) << (i * 7)
            if b < 128:
                need(i == 0 or b != 0)
                return out
        raise Reject("INVALID")


def number(value, bits):
    need(isinstance(value, int) and 0 <= value < 1 << bits)
    return value.to_bytes(bits // 8, "little")


def known(layout, reader):
    fields = {1: (5, "sequence")}
    if layout.payload:
        fields[2] = (12, "payload")
    if layout.array:
        fields[3] = (14, "values")
    if layout.child:
        fields[4] = (15, "child")
    fields.update({8 + i: (5, i) for i in range(layout.slots)})
    if reader == 2:
        fields.update({5: (5, "counter"), 63: (12, "token")})
    return fields


def raw_fields(body):
    c, previous = Cursor(body), 0
    while c.pos < len(c.data):
        start = c.pos
        tag, size = c.varint(), c.varint()
        identity, kind = tag >> 5, tag & 31
        need(identity > previous)
        previous = identity
        data = c.take(size)
        yield identity, kind, data, bytes(c.data[start:c.pos])


def known_mask(layout, reader):
    mask = 1 | (2 if layout.payload else 0) | (4 if layout.array else 0)
    mask |= 8 if layout.child else 0
    mask |= ((1 << layout.slots) - 1) << 8
    return mask | (48 if reader == 2 else 0)


def decode_node(fmt, layout, body, reader, budget, depth):
    budget.node(depth)
    out = Node()
    if fmt == "compact":
        schema = known(layout, reader)
        for identity, kind, data, raw in raw_fields(body):
            budget.fields(1)
            if identity not in schema:
                budget.charge("unknown", len(raw))
                out.unknown[identity] = raw
                continue
            expected, name = schema[identity]
            need(kind == expected)
            if kind == 5:
                need(len(data) == 8)
                value = int.from_bytes(data, "little")
            elif name == "values":
                need(len(data) % 4 == 0)
                budget.elements(len(data) // 4)
                value = tuple(x[0] for x in struct.iter_unpack("<I", data))
            elif name == "child":
                value = decode_node(fmt, layout.child, data, reader, budget, depth + 1)
            else:
                value = bytes(data)
            if isinstance(name, int):
                out.options[name] = value
            else:
                setattr(out, name, value)
        return out

    c = Cursor(body)
    block_size, writer = c.uint(2), c.uint(2)
    need(writer in (1, 2), "UNSUPPORTED")
    if writer == 1 or reader == 2:
        need(block_size == layout.fixed() + (8 if writer == 2 else 0))
    else:
        # 旧读者只知道旧固定区，新增区域的长度来自块边界。
        need(block_size >= layout.fixed())
    fixed = Cursor(c.take(block_size))
    mask, sequence = fixed.uint(8), fixed.uint(8)
    if writer == 1 or reader == 2:
        need(not mask & ~known_mask(layout, writer))
    budget.fields(1 + layout.slots)
    out.sequence = sequence if mask & 1 else None
    need(out.sequence is not None or sequence == 0)
    for i in range(layout.slots):
        value = fixed.uint(8)
        if mask & (1 << (8 + i)):
            out.options[i] = value
        else:
            need(value == 0)
    if writer == 2:
        if reader == 2:
            value = fixed.take(8)
            budget.fields(1)
            out.counter = int.from_bytes(value, "little") if mask & 16 else None
            need(out.counter is not None or not any(value))
        else:
            out.block_tail = bytes(fixed.take(len(fixed.data) - fixed.pos))
            budget.charge("unknown", len(out.block_tail))
    for enabled, bit, name in [(layout.payload, 2, "payload"),
                                (layout.array, 4, "values"),
                                (layout.child is not None, 8, "child")]:
        if not enabled:
            continue
        budget.fields(1)
        data = c.take(c.uint(4))
        if not mask & bit:
            need(len(data) == 0)
            continue
        if name == "values":
            need(len(data) % 4 == 0)
            budget.elements(len(data) // 4)
            value = tuple(x[0] for x in struct.iter_unpack("<I", data))
        elif name == "child":
            value = decode_node(fmt, layout.child, data, reader, budget, depth + 1)
        else:
            value = bytes(data)
        setattr(out, name, value)
    if writer == 2:
        if reader == 2:
            budget.fields(1)
            data = c.take(c.uint(4))
            need(mask & 32 or len(data) == 0)
            out.token = bytes(data) if mask & 32 else None
        else:
            out.suffix = bytes(c.take(len(c.data) - c.pos))
            budget.charge("unknown", len(out.suffix))
    need(c.pos == len(c.data))
    out.unknown_mask = mask & ~known_mask(layout, reader)
    out.writer = writer
    return out


def decode(fmt, layout, frame, reader, limits=Limits(), with_usage=False):
    need(fmt in ("compact", "block") and reader in (1, 2))
    need(len(frame) <= limits.frame, "LIMIT")
    c = Cursor(frame)
    need(c.uint(4) == len(frame) - 4)
    budget = Budget(limits)
    out = decode_node(fmt, layout, c.take(len(frame) - 4), reader, budget, 1)
    return (out, budget.used) if with_usage else out


def encode_node(fmt, layout, node, reader, budget, depth):
    budget.node(depth)
    need(0 <= layout.slots <= 32)
    need(all(isinstance(i, int) and 0 <= i < layout.slots for i in node.options))
    need(layout.payload or node.payload is None)
    need(layout.array or node.values is None)
    need(layout.child or node.child is None)
    need(reader == 2 or (node.counter is None and node.token is None))
    values = {}
    if node.sequence is not None:
        values[1] = (5, number(node.sequence, 64))
    if node.payload is not None:
        need(isinstance(node.payload, bytes))
        values[2] = (12, node.payload)
    if node.values is not None:
        budget.elements(len(node.values))
        values[3] = (14, b"".join(number(x, 32) for x in node.values))
    if node.child is not None:
        values[4] = (15, encode_node(fmt, layout.child, node.child, reader, budget, depth + 1))
    for i, value in node.options.items():
        values[8 + i] = (5, number(value, 64))
    if reader == 2:
        if node.counter is not None:
            values[5] = (5, number(node.counter, 64))
        if node.token is not None:
            need(isinstance(node.token, bytes))
            values[63] = (12, node.token)
    if fmt == "compact":
        need(not node.block_tail and not node.suffix and not node.unknown_mask)
        records = {i: varint(i << 5 | k) + varint(len(v)) + v
                   for i, (k, v) in values.items()}
        schema = known(layout, reader)
        for i, raw in node.unknown.items():
            # 不能用携带接口伪造一个当前已知字段，连当前缺席字段也不行。
            need(i not in schema)
            parsed = list(raw_fields(raw))
            need(len(parsed) == 1 and parsed[0][0] == i)
            budget.charge("unknown", len(raw))
            records[i] = raw
        budget.fields(len(records))
        return b"".join(records[i] for i in sorted(records))

    need(not node.unknown)
    writer = max(reader, node.writer)
    need(writer in (1, 2), "UNSUPPORTED")
    need(not node.unknown_mask & known_mask(layout, reader))
    need(writer != 1 or node.unknown_mask == 0)
    need(reader == 1 or not (node.block_tail or node.suffix or node.unknown_mask & 48))
    mask = node.unknown_mask
    for identity, bit in [(1, 1), (2, 2), (3, 4), (4, 8), (5, 16), (63, 32)]:
        if identity in values:
            mask |= bit
    for i in node.options:
        mask |= 1 << (8 + i)
    fixed = number(mask, 64) + number(node.sequence or 0, 64)
    fixed += b"".join(number(node.options.get(i, 0), 64) for i in range(layout.slots))
    budget.fields(1 + layout.slots)
    if writer == 2:
        if reader == 2:
            fixed += number(node.counter or 0, 64)
            budget.fields(1)
        else:
            fixed += node.block_tail
            budget.charge("unknown", len(node.block_tail))
    body = number(len(fixed), 16) + number(writer, 16) + fixed
    for enabled, identity in [(layout.payload, 2), (layout.array, 3), (layout.child is not None, 4)]:
        if enabled:
            budget.fields(1)
            data = values.get(identity, (0, b""))[1]
            body += number(len(data), 32) + data
    if writer == 2:
        if reader == 2:
            data = node.token if node.token is not None else b""
            body += number(len(data), 32) + data
            budget.fields(1)
        else:
            body += node.suffix
            budget.charge("unknown", len(node.suffix))
    return body


def encode(fmt, layout, node, reader, limits=Limits()):
    need(fmt in ("compact", "block") and reader in (1, 2))
    body = encode_node(fmt, layout, node, reader, Budget(limits), 1)
    need(len(body) + 4 <= limits.frame, "LIMIT")
    return number(len(body), 32) + body
