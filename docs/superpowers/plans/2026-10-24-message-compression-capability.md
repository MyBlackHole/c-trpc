# Message Compression Capability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a feature-gated, directional Message compression capability exchange to Channel while leaving unnegotiated peers and the `none` data path unchanged.

**Architecture:** Reuse the existing HELLO/ACK feature intersection to gate one `MESSAGE_CAPS` control frame. Each physical Channel lane records the peer's receive algorithm mask and encoded wire limit independently; this plan does not advertise or send compressed DATA yet.

**Tech Stack:** C11, Xmake, existing TRP frame parser, Channel Reactor owner callbacks, `tests/test_transport.c`.

**Spec:** `docs/superpowers/specs/2026-10-24-message-layer-compression-design.md`; decision rationale: `docs/architecture/decisions/ADR-008-message-compression-protocol.md`.

## Global Constraints

- Use feature bit `TR_CHANNEL_FEATURE_MESSAGE_CAPS`; do not change TRP header size/version or consume header reserved fields.
- `TR_FRAME_MESSAGE_CAPS` payload is exactly 12 bytes: schema version `u16=1`, reserved `u16=0`, receive algorithm mask `u32`, max encoded wire bytes `u32`, all little-endian.
- The codec recognizes `TR_MESSAGE_ALGORITHM_ZSTD`, but Channel must advertise an empty local receive set until compressed DATA decoding and per-frame credit return are implemented; `none` is implicit and has no bit.
- Send `MESSAGE_CAPS` only when HELLO/ACK feature intersection includes `MESSAGE_CAPS`; no negotiated feature means no new frame and all existing behavior remains available.
- A peer mask of zero requires a zero encoded limit. A nonzero peer mask requires an encoded limit of at least 17 bytes. The peer's value is its receive limit, not a request to allocate that amount locally.
- Do not alter the two user-owned untracked plans in the original worktree.
- Run Xmake with `TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" PYTHONDONTWRITEBYTECODE=1`; do not write test files, logs, caches, or build outputs under `/tmp`.

---

### Task 1: Add the Message capability frame and codec

**Files:**
- Modify: `src/transport/protocol/wire.h`
- Modify: `src/transport/protocol/wire.c`
- Create: `src/transport/protocol/message_caps.h`
- Create: `src/transport/protocol/message_caps.c`
- Modify: `tests/test_transport.c`
- Modify: `xmake.lua`

**Interfaces:**
- Produces `TR_FRAME_MESSAGE_CAPS = 16` and `struct tr_message_caps { uint32_t receive_algorithm_bits; uint32_t max_encoded_wire_bytes; }`.
- Produces `TR_MESSAGE_ALGORITHM_ZSTD = 1U << 0`, `tr_message_caps_encode(uint8_t out[12], const struct tr_message_caps *)`, and `tr_message_caps_decode(const uint8_t *, uint32_t, struct tr_message_caps *)`.

- [x] **Step 1: Write the failing codec test**

Add `test_message_caps_roundtrip_and_rejects_invalid_payload` in `tests/test_transport.c`. Round-trip the ZSTD bit with a 4096-byte limit. Assert `TR_ERR_BAD_LENGTH` for payload length 11, `TR_ERR_RESERVED` for a nonzero reserved field, `TR_ERR_UNSUPPORTED` for an unknown algorithm bit, `TR_ERR_INVALID` when mask=0 but max=4096, and `TR_ERR_BAD_LENGTH` when mask=ZSTD but max=16.

- [x] **Step 2: Run the focused test and observe failure**

Run: `TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" PYTHONDONTWRITEBYTECODE=1 xmake run -P . test_transport`

Expected: compilation fails because `tr_message_caps_*` and `TR_FRAME_MESSAGE_CAPS` are not defined.

- [x] **Step 3: Implement the schema codec and frame registration**

Implement fixed-offset little-endian read/write for the 12-byte schema. Reject null parameters, invalid masks, inconsistent empty capability, limits below 17, and wrong payload length. Register frame type 16 in `tr_frame_type_valid`; keep the frame-header reserved field zero. Add `message_caps.c` to the explicit `trcore` source list in `xmake.lua`.

- [x] **Step 4: Run the focused test and verify it passes**

Run the command from Step 2. Expected: `test_transport` exits 0 and the new codec assertions pass.

- [ ] **Step 5: Commit the protocol primitive**

```bash
git add src/transport/protocol/wire.h src/transport/protocol/wire.c src/transport/protocol/message_caps.h src/transport/protocol/message_caps.c tests/test_transport.c xmake.lua
git commit -m "feat: 增加 Message 能力帧编解码"
```

### Task 2: Exchange and retain directional capabilities

**Files:**
- Modify: `src/transport/channel/channel.h`
- Modify: `src/transport/channel/channel.c`
- Modify: `tests/test_transport.c`

**Interfaces:**
- Consumes Task 1's `tr_message_caps_encode/decode`.
- Adds local receive fields `receive_compression_algorithms` and `max_encoded_message_bytes` to `tr_channel_config`.
- Adds peer receive fields `receive_algorithm_bits`, `max_encoded_wire_bytes`, and `message_caps_ready` to `tr_channel_capabilities`; `tr_channel_get_capabilities()` reports the peer's receive limits for the selected lane. Never compare the peer's receive limit to the local receive limit: each bounds a different send direction.

- [x] **Step 1: Write the failing negotiation tests**

Add Channel capability exchange, feature-gating, and uncompressed Stream tests. The current Channel must exchange an empty local receive set and remain ready; configuration that tries to advertise ZSTD must be rejected until compressed DATA receive support exists.

- [x] **Step 2: Run the focused tests and observe failure**

Run: `TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" PYTHONDONTWRITEBYTECODE=1 xmake run -P . test_transport`

Expected: compilation fails because the new config/capability fields and feature constant are absent.

- [x] **Step 3: Implement owner-serialized exchange**

Store local receive capability in Channel config. When HELLO/ACK establishes a shared `MESSAGE_CAPS` feature, enqueue exactly one local capability frame before publishing the lane READY, then accept exactly one peer capability frame on the owning Reactor. Validate frame identity (`flags=0`, `stream_id=0`, `message_id=0`, header reserved=0) and schema. Store the peer fields under the Channel lock and publish `message_caps_ready`. Reject duplicate capability frames. Reset peer capability fields when the physical lane goes down or is replaced. Keep `tr_channel_wait_ready()` semantics unchanged so `none` traffic is not delayed by waiting for the peer capability frame.

- [x] **Step 4: Run the focused tests and verify the negotiation**

Run the command from Step 2. Expected: directional capability assertions pass, feature-absent peers exchange no new frame, and ordinary uncompressed Stream traffic still succeeds.

- [ ] **Step 5: Commit the Channel capability exchange**

```bash
git add src/transport/channel/channel.h src/transport/channel/channel.c tests/test_transport.c
git commit -m "feat: 协商方向性 Message 接收能力"
```

### Task 3: Verify lane replacement and malformed capability termination

**Files:**
- Modify: `src/transport/channel/channel.c`
- Modify: `tests/test_transport.c`

**Interfaces:**
- Uses Task 2 capability state; no new public interface.

- [x] **Step 1: Write failing lifecycle and malformed-frame tests**

Add `test_channel_message_caps_clear_on_lane_replacement` and `test_channel_message_caps_reject_malformed_frame`. The replacement test negotiates ZSTD, replaces the physical connection, and asserts the replacement lane cannot report `message_caps_ready` until its new peer capability frame arrives. The malformed-frame test injects a `MESSAGE_CAPS` frame with a nonzero stream id, verifies that the exact physical connection is closed, and verifies the closed lane no longer returns capabilities.

- [x] **Step 2: Run tests and observe failure**

Run: `TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" PYTHONDONTWRITEBYTECODE=1 xmake run -P . test_transport`

Expected: at least one lifecycle or malformed-frame assertion fails before the implementation.

- [x] **Step 3: Complete lane-generation cleanup and strict frame validation**

Clear the capability-ready bit, algorithm mask, and encoded limit in the same owner transition that clears lane readiness. Validate all control-frame identity and reserved fields before publishing state. A malformed or unsupported capability frame must follow existing Channel protocol-error termination; it must not leave a partial capability record.

- [x] **Step 4: Run the complete transport test target**

Run: `TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" PYTHONDONTWRITEBYTECODE=1 xmake run -P . test_transport`

Expected: all existing transport groups and the new capability negotiation/lifecycle groups pass.

- [ ] **Step 5: Commit the lifecycle checks**

```bash
git add src/transport/channel/channel.c tests/test_transport.c
git commit -m "test: 校验 Message 能力重连与错误回收"
```

### Task 4: Update architecture indexes and verify documentation consistency

**Files:**
- Modify: `docs/architecture/decisions/README.md`
- Modify: `docs/architecture/11-compression-tls-requirements.md`
- Modify: `docs/architecture/README.md`

**Interfaces:**
- Links the proposed protocol design and implementation spec; no code API changes.

- [x] **Step 1: Add the ADR index entry and correct the layer ownership text**

Add ADR-008 to the decisions index. In the compression requirement section, replace the old `Channel/Stream` algorithm-ownership text with independent `src/message/` ownership and link ADR-008; keep TLS requirements intact.

- [x] **Step 2: Check the stale-claim search**

Run: `rg -n "压缩属于.*Channel|Channel/Stream.*逻辑消息处理|唯一实现归传输层" docs/architecture`

Expected: no current architecture claim assigns the compression implementation to Channel. Any retained historical statement labels itself as superseded.

- [x] **Step 3: Verify links and repository formatting**

Run: `git diff --check` and `git status --short`. Expected: no whitespace errors; all created paths are within this worktree.

---

## Scope boundary

Completing this plan establishes directional negotiation only. It does not close #235 and does not claim compressed DATA, `C > W` streaming, RPC adaptation, connection-group adaptation, security acceptance, or performance acceptance. Those are the next independently reviewable plans under the same spec.

The implementation commit steps above remain unchecked until the development version can be synchronized with the open V2 PR heads (#312 `.21`, #313 `.19`) without regressing the base `.17` or changing their draft state.
