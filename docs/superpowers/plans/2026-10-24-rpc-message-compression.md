# RPC Message Body Compression Implementation Plan

> **Execution:** Inline in the current session/worktree. Do not use background agents. Steps use checkbox syntax so the plan can track the persistent V2 implementation.

**Goal:** Add explicit, bounded Zstandard compression for RPC MESSAGE bodies while keeping RPC record headers, metadata, statuses, identities, and credentials outside the compressor.

**Architecture:** RPC methods declare which algorithms their request and response bodies permit. Each outgoing MESSAGE selects `none` or `zstd` explicitly; the receiving method independently enforces its local policy. `tr_rpc_record_stream` submits only the business body to the Channel's existing MessageService, keeps transformed records in budgeted per-call queues, and emits the unchanged RPC record header outside the compressed bytes. The existing NONE path remains synchronous and zero-copy.

**Tech Stack:** C11, `tr_message`/`tr_message_transform`, Runtime MessageService, Channel/Stream owner callbacks, Xmake, RPC record and V2 integration tests.

**Spec:** `docs/superpowers/specs/2026-10-24-message-layer-compression-design.md`

## Global Constraints

- `none` is the default and requires no MessageService task, wrapper allocation, or body copy.
- Compression is selected per MESSAGE; no implicit fallback from Zstandard to `none` is allowed.
- The compressor receives only MESSAGE body bytes. RPC record headers and metadata remain separate; OPEN, FINAL_STATUS, identity, credentials, and status bytes are never compressed.
- Method policy is direction-specific. Unknown compression bits and disallowed algorithms are rejected before state or caller ownership changes.
- Zstandard work runs through the Channel's owning Runtime MessageService, never on a Reactor callback or application worker.
- Every queue node, encoded input/output, transform workspace, and temporary TX fragment uses the existing nonzero memory budget and a finite service/task capacity.
- Per-call transform completion is delivered in RPC record order, even if worker completions arrive out of order.
- RPC record bodies larger than the current Stream credit are sent as bounded byte chunks; each accepted chunk is an ordinary Channel message and receive credit can progress before the whole RPC record arrives.
- A stale call/Stream generation, cancelled operation, retired physical connection, or OWNER_STOPPED result cannot send or deliver a body.
- Xmake commands use `TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" PYTHONDONTWRITEBYTECODE=1`; do not create test files, logs, caches, or outputs under `/tmp`.

## File Map

- `include/tr/rpc.h`: public compression enum and per-direction method policy masks.
- `include/tr/rpc_v2.h`: explicit send/append/producer APIs; existing entry points continue selecting NONE.
- `src/rpc/rpc_record_dispatch.c` and `src/rpc/rpc_record_producer.c`: retain each response MESSAGE's selected algorithm with its copied body.
- `src/rpc/rpc_record_stream.[ch]`: policy checks, body-only transforms, bounded ordered TX/RX nodes, cancellation, generation checks, and RPC record chunk framing.
- `src/transport/channel/channel_internal.h` and `src/transport/channel/channel.c`: expose an owner-only snapshot of local/peer compression limits needed by RPC; continue using the existing MessageService route.
- `tests/test_rpc_record.c`, `tests/test_rpc_record_stream.c`, `tests/test_client_record.c`, `tests/test_rpc_v2_sdk.c`, `tests/test_rpc_v2_server.c`, and `tests/test_rpc_record_endpoint_tcp.c`: policy, transform lifecycle, record separation, ordering, resource, and live endpoint coverage.
- `xmake.lua`: add any new implementation unit to the explicit production/test source lists.
- `docs/superpowers/specs/2026-10-24-message-layer-compression-design.md`: record the completed RPC contract and supported constraints.

---

### Task 1: Define and validate the opt-in RPC method policy

**Files:** `include/tr/rpc.h`, `src/client.c`, `src/server.c`, `src/rpc/rpc_record_router.c`, `tests/test_rpc_record_router.c`, `tests/test_rpc_v2_sdk.c`.

**Interfaces:**

- Add `enum tr_rpc_compression { TR_RPC_COMPRESSION_NONE = 0, TR_RPC_COMPRESSION_ZSTD = 1 }` and `TR_RPC_COMPRESSION_MASK_ZSTD` (`1U << TR_RPC_COMPRESSION_ZSTD`). NONE is implicit and is never a mask bit.
- Append `request_compression_algorithms` and `response_compression_algorithms` to `struct tr_rpc_method_desc`. Zero means that direction permits NONE only.
- Existing APIs continue selecting NONE until the later facade task wires explicit per-message selection.

- [x] **Step 1: Write failing public-policy tests**

Test that zero-initialized legacy descriptors remain valid, unknown method mask bits are rejected by router creation, and a ZSTD-enabled method descriptor is accepted. In `test_rpc_v2_sdk`, assert `tr_client_create` rejects an unknown mask bit before publishing a Client.

- [x] **Step 2: Run focused tests and confirm the expected failure**

Run `xmake run -P . test_rpc_record_router` and `xmake run -P . test_rpc_v2_sdk` with the required local TMPDIR variables. Expected: the new descriptor assertions fail to compile because the policy fields and enum do not exist yet.

- [x] **Step 3: Implement the public method policy surface and validation**

Add the enum, mask, and appended method fields. Reject unknown mask bits consistently in Client, Server, and RPC router method-table validation. A zero-initialized descriptor continues to mean NONE only.

- [x] **Step 4: Run focused SDK/server tests**

Run both focused targets again. Expected: legacy descriptors still construct and unknown policy bits are rejected without publishing a facade or reserving persistent budget.

### Task 2: Add RPC access to directional Channel limits

**Files:** `src/transport/channel/channel_internal.h`, `src/transport/channel/channel.c`, `tests/test_transport.c`.

**Interfaces:**

- Add an owner-only `tr_channel_message_limits_on_owner(channel, lane, out)` snapshot returning local receive algorithm bits/encoded limit, peer receive algorithm bits/encoded limit/readiness, local max message bytes, compression workspace bytes, and window log.
- The helper is a value snapshot only; it does not enable RPC compression or retain Channel-owned lane state.
- Convert the Channel encoded-wire limit to an RPC encoded-body ceiling by subtracting the 16-byte Message envelope conservatively when the record-stream adapter consumes this snapshot.

- [x] **Step 1: Add failing directional capability snapshot test**

With the existing real Channel handshake, assert the owner snapshot reports distinct local and peer receive limits. Assert the existing lane replacement scenario returns AGAIN while the lane is down and only reports the new peer's values after the replacement handshake completes.

- [x] **Step 2: Run the focused test and confirm the expected failure**

Run `xmake run -P . test_transport` and `xmake run -P . test_rpc_record_stream`. Expected: the internal limits accessor is absent and the new RPC admission assertions fail.

- [x] **Step 3: Implement the owner-only limits snapshot**

Read Channel configuration and per-lane capability state under the existing owner/lock contract. Do not retain pointers into Channel capability storage. Preserve an unready peer snapshot when capability exchange is absent.

- [x] **Step 4: Re-run the focused test**

Expected: capability readiness and limits are generation-specific. This accessor does not change the existing NONE-only RPC record path.

### Task 3: Transform received RPC bodies asynchronously and in order

**Files:** `src/rpc/rpc_record_stream.c`, `src/rpc/rpc_record_stream.h`, `tests/test_rpc_record_stream.c`, `xmake.lua` only if a focused helper unit is added.

**Interfaces:**

- Keep `tr_rpc_record_stream_receive_on_owner()` as the byte-stream parser. For NONE, preserve the current direct borrowed view path.
- For ZSTD, copy only encoded business-body bytes into a budgeted Message, submit `TR_MESSAGE_ZSTD_DECODE`, and deliver the existing `tr_rpc_record_view` callback only after decode succeeds and the actual output length equals `decoded_size`.
- Add a bounded ordered receive queue. Each queued record carries its sequence, header, copied metadata, and either a transform handle or a ready owned Message. Commit RPC state in delivery order; a later ready completion cannot overtake an earlier pending transform.
- On cancel/reset/terminal, cancel every accepted task and release queued state only after its exactly-once completion. OWNER_STOPPED completion releases local Message/budget state without touching Stream or RPC callbacks.

- [x] **Step 1: Write failing receive test**

Send real Zstandard-encoded RPC MESSAGE records through paired Channels. Assert the callback sees decoded bytes and original decoded length, not encoded bytes. Add two back-to-back compressed records whose completion order is deliberately reversed and assert callback order remains record order. Assert the NONE test still observes the original input body address.

- [x] **Step 2: Run the focused test and observe RED**

Run `xmake run -P . test_rpc_record_stream`. Expected: Zstandard is currently rejected by the binding's NONE-only receive limits.

- [x] **Step 3: Implement receive transform nodes and ordered delivery**

Reserve queue-node and Message transform resources before accepting the record. Keep method/codec/decoded-size/state preflight before transform submission. On any decode or budget failure, terminate the call with the mapped RPC status and clear later queued records.

- [x] **Step 4: Verify receive success, ordering, failure, and lifecycle**

  - [x] Real MessageService decode delivers the original body and decoded length.
  - [x] Deterministically complete the second queued record before the first and verify delivery order.
  - [x] Reject malformed, truncated, trailing, and decoded-size-mismatched Zstandard frames before callback delivery.
  - [x] Verify local cancel and RESET wait for accepted transform completion; stale Stream completion cannot deliver a body.
  - [x] Reap OWNER_STOPPED work and verify RPC/message budgets return to zero.
  - [x] Verify the NONE path still lends the original RX body address.
  - [x] Exercise MessageService stop while an RPC transform is pending, plus RPC receive budget/workspace exhaustion.

### Task 4: Transform outgoing RPC bodies and stream record bytes through credit

**Files:** `src/rpc/rpc_record_stream.c`, `src/rpc/rpc_record_stream.h`, `tests/test_rpc_record_stream.c`, `tests/test_rpc_record_endpoint_tcp.c`.

**Interfaces:**

- `tr_rpc_record_stream_send_on_owner()` accepts ZSTD only when the descriptor policy and peer Channel capability permit it; `decoded_size` is the caller body size and `encoded_size` is populated from actual transform output.
- A successful MessageService submit consumes the body and commits one record intent. Transform failure selects a terminal RESOURCE_EXHAUSTED, INVALID_ARGUMENT, or INTERNAL result and never emits a partial RPC header.
- After encoding, keep a budgeted per-call TX node until all record bytes have been accepted by Channel. Send the fixed RPC header/metadata followed by the encoded body as a byte stream; when current Channel credit is smaller than the record, copy and send a bounded chunk no larger than current credit, then resume on WRITABLE. NONE continues using one existing scatter/gather send.
- Ordered TX nodes cannot interleave bytes on one RPC stream. A later message is accepted only when its node and transform resources are reserved; all pending storage remains limited by the existing hard budget and task capacity.

- [x] **Step 1: Add focused TX behavior tests**

Assert unapproved ZSTD returns an error and retains the caller buffer; approved ZSTD consumes only after MessageService accepts; wire records contain a plain RPC header whose compression/encoded/decoded fields match the produced body. Use a small Stream window to force the RPC record across multiple Channel messages and verify the peer parser reconstructs exactly one record.

- [x] **Step 2: Run focused TX tests**

Run `xmake run -P . test_rpc_record_stream` and `xmake run -P . test_rpc_record_endpoint_tcp`. Both pass with the ZSTD sender enabled and records larger than current Stream credit.

- [x] **Step 3: Implement TX transform, ordered pending nodes, and credit-driven chunking**

Build the record header only after actual encoded size is known. Store no RPC metadata in the transform input. Advance the TX offset only after `tr_stream_sendv()` returns TR_OK; on AGAIN retain the exact node and retry on the next owner WRITABLE event.

- [x] **Step 4: Verify ownership, ordering, C<W/C=W/C>W, and termination**

  - [x] Rejected sends retain caller input; transform queue full returns `TR_AGAIN`; encoded-limit failure emits no partial record.
  - [x] Verify `C < W`, `C = W`, and `C > W` with real paired Channels.
  - [x] Complete compressed sends on four independent Streams out of order and verify each decoded body and header independently.
  - [x] Reset, explicit abort, and physical lane loss cancel pending TX transforms; late completion does not send a MESSAGE.
  - [x] Verify capability snapshots become unavailable after lane retirement; `test_transport` covers capability retirement/re-handshake on connection replacement.
  - [x] Reap OWNER_STOPPED pending TX work and verify RPC/message budgets return to zero.

### Task 5: Connect public Client, Server, and Producer selection

**Files:** `src/client.c`, `src/rpc/rpc_record_client.c`, `src/rpc/rpc_record_dispatch.c`, `src/rpc/rpc_record_producer.c`, `include/tr/rpc_v2.h`, `tests/test_client_record.c`, `tests/test_rpc_v2_sdk.c`, `tests/test_rpc_v2_server.c`.

**Interfaces:**

- Append `request_compression` to `struct tr_rpc_call_options`; zero-initialized existing callers retain NONE.
- Add `tr_rpc_v2_call_send_with_compression(call, body, compression)`, `tr_rpc_v2_reply_append_with_compression(reply, body, compression)`, and `tr_rpc_v2_producer_send_with_compression(producer, body, compression)`. Existing functions delegate to these with NONE.
- Store the selected response algorithm alongside each owned response body until record transmission.
- Client `call_send_with_compression` selects the next request MESSAGE; unary `tr_rpc_call_options.request_compression` selects its one automatic request MESSAGE.
- Server synchronous `reply_append_with_compression` and asynchronous `producer_send_with_compression` select one response MESSAGE each.
- Existing send/append/producer calls remain byte-compatible NONE wrappers.
- The method descriptor's request/response policy applies symmetrically at each endpoint; the receiver's own setting is authoritative. No automatic retry or fallback occurs.

- [x] **Step 1: Add facade tests**

Exercise one unary compressed request, one streaming compressed request, one synchronous compressed response, and one producer compressed response through real V2 Client/Server facades. Inspect the received body and RPC header; include sentinel bytes in metadata/OPEN/status and assert they remain outside the compressed body.

- [x] **Step 2: Run focused facade tests**

Run `xmake run -P . test_rpc_v2_sdk`, `xmake run -P . test_rpc_v2_server`, and `xmake run -P . test_client_record`. All three pass with legacy NONE behavior and explicit compression selection.

- [x] **Step 3: Pass per-message selections through the existing fixed slots and reply ownership**

Copy compression selection with each body into the existing bounded request/reply/producer representation. Reject disallowed algorithms before taking source bytes; pass the selection to the RPC record stream only when the existing progress owner is ready.

- [x] **Step 4: Verify API compatibility and failure ownership**

Run the three targets plus `test_rpc_record_endpoint_tcp`. Verify legacy entry points always choose NONE, invalid/disallowed selections retain caller input, and an accepted compressed body is delivered or terminated exactly once.

### Task 6: Complete security, lifecycle, and full-suite acceptance

**Files:** focused RPC/channel tests and `docs/superpowers/specs/2026-10-24-message-layer-compression-design.md`.

- [x] **Step 1: Complete compression security/fault cases**

Use authentication/credential-like metadata and body sentinels; prove the RPC header, metadata, final status, identity, and credential fields never enter the Message transform. Reject unknown algorithm IDs, disallowed method directions, mismatched sizes, malformed Zstandard frames, trailing data, and local/peer encoded limits before callback delivery.

  - [x] Reject unknown policy bits/algorithm IDs, disallowed method directions, malformed/trailing frames, mismatched decoded sizes, and encoded-limit overflow.
  - [x] Verify metadata remains byte-identical across compressed request/reply paths.
  - [x] Add credential-like and peer-id metadata sentinels; the transform adapter verifies its input is exactly the RPC message body. Connection-authenticated identity remains part of the separate TLS/connection-group audit.

- [x] **Step 2: Complete lifecycle/resource conservation checks**

Run the RPC tests under available ASan/UBSan and thread tests under TSan. Exercise cancellation, RESET, physical lane replacement, transform completion after terminal, owner shutdown/reaper delivery, and destruction with live Message references. Assert memory-budget/task counts return to baseline after all accepted work settles.

  - [x] Run `test_rpc_record_stream` and `test_rpc_v2_sdk` under ASan/UBSan; run `test_runtime_threads` under TSan.
  - [x] Exercise compressed TX/RX cancellation, RESET, lane retirement, late completion, and OWNER_STOPPED reaping with budget checks.
  - [x] Exercise replacement-handshake generation change while a compressed RPC TX transform is pending; a late successful callback after cancellation must not send on the replacement lane.
  - [x] Add a dedicated compressed TX destruction case with a live encoded Message; destroy is rejected while the item is pending, then succeeds after abort releases it.

- [x] **Step 3: Run focused and complete test suites**

Run `test_rpc_record`, `test_rpc_record_stream`, `test_rpc_record_endpoint_tcp`, `test_client_record`, `test_rpc_v2_sdk`, `test_rpc_v2_server`, then `xmake test -P . -j1` with all Xmake temporary paths under this worktree.

  - [x] Latest full-suite run passed 88/88 in 80.540 s. Added a regression for a compressed response whose async FINAL decode races with normal remote close; the ordered receive queue must finish decoding before applying that close. `test_connection_group_facade` also passed 10 consecutive runs after its asynchronous teardown assertions were changed to use the existing bounded destroy-retry helpers.

- [x] **Step 4: Update design evidence**

Record RPC body policy, record chunking, the full-suite and close-race regression evidence, and the RPC TLS × compression benchmark in the design spec. Keep connection-group performance, representative application workloads, and final release thresholds listed as remaining work; do not mark overall Message compression or V2 complete until those milestones also pass.

## Scope Boundary

This plan closes only RPC MESSAGE body compression. Its loopback benchmark does not close connection-group DATA performance or final security/performance acceptance; those remain separate required milestones in the parent design spec.
