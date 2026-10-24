# Connection Group DATA Message Compression Implementation Plan

> **Execution:** Inline in the current worktree. Do not use background agents. Preserve all existing uncommitted changes. Use local Xmake temporary paths under `build/`; do not create test files, logs, caches, or outputs under `/tmp`.

**Goal:** Add explicit, bounded Zstandard compression to client-to-server connection-group DATA while preserving the existing NONE send/receive path and tying negotiated permission and callbacks to the exact DATA route generation.

**Architecture:** The client and server opt in through directional connection-group configuration; the application selects Zstandard per send. A client-initiated `TR_FRAME_MESSAGE_CAPS` exchange on each attached DATA connection advertises the server's receive limits without sending new frames to old clients. Client TX and server RX use their Runtime shard's MessageService, with finite budgets derived from configured message and transfer limits. Server callbacks receive one complete decoded compressed message; the existing NONE callback path continues to receive the original DATA frames.

**Tech Stack:** C11, `tr_message`/`tr_message_transform`, Runtime MessageService, Reactor owner callbacks, connection-group Pipeline ingress, Xmake, connection-group facade/TLS tests.

**Spec:** `docs/superpowers/specs/2026-10-24-message-layer-compression-design.md`

## Global Constraints

- `tr_client_connection_group_send()` remains an explicit NONE send and emits no capability frame unless compression is configured.
- Compression is selected per logical DATA message. Unknown/disallowed algorithms fail before accepting caller bytes; there is no implicit compression or fallback.
- The first capability frame is client initiated on each DATA route; the server never sends a capability frame before receiving that request.
- No capability is reused across DATA connection, TLS, route, or member-generation replacement.
- The transform receives only the complete logical DATA body. TRR1 route bytes, TRP headers, TLS identity, and Pipeline control messages stay outside it.
- Receive encoded bytes, decoded Message, transform workspace, active tasks, and TX outputs remain within explicit finite bounds derived from the existing connection/transfer/message limits.
- A compressed message is delivered to the server callback exactly once and only after complete validation and decode. NONE keeps its current per-frame buffer ownership and callback behavior.
- A task completion may affect a group or invoke an application callback only if its exact DATA connection and route/member generation remain active.
- Xmake commands use `TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" PYTHONDONTWRITEBYTECODE=1`.

## File Map

- `include/tr/transport.h`: add connection-group compression enum/mask, directional configuration fields, and explicit send-selection API.
- `src/client.c`, `src/server.c`: enable the existing Runtime MessageService and finite Runtime budgets only when group compression is configured; bind the service into the owning ClientGroup/PipelineListener.
- `src/transport/group/client_group.[ch]`: DATA capability generation state, explicit compressed-send admission, asynchronous encode, bounded TX ownership, and stale completion retirement.
- `src/transport/group/pipeline_ingress.[ch]`, `src/transport/group/pipeline_listener.[ch]`: per-route capability/RX state and cleanup on exact connection-generation retirement.
- `src/server.c`: validate and asynchronously decode complete compressed group messages, adapt the owned decoded Message to the existing callback/release contract.
- `tests/test_connection_group_facade.c`, `tests/test_pipeline_listener.c`, `tests/test_pipeline_listener_tls.c`, `tests/test_connection_group_tls.c`: capability, API, ownership, route-generation, NONE, TLS, and lifecycle scenarios.
- `xmake.lua`: add only any new production unit required by focused group compression state/helpers.
- `docs/superpowers/specs/2026-10-24-message-layer-compression-design.md`: record verified group behavior and remaining final security/performance acceptance.

---

### Task 1: Add the opt-in public group compression policy

**Files:** `include/tr/transport.h`, `tests/test_connection_group_facade.c`, `src/client.c`, `src/server.c`.

**Interfaces:**

- Add `enum tr_connection_group_compression { TR_CONNECTION_GROUP_COMPRESSION_NONE = 0, TR_CONNECTION_GROUP_COMPRESSION_ZSTD = 1 }` and `TR_CONNECTION_GROUP_COMPRESSION_MASK_ZSTD`.
- Append `send_compression_algorithms` and `max_encoded_message_bytes` to `tr_connection_group_client_config`; append `receive_compression_algorithms` and `max_encoded_message_bytes` to `tr_connection_group_server_config`.
- Add `tr_client_connection_group_send_with_compression(client, stream_id, message_id, bytes, compression)`. The existing send function delegates to NONE.
- A zero algorithm mask keeps the existing behavior and creates no MessageService workers for group-only facades. ZSTD requires a nonzero encoded limit of at least the 16-byte envelope plus one encoded byte; decoded size is bounded by `tr_facade_limits.max_message_bytes`.

- [x] **Step 1: Add failing facade policy/API tests**

Assert zero-initialized group configs remain valid; unknown mask bits, inconsistent limits, ZSTD with a zero encoded limit, and an unknown per-message compression enum are rejected without taking caller bytes. Assert the old send API still selects NONE.

- [x] **Step 2: Run focused tests and confirm RED**

Run `xmake run -P . test_connection_group_facade` with the local TMPDIR variables. Expected: compilation or policy assertions fail because the new enum, fields, and function are absent.

- [x] **Step 3: Implement public configuration validation and NONE wrapper**

Normalize and validate the new directional fields. Keep existing send behavior byte-compatible by forwarding the old API to `send_with_compression(..., NONE)`.

- [x] **Step 4: Run focused facade tests**

Run `test_connection_group_facade` and `test_connection_group_tls`; verify existing raw sends and all current configuration defaults still pass.

### Task 2: Negotiate receive capability per DATA route generation

**Files:** `src/transport/group/client_group.c`, `src/transport/group/client_group_internal.h`, `src/transport/group/pipeline_ingress.c`, `src/transport/group/pipeline_ingress_internal.h`, `src/transport/group/pipeline_listener.c`, `src/transport/group/pipeline_listener_internal.h`, `tests/test_pipeline_listener.c`, `tests/test_connection_group_facade.c`.

**Interfaces:**

- Client DATA state records whether its client-initiated capability request was sent, whether the matching server response is ready, the peer receive algorithm mask/limit, and the exact connection/route generation.
- In compression-enabled mode, the client sends one valid 12-byte `TR_FRAME_MESSAGE_CAPS` with zero receive mask/limit after DATA route attachment. The server replies once with its configured local receive mask/encoded limit, including an empty response when compression is disabled.
- The server sends no capability frame before the client request. An old server may ignore the request; in that case compressed send returns `TR_AGAIN`, while the existing NONE API keeps working.
- The pipeline ingress wrapper consumes capability frames internally and never exposes them to application callbacks. Duplicate, malformed, wrong-identity, or stale-generation capability frames terminate only the exact DATA connection.

- [x] **Step 1: Write failing negotiation and old-peer tests**

Exercise client request/server response on a real DATA route; assert asymmetric limits are stored on both sides, duplicate/malformed capability frames close only the exact route, route replacement clears readiness, and an old-peer simulation that drops the new frame still supports NONE.

- [x] **Step 2: Run focused tests and observe RED**

Run `test_pipeline_listener`, `test_connection_group_facade`, and `test_pipeline_listener_tls`. Expected: capability frames currently reach the generic callback or the client DATA handler aborts them as unsupported.

- [x] **Step 3: Implement role-ordered capability exchange**

Send the client request only when the client compression mask is enabled. Validate header identity and payload with `tr_message_caps_decode`; respond from the server using its own configured receive limits. Bind state to the current `tr_conn_handle` and route/member generation. Clear it in the same retirement transition that detaches that route.

- [x] **Step 4: Verify negotiation and compatibility**

Run the three focused targets. Confirm no unsolicited server capability frame, no capability frame in the NONE-only path, and no permission carried across route replacement.

### Task 3: Wire bounded MessageService and group memory budgets

**Files:** `src/client.c`, `src/server.c`, `src/transport/group/client_group_internal.h`, `src/transport/group/pipeline_listener_internal.h`, `src/runtime/runtime_internal.h`, tests.

**Interfaces:**

- Pass `tr_runtime_shard_message_submit_adapter`, `tr_runtime_shard_message_cancel_adapter`, and the owning shard context to group internals; do not expose a MessageService handle publicly.
- Enable a Runtime MessageService only when the relevant group compression mask is nonzero. Use a finite task capacity bounded by the configured active-transfer/stream limits and existing executor tuning.
- Derive a checked finite message budget from max logical message size, encoded limit, configured simultaneous transfers, and bounded per-task workspace. Reject overflow, zero, or insufficient budget during construction before publishing Client/Server ownership.
- Group-only facades without compression retain their current no-worker construction path.

- [x] **Step 1: Add construction/budget tests**

Verify compression-enabled group-only Client and Server construct a MessageService with finite budget, invalid/overflowing budgets fail transactionally, and compression-disabled group facades create no MessageService work capacity or additional worker thread.

- [x] **Step 2: Run focused lifecycle tests and observe RED**

Run `test_connection_group_facade`, `test_connection_group_tls`, `test_server_create_oom`, and `test_runtime_message_api`. Expected: group compression currently has no service route or hard budget.

- [x] **Step 3: Bind the existing Runtime service and budget**

Configure the owning Runtime shard before start. Keep the shard budget alive through all accepted tasks and callbacks; ensure Client/Server rollback and destroy retry paths retain ownership if the MessageService cannot drain.

- [x] **Step 4: Verify construction, rollback, and no-compression defaults**

Run the focused targets and assert all budgets/task counts return to baseline after destroy and injected construction failures.

### Task 4: Implement explicit compressed client DATA send

**Files:** `src/transport/group/client_group.c`, `src/transport/group/client_group_internal.h`, `src/client.c`, `tests/test_connection_group_facade.c`, `tests/test_connection_group_tls.c`.

**Interfaces:**

- `send_with_compression(..., ZSTD)` checks local send policy, current route, current peer receive capability, decoded limit, encoded limit, task capacity, and finite budget before accepting caller bytes.
- If capability is still pending, return `TR_AGAIN` and retain caller bytes. If the negotiated response has no ZSTD bit or local policy denies it, return `TR_ERR_UNSUPPORTED` and retain caller bytes.
- On accepted task, copy the logical body into a budgeted `tr_message`; asynchronous Zstandard output is prefixed with the existing 16-byte envelope and sent as ordinary bounded DATA fragments with COMPRESSED/FIRST/LAST set consistently.
- TX completion owns the encoded Message until all framed output is accepted by Reactor. Transform failure/close/route retirement returns budget once and reports the existing DATA error event without sending a partial compressed message.

- [x] **Step 1: Add failing client compressed-send tests**

Test no capability/local policy rejection retains bytes; task queue or Reactor pressure returns `TR_AGAIN` with caller bytes retained; accepted send consumes only after task acceptance; real encoded payload arrives in multiple DATA frames and remains independent from Pipeline route bytes and TLS identity.

- [x] **Step 2: Run focused tests and observe RED**

Run `test_connection_group_facade` and `test_connection_group_tls`. Expected: the selection API is absent and current client DATA handling rejects capability/encoded sends.

- [x] **Step 3: Add bounded per-message TX state**

Reserve the TX node, source Message, output allowance, task, and send-byte accounting before consuming the caller input. Keep the existing Reactor frame-size limit and avoid an unbounded group-level queue. Reject or terminate a message that cannot fit the negotiated encoded limit.

- [x] **Step 4: Verify client TX, ownership, and NONE compatibility**

Run the facade and TLS targets with small/one-frame and bounded multi-frame Zstandard output, queue pressure, and application reuse of caller memory after `TR_OK`. The NONE path retains existing payload address/copy behavior. The facade confirms a second compression request on a busy transfer returns `TR_AGAIN` without changing the caller buffer; it fills the Reactor DATA TX item pool until further admission returns `TR_AGAIN`, then verifies the accepted asynchronous compressed send fails the exact route atomically with `TR_ERR_STATE`, delivers no partial message, and returns `send_bytes_inflight` to zero. It also asserts compressed output exceeds the 64-byte frame size and is submitted through the multi-frame path. Facade and real mTLS targets pass in release; the facade saturation/ownership scenario passes under ASan/UBSan with leak detection. The complete release suite passes 88/88 after these cases were added.

### Task 5: Reassemble and decode compressed server DATA

**Files:** `src/transport/group/pipeline_ingress.c`, `src/transport/group/pipeline_ingress_internal.h`, `src/transport/group/pipeline_listener.c`, `src/server.c`, `tests/test_pipeline_listener.c`, `tests/test_pipeline_listener_tls.c`.

**Interfaces:**

- Maintain bounded receive state by `(connection generation, route/member generation, stream_id, message_id)`; FIRST must reserve the full encoded limit before copying any payload.
- Each compressed DATA fragment must match message identity/flags and contributes to the exact declared encoded body length. The first payload must contain the complete 16-byte envelope. Unknown codec, missing/duplicate FIRST, early/duplicate LAST, size overflow, trailing/truncated Zstandard, or unsupported route capability closes the exact DATA connection without invoking `on_message`.
- After LAST, submit one complete encoded Message to the owning MessageService. On successful decode, adapt the owned decoded Message into the existing `tr_connection_group_message` ownership/release contract and invoke the application callback once with FIRST|LAST flags.
- NONE frames continue through the existing callback synchronously with the original RX buffer; no transform wrapper or copy is added.

- [x] **Step 1: Add failing real-listener receive tests**

Send compressed DATA over a routed connection and assert exactly one decoded callback; cover multi-frame assembly, interleaved streams/messages, malformed envelope, incorrect encoded/decoded size, trailing/truncated Zstandard, no capability, and application TAKE_OWNERSHIP/release.

- [x] **Step 2: Run focused listener tests and observe RED**

Run `test_pipeline_listener` and `test_pipeline_listener_tls`. Expected: current server callback either sees raw frame bytes or ignores/aborts the new compressed flags.

- [x] **Step 3: Implement budgeted per-message RX and MessageService decode**

Reserve builder and per-message state before accepting FIRST payload; copy each fragment before releasing its frame buffer. Track each active message against route transfer limits. Submit decode only after exact encoded length is complete. Wrap decoded Message storage with a release handle that remains valid if the application takes ownership.

- [x] **Step 4: Verify delivery, application ownership, and NONE path identity**

Run the focused tests and assert callback cardinality, decoded bytes, release exactly once, and unchanged NONE buffer address/ownership behavior. The valid multi-frame callback, decoded bytes, TAKE_OWNERSHIP destroy barrier, and NONE behavior pass. Listener rejection tests cover missing CAPS, unknown codec, encoded-length truncation/trailing bytes, decoded-size limit, and real Zstandard frames with trailing or truncated payloads. Two streams with partial compressed messages are interleaved before one completes. A held server decode task is cancelled on exact route close; its late success after connection-slot reuse cannot reach the replacement route.

### Task 6: Close cancellation, route replacement, TLS, and full-suite gaps

**Files:** group implementation/tests and `docs/superpowers/specs/2026-10-24-message-layer-compression-design.md`.

- [x] **Step 1: Verify DATA route generation retirement**

Server-side cancellation on exact DATA connection close is covered with a held decode task; its late successful completion after the connection slot is reused cannot affect the replacement route, and the message budget returns to baseline. The facade test now holds a client Zstandard worker after encoding, closes its route, reconnects the same group and stream while the old completion is still held, then releases it. The old completion delivers no message, the replacement transfer remains active, and a new compressed message is delivered. `test_connection_group_facade` passed in release and ASan/UBSan with leak detection.

- [x] **Step 2: Verify TLS identity separation**

The real mTLS fixture checks the peer certificate identity on CONTROL and the matching control identity on DATA, rejects use of the old group route after graceful profile replacement, and accepts a compressed message after reconnect with the rotated client/server certificates. It now also revokes the active TLS generation and verifies that the group is retired and another compressed send returns `TR_ERR_CLOSED`. The facade and TLS targets pass in release; both pass under ASan/UBSan with leak detection. The test identity rotation preserves the client URI SAN, matching the public identity view used by authorization.

- [x] **Step 3: Run sanitizers and full tests**

Run the group facade/listener targets under ASan/UBSan; `test_runtime_threads` under TSan; then `xmake test -P . -j1`. Use only worktree-local Xmake temp paths. Assert all active compression tasks, builders, output Messages, and callbacks return to budget baseline after shutdown.

- [x] **Step 4: Update evidence and remaining acceptance**

Record the group capability/API/data-path results in the parent spec. Keep TLS ON/OFF performance, P50/P99, CPU, allocation/RSS, rejection/completion rates, and the full compression side-channel audit open until separately measured and reviewed.

## Scope Boundary

This plan covers client-to-server connection-group DATA compression because the current public API supports only that direction. It does not claim server-to-client group DATA, full Message compression release acceptance, or V2 completion until the parent spec's TLS/security/performance matrix is also verified.
