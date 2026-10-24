# Message Compression Channel Data Plane Plan

> **For agentic workers:** follow the repository TDD workflow. Implement one task at a time, observe the expected RED, then verify GREEN. Keep all temporary/build outputs under this worktree's `build/` directory.

**Goal:** Complete the Channel/Stream Zstandard data path without advertising receive support before it works, while preserving the `none` fast path and bounded progress when encoded bytes `C` exceed network window `W`.

**Architecture:** `src/message/` remains the only transform implementation. Channel owns compressed framing, per-stream bounded encoded staging, network credit, message ordering, and stale-generation rejection. Runtime's existing per-shard MessageService performs CPU work and returns results to the Reactor owner. RPC and connection-group adapters remain separate follow-up plans.

**Spec:** `docs/superpowers/specs/2026-10-24-message-layer-compression-design.md`; protocol rationale: `docs/architecture/decisions/ADR-008-message-compression-protocol.md`.

## Constraints

- Do not remove the current `TR_ERR_UNSUPPORTED` guard for nonzero local receive algorithms until inbound compressed DATA is implemented and tested.
- Add `TR_FRAME_F_COMPRESSED` only for DATA. Preserve the fixed TRP header and its reserved field.
- The first compressed DATA frame carries the 16-byte versioned envelope defined in the spec; its bytes count against encoded limit and stream network credit.
- Bound encoded staging by the advertised per-message encoded limit and the owning memory budget. Bound decoded output and Zstd workspace independently.
- Return receive credit only after each encoded DATA payload has been copied into storage already reserved for that message.
- Keep at most one not-yet-fully-enqueued compressed message per Stream. Do not add an unbounded producer queue.
- The TX scheduler must make bounded per-turn progress and rotate among active Streams. Control traffic keeps existing Reactor priority.
- A transform result may be sent or delivered only if its exact Stream generation and connection generation are still current.
- Do not modify the two user-owned plans in the original worktree or change existing PR draft states.
- Test command: `TMPDIR="$PWD/build/tmp" XMAKE_TMPDIR="$PWD/build/xmake-tmp" PYTHONDONTWRITEBYTECODE=1 xmake run -P . test_transport`.

## Task 1: Define compressed DATA framing primitives

**Files:** `src/transport/protocol/wire.h`, `src/transport/protocol/wire.c`, new `message_envelope.h/.c`, `src/execution/reactor.c`, `tests/test_transport.c`, `xmake.lua`.

- [x] Add a failing test proving `TR_FRAME_F_COMPRESSED` is accepted on DATA and rejected on all control-frame types by the wire validator and on a control send by the Reactor API.
- [x] Add failing envelope round-trip and malformed-input tests for the 16-byte schema: version, Zstandard codec ID, encoded body size `C`, decoded size `U`, and reserved zero.
- [x] Run `test_transport`; the flag test failed at DATA validation and the envelope test first failed to compile because its interface was absent.
- [x] Implement frame-type-specific flag validation and fixed-offset little-endian envelope encode/decode. Reject unknown codecs, nonzero reserved fields, zero encoded body, truncation, and arithmetic overflow.
- [x] Run `test_transport`, then changed-range clang-format 18 and `git diff --check`.

## Task 2: Bind Channel to its owning MessageService

**Files:** `src/transport/channel/channel_internal.h`, `src/transport/channel/channel.c`, `src/client.c`, `src/server.c`, and focused tests.

- [x] Add a failing lifecycle test for non-owner calls, mismatched Reactor, successful owner bind, and duplicate bind rejection.
- [x] Add an internal owner-only Channel binding to the Runtime shard's existing MessageService submit route. Do not expose MessageService handles through the stable public SDK.
- [x] Enforce Reactor identity and require binding before HELLO; wire the V2 Client/Server deferred Channel creation paths.
- [x] Document the borrowed Runtime context lifetime; Client/Server own the Runtime longer than their Channels.
- [x] Track accepted transform jobs through cancellation and completion so Channel teardown and stopped-owner reaping cannot touch retired Channel state. This is required with Task 3 before compressed DATA can be enabled.
- [x] Run transport and V2 facade integration targets: `test_transport`, `test_rpc_record_endpoint_tcp`, `test_rpc_v2_sdk`, and `test_rpc_v2_server`.

## Task 3: Receive compressed DATA with bounded encoded staging

**Files:** `src/message/builder.h`, `src/message/builder.c`, `src/transport/channel/channel.c`, `src/transport/channel/channel.h`, `tests/test_message.c`, `tests/test_transport.c`, `xmake.lua`.

- [x] Add a budgeted fixed-capacity Message builder that copies incremental input, rejects overflow, publishes only when full, and returns both payload and control-block budget on release or cancel. Verify partial publish, append overflow, budget rejection, and cancellation.

- [x] Add tests for valid single-frame and multi-frame compressed DATA, malformed envelopes, undeclared codecs, `C`/`U` overflow, trailing/truncated Zstandard input, and copied-frame credit return.
- [x] Reserve exact encoded storage from the Channel/Runtime budget on FIRST before accepting payload bytes. Keep one receive builder per active Stream and validate message ID, flags, declared `C`, declared `U`, lane, capability, and local limits on every frame.
- [x] Copy each frame into the reserved encoded builder, then consume exactly that frame's network payload credit, including the FIRST envelope. Do not return credit before the copy succeeds.
- [x] On LAST, publish one immutable Message and submit bounded decode work to MessageService. Convert successful decoded output to the existing Stream buffer delivery contract with explicit accounting; preserve `none` zero-copy paths.
- [x] On RESET, lane loss/replacement, malformed DATA, exhausted budget, cancellation, or owner stop, release builder/work/output exactly once. A stale completion must not deliver or send.
- [x] Verify `C < W`, `C = W`, and `C > W`, multiple-stream fairness, constrained builder budget, cancellation, and connection replacement.

## Task 4: Send compressed DATA with bounded fair credit progression

**Files:** `src/transport/channel/channel.c`, `src/transport/channel/channel.h`, tests.

- [x] Add tests for explicit per-message Zstandard send, peer capability rejection, encoded-size rejection, ownership on synchronous rejection, asynchronous failure, and `C > W` completion.
- [x] Add a bounded per-Stream TX state with one in-flight compression and one message owner. Accept the caller's message only after task/output/control-state reservations succeed.
- [x] Prefix encoded output with the envelope. Split into DATA frames that fit current stream credit and max frame size. Keep `FIRST`, `LAST`, and `COMPRESSED` consistent across the full logical message.
- [x] Resume on WINDOW_UPDATE and rotate active Streams with a fixed work budget so a large compressed message cannot monopolize the owner. Preserve ordering against later messages on the same Stream.
- [x] Cancel or discard pending output on RESET, close, connection-generation change, service stop, or task failure; never send stale output.
- [x] Verify fairness with one `C > W` Stream and concurrent small Streams, and verify exact payload ownership on every return path.

## Completion boundary

This plan closes only generic Channel/Stream compressed DATA and `C > W` progression. It does not complete RPC body policy, connection-group adaptation, TLS/security acceptance, or performance acceptance. Keep local receive advertisement disabled until Task 3 passes; keep production send selection disabled until Task 4 passes.
