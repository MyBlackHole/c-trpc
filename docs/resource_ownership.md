# Resource Ownership

c-trpc uses ISO C11 as its language baseline. GCC/Clang's `cleanup`
attribute is the only compiler extension required for scope-owned resources.

The resource model is based on explicit ownership rather than implicit lifetime.

## Ownership states

Every resource must be in one of these states:

- **owned**: exactly one owner is responsible for release.
- **borrowed**: temporary access; no release responsibility is transferred.
- **shared reference**: an explicit reference keeps an asynchronous object alive.
- **transferred**: ownership moves from one owner to another at a documented API
  boundary.

Handles such as `tr_conn_handle` are capabilities, not ownership.

## Success-only transfer

Unless an API explicitly documents otherwise, ownership transfers only when the
operation returns `TR_OK`.

Example:

```c
struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;

ret = tr_buffer_acquire(pool, size, &buffer);
if (ret != TR_OK)
    return ret;

ret = tr_reactor_send(connection, type, flags, stream_id, message_id, buffer);
if (ret != TR_OK)
    return ret;

(void)tr_buffer_take(&buffer);
return TR_OK;
```

If send fails, scope cleanup releases the buffer. If send succeeds,
`tr_buffer_take()` clears the cleanup-managed variable because ownership now
belongs to Reactor.

## Scope cleanup

Resources with lexical ownership should use `TR_AUTO(cleanup_fn)` by default.

Current typed ownership helpers include:

- `tr_fd_cleanup()` / `tr_fd_take()`
- `tr_buffer_cleanup()` / `tr_buffer_take()`

New resource types should provide typed cleanup/take helpers rather than generic
casts.

Cleanup functions must:

1. tolerate the disarmed/empty state;
2. release exactly one owned resource;
3. leave the variable in its empty state when practical;
4. never acquire ownership of another resource.

## Cleanup order

Cleanup variables are destroyed in reverse declaration order.

Declare dependencies first and dependents later:

```c
struct parent *parent TR_AUTO(parent_cleanup) = NULL;
struct child *child TR_AUTO(child_cleanup) = NULL;
```

The child is released before the parent.

Do not reorder cleanup-managed declarations without checking dependency order.

## Construction and transaction guards

Automatic cleanup has two forms in c-trpc.

### Simple lexical owner

Use a typed cleanup directly for one independent resource:

```c
int fd TR_AUTO(tr_fd_cleanup) = -1;
struct tr_buffer *buffer TR_AUTO(tr_buffer_cleanup) = NULL;
```

### Complex build guard

Objects whose destructor is only valid after several subresources are
initialized use a small build guard. The guard records which initialization
steps succeeded and unwinds only those resources.

This is the required pattern for constructors such as Reactor, Channel and RPC
Endpoint. It prevents two common bugs:

- calling a full destructor on a partially initialized object;
- forgetting to extend every `goto fail_*` chain when a new resource is added.

A build guard is disarmed only after the fully initialized object is handed to
its owner.

### Transaction rollback guard

Operations that mutate an existing owner across several steps use an armed
rollback guard. Examples are Client connect and Server peer adoption.

```text
begin transaction
    |
acquire/transfer resources
    |
failure ---------> scope cleanup rolls back
    |
success
    |
disarm guard
```

This makes a newly added early return rollback-safe by default.

## Explicit ownership transfer

Ownership transfer must be visible in code through a typed `*_take()` helper
or an API whose documented success contract performs the transfer.

Do not use a bare assignment followed by an unexplained `ptr = NULL` as the
normal ownership-transfer idiom.

## Async lifetime

Scope cleanup does not solve asynchronous lifetime.

Before a pointer crosses a thread/queue/callback boundary, its lifetime must be
protected by one of:

- a single-owner guarantee;
- explicit quiescence;
- an explicit shared reference.

Never publish a pointer to another thread and add its reference afterward.

## Reactor ownership

Mutable Reactor/Connection transport state is owned by the Reactor thread.
Other threads submit commands rather than taking ownership of that mutable
state.

Synchronization should protect genuinely shared state; it must not compensate
for unclear ownership.

## Error paths

Automatic cleanup is preferred for local resources because new early returns
cannot silently skip release.

Traditional downward `goto` unwind remains valid for objects whose partial
construction cannot be expressed as independent scope-owned resources. Do not
mix two competing cleanup mechanisms for the same resource.

## Compiler policy

The project is compiled as:

```
-std=c11 -Wall -Wextra -Werror -pedantic
```

The language remains C11. `TR_AUTO()` intentionally wraps the GCC/Clang
`cleanup` attribute; broader GNU language constructs are not implied or
enabled.
