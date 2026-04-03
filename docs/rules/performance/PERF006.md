# Rule PERF006
Avoid bare `.checkpoint()` / `.localCheckpoint()` — always pass an explicit `eager` argument

## Severity

🟡 **MEDIUM** — Moderate performance impact.

## Information
Both checkpoint methods accept an `eager` boolean that controls when the
checkpoint is actually materialised:

- **`eager=True`** — the checkpoint is computed and written immediately, blocking
  the current action until it completes. The next action reads from the checkpoint.
- **`eager=False`** — the checkpoint is scheduled lazily and materialised on the
  first action that triggers it. The checkpoint overhead is deferred but may
  cause that action to be slower than expected.

Calling `.checkpoint()` or `.localCheckpoint()` without `eager` leaves the
behaviour implicit:

- The next developer has no idea whether this call will block or not
- Subtle performance differences between eager and lazy checkpointing become invisible
- Any future change to the Spark default would silently alter pipeline behaviour

Always pass `eager=True` or `eager=False` explicitly so the intent is a
visible contract in the code.

## Best practices
Pass the `eager` argument explicitly, even when it matches the default:

```python
df.checkpoint(eager=True)    # blocks immediately — predictable latency
df.checkpoint(eager=False)   # deferred — first downstream action pays the cost

df.localCheckpoint(eager=True)
df.localCheckpoint(eager=False)
```

**Rule of thumb:** If it is worth checkpointing, it is worth being explicit about when.

### Example

Bad:
```python
df = df.checkpoint()
df = df.localCheckpoint()
```

Good:
```python
df = df.checkpoint(eager=True)
df = df.localCheckpoint(eager=False)
```
