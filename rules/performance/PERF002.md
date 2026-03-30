# Rule PERF002
Avoid multiple `getOrCreate()` calls — use `getActiveSession()` instead

## Information
`SparkSession.builder.getOrCreate()` is expensive: it checks whether a session already exists and, if not, initializes a new one. Calling it multiple times throughout a codebase causes repeated overhead and makes session lifecycle management unclear.

- Each `getOrCreate()` call acquires a lock and inspects the session registry
- Scattering `getOrCreate()` across modules makes it impossible to know which call actually created the session
- `SparkSession.getActiveSession()` returns the already-running session without any initialization cost and returns `None` if no session exists, making the dependency explicit

This rule fires when more than one `getOrCreate()` call is found in the same file.

## Best practices
- Call `getOrCreate()` exactly once at the entry point of your application
- Everywhere else, retrieve the session with `SparkSession.getActiveSession()` or pass it as a parameter

### Example

Bad:
```python
# module_a.py
spark = SparkSession.builder.getOrCreate()

# module_b.py
spark = SparkSession.builder.getOrCreate()  # unnecessary second call
```

Good:
```python
# entry_point.py
spark = SparkSession.builder.getOrCreate()

# module_b.py
spark = SparkSession.getActiveSession()
```
