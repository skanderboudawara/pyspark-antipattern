# Versioning

`pyspark-antipattern` uses a three-part version number: `MAJOR.MINOR.PATCH`.

The meaning of each segment is intentionally different from generic SemVer — it
is tuned to the realities of a linter, where the most disruptive change is not
a new feature but a shift in what the tool considers correct.

---

## PATCH — fixes and new rules

Bumped when:

- A bug in an existing rule is fixed (false positive removed, false negative closed)
- A new rule is added
- Documentation is updated
- Internal refactors with no user-visible effect

```
0.4.2 → 0.4.3
```

A PATCH upgrade is always safe to apply without reviewing your CI results first.

---

## MINOR — breaking changes

Bumped when:

- A rule is removed or renamed
- A configuration key is renamed or removed (e.g. `ignore_rules` → `ignore`)
- A CLI flag is renamed or removed
- The `noqa` comment syntax changes
- The exit-code contract changes

```
0.4.3 → 0.5.0
```

A MINOR upgrade requires reviewing the changelog before upgrading — your
`pyproject.toml` or CI scripts may need updating.

---

## MAJOR — code behaviour change

Bumped when:

- The detection logic of one or more rules changes in a way that affects what
  is flagged across the board (e.g. a new AST pass, a change to how chains are
  resolved, a new cross-file analysis stage)
- The output format changes in a way that breaks downstream parsers
- A fundamental architectural shift that makes the tool behave differently on
  existing codebases

```
0.5.0 → 1.0.0
```

A MAJOR upgrade should be treated like a migration: run the new version against
your full codebase, review all new violations, and decide which ones to address
or suppress before locking the version in CI.

---

## Summary

| Segment | Trigger | Safe to auto-upgrade? |
|---|---|---|
| `PATCH` | Bug fix, new rule, docs | Yes |
| `MINOR` | Breaking config / CLI change | Review changelog first |
| `MAJOR` | Detection behaviour change | Full codebase audit recommended |
