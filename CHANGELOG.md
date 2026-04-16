# Changelog – cachly SDK (kotlin)

**Language:** Kotlin Multiplatform  
**Package:** `dev.cachly:sdk-kotlin`

> Full cross-SDK release notes: [../CHANGELOG.md](../CHANGELOG.md)

---

## [0.2.0] – 2026-04-07

### Added

- **`mset(items: List<MSetItem>)`** – bulk set with per-key TTL via Redis pipeline
- **`mget(keys: List<String>)`** – bulk get in one round-trip; returns `List<Any?>` (null on miss)
- **`lock(key: String, options: LockOptions)`** – distributed lock (SET NX PX + Lua release)
  - Returns `LockHandle?`; `null` when retries exhausted
  - `LockHandle.release()` for early, token-fenced unlock
  - Auto-expires after TTL to prevent deadlocks
- **`streamSet(key: String, chunks: Flow<String>, options: StreamSetOptions)`** – cache token stream via RPUSH (expect/actual per platform)
- **`streamGet(key: String)`** – returns `Flow<String>?`; null on miss

### Fixed

- `Known limitations` section updated – bulk ops now implemented

---

## [0.1.0-beta.1] – 2026-04-07

Initial beta release.

### Added

- `set(key, value, ttl?)` – store a value with optional TTL
- `get(key)` – retrieve a value by key
- `delete(key)` – remove a key
- `clear(namespace?)` – flush namespace or entire cache
- **Semantic cache:** `semantic.set(...)`, `semantic.get(...)`, `semantic.clear()`
- Namespace support via `withNamespace(ns)`
- API-key-based authentication
- TLS by default, EU data residency (German servers)

### Known limitations

- ~~Bulk operations (`mset` / `mget`) not yet implemented~~ ✅ resolved in v0.2.0
- Pub/Sub not yet supported

---

## [Unreleased]

See [../CHANGELOG.md](../CHANGELOG.md) for upcoming features.

