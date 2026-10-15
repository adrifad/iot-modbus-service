# Industrial Modbus Reliability Hardening Plan

> **For agentic workers:** Execute inline in the current session; the user explicitly requested implementation without a plan handoff.

**Goal:** Correct measurement decoding and harden gateway lifecycle, transport coordination, calibration, persistence, realtime telemetry, and deployment without enabling device writes.

**Architecture:** Preserve FastAPI, asyncio TCP sessions, the existing Modbus codec, SQLite schema, and React console. Add explicit register metadata and migrations, isolate protocol and processing failures, make gateway reconnection safe under the available identity constraints, and validate behavior using deterministic local simulators.

**Tech Stack:** Python 3.11+, FastAPI, asyncio, SQLite, React, TypeScript, Vite, Tailwind CSS, unittest.

---

### Task 1: Register metadata and measurement pipeline

**Files:** `backend/profiles.py`, `backend/store.py`, `backend/scheduler.py`, `backend/app.py`, `tests/test_scheduler.py`

- [x] Define explicit analog measurement metadata for each ME31 AI register and block.
- [x] Persist the metadata through a backward-compatible schema migration.
- [x] Split successful Modbus decode/storage from measurement conversion and calibration.
- [x] Record conversion failures separately from invalid frames; ensure configuration and custom reads persist successfully without creating channel samples.
- [x] Test AI1–AI6, AI block, configuration categories, device information, and custom registers.

### Task 2: Gateway lifecycle and Modbus stream recovery

**Files:** `backend/gateway.py`, `backend/app.py`, `backend/store.py`, `tests/test_modbus.py`, `tests/test_scheduler.py`, `docs/hardware-verification.md`

- [x] Add explicit session lifecycle states, activity/timeout handling, stale-session cleanup, duplicate-session policy, and reconnect metrics.
- [x] Preserve manual commissioning; support G781 LINK registration packets in transparent serial mode with token and TLS/trusted VPN checks, and prohibit source-address identity.
- [x] Reinitialize parser state per connection and recover or close safely on malformed frames.
- [x] Correlate responses under one gateway transaction lock, quarantine delayed RTU replies after timeouts, and test reconnect/fragmentation/concatenation/invalid responses.

### Task 3: Float32 configuration and finite-value integrity

**Files:** `backend/profiles.py`, `backend/modbus.py`, `backend/app.py`, `web/src/App.tsx`, `tests/test_modbus.py`, `tests/test_api.py`

- [x] Support explicit byte and word-order variants while leaving unverified float profiles disabled.
- [x] Validate channel association and float block quantity; reject non-finite decoded values as measurements.
- [x] Add UI controls and a known-reference verification workflow; require verified settings before enabling float polling.
- [x] Test endian variants, six-channel decoding, NaN/infinity, and disabled/unverified polling.

### Task 4: Independent health, scheduler lifecycle, and batching

**Files:** `backend/app.py`, `backend/scheduler.py`, `backend/gateway.py`, `web/src/App.tsx`, `tests/test_scheduler.py`

- [x] Report gateway, Modbus device, polling item, and measurement quality independently with per-item freshness thresholds.
- [x] Make task cancellation and replacement safe for enable/disable/delete/interval edits and shutdown.
- [x] Batch only adjacent compatible AI integer registers when all involved logical items are due; retain each item's enable state and sample only enabled channels.
- [x] Test response quality and compatible batching behavior. Multi-gateway/multi-slave and cancellation timing still lack dedicated integration fixtures.

### Task 5: Calibration, database concurrency, and telemetry stream

**Files:** `backend/calibration.py`, `backend/store.py`, `backend/scheduler.py`, `backend/app.py`, `web/src/App.tsx`, `web/src/api.ts`, tests

- [x] Preserve raw samples, validate finite calibration/scaling, and test revision/revert persistence.
- [x] Serialize SQLite operations safely across asyncio tasks and use atomic measurement writes.
- [x] Connect the live page to the authenticated WebSocket with reconnect/backoff, cleanup, and HTTP fallback.
- [x] Test WebSocket authentication; browser disconnect/reconnect presentation is build-checked but not browser-driven.

### Task 6: Deployment hardening and final verification

**Files:** `deploy/iot-modbus.service`, `.env.example`, `backend/store.py`, `README.md`, `docs/hardware-verification.md`, automated tests

- [x] Use `/var/lib/iot-modbus-service/iot-modbus.db` in deployment examples and verify state-path restart persistence in a temporary directory. systemd itself was not run on Ubuntu in this environment.
- [x] Run the full backend suite and frontend TypeScript/build checks; fix regressions.
- [x] Document the G781 identity/reconnect boundary, SQLite scale limits, service commands, and actual hardware checks still required.
- [x] Leave analog output writes disabled and do not deploy or push to `main`.
