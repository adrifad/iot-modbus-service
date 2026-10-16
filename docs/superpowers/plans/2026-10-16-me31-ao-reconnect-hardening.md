# ME31 AO Polling and Gateway Recovery Hardening Plan

> **For agentic workers:** This plan is being executed inline because the task explicitly requests implementation and validation now.

**Goal:** Provision persistent read-only AO polling for every active ME31 output node, add an allowlisted and confirmed per-channel range configuration operation, and correct trusted gateway recovery and polling fairness without changing hardware framing.

**Architecture:** Reuse SQLite polling rows, `PollScheduler`, `OutputDriver`, the existing `GatewaySession` lock, WireGuard ingress, and current React pages. Add an idempotent backfill migration, keep range writes in the device-specific output driver, and reject any attempt to infer gateway identity from public socket metadata.

**Tech Stack:** Python 3.11, FastAPI, asyncio, SQLite, Modbus RTU-over-TCP, React, TypeScript, TanStack Query.

---

### Task 1: AO polling provisioning and fair scheduling

**Files:** `backend/app.py`, `backend/store.py`, `backend/scheduler.py`, `tests/test_api.py`, `tests/test_store.py`, `tests/test_scheduler.py`.

- Add the two profile-derived default AO polling items in the same transaction as AO node/channel creation.
- Add an idempotent migration backfill for active AO nodes. Insert only absent profile/name pairs; preserve item settings, values, disabled state, and node-level polling state; exclude archived nodes.
- Make due AO work oldest-due-first with bounded per-gateway dispatch and preserve skipped-cycle behavior when gateway control traffic is busy.
- Add migration, creation, restart, disabled, archived, and deterministic fairness tests. Confirm AO reads never create rows in `measurements` or send a write function.

### Task 2: Verified AO range configuration service and API

**Files:** `backend/modbus.py`, `backend/gateway.py`, `backend/output.py`, `backend/app.py`, `backend/store.py`, `tests/test_modbus.py`, `tests/test_output.py`, `tests/test_api.py`.

- Add a frame builder and FC06 acknowledgement decoder for a single register, preserving RTU CRC and the session transaction lock.
- Add `OutputDriver.set_output_range`: validate fixed output profile, active node/channel, code 0/1, fresh commissioned transparent session, safe setpoint compatibility, and no armed pipeline; hold the channel lock across pre-reads, FC06, and FC03 range verification.
- Record attempt, acknowledgement/readback, latency, request/response, and ambiguous outcome in a dedicated history record plus communication and audit events. Never retry an ambiguous FC06 write.
- Add authenticated `PUT /api/nodes/{node_id}/outputs/{channel}/range`, require `confirm: true`, return no-change distinctly, and invalidate/update range polling cache only after verified readback.
- Add simulator tests for AO1/AO6 addressing, CRC, FC06 echo, exception, timeout quarantine, compatibility checks, pipeline ownership, concurrency against FC16, and verified readback.

### Task 3: Trusted reconnect validation and lifecycle regressions

**Files:** `backend/app.py`, `backend/gateway.py`, `backend/healthcheck.py`, `tests/test_reconnect.py`, `README.md`.

- Correct or explicitly constrain any in-band identity path so it cannot consume or prepend bytes on the verified `MODBUS_TCP` G781 or `TRANSPARENT_SERIAL` W630 streams. Default zero-touch commissioning remains the existing dedicated private WireGuard ingress with unique peer `/32`, listener, and firewall binding.
- Verify mode/model compatibility at commissioning and continue session-generation fencing. Ensure automatic recovery never arms pipelines or replays FC16 commands; W630 reconnect probes remain FC03-only.
- Add tests for identity mismatch, suspended ingress, model/transport compatibility, replacement callback fencing, and no output writes on reconnect. Document that direct public LTE TCP alone cannot authenticate a gateway and requires a trusted site relay/VPN.

### Task 4: UI, operator documentation, and full validation

**Files:** `web/src/App.tsx`, `web/src/api.ts`, `README.md`, `docs/hardware-verification.md`, backend/frontend test files.

- Show AO defaults immediately in the existing Modbus Polling page and offer channel-specific Configure Range with current range/setpoint, pipeline ownership, gateway health, physical-output warning, and explicit confirmation. Display reported setpoint terminology.
- Show gateway auto-reconnect trust/state and last Modbus status in existing Settings registry without implying public-IP identity.
- Document FC06 manual evidence, readback limits, WireGuard prerequisites, and separate physical bench validation.
- Run compileall, complete unittest discovery, `npm ci`, and production build; fix regressions and review final diff for unauthorized writes, lock order, migration safety, and incorrect reconnect claims.
