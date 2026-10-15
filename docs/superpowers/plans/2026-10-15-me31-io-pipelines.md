# ME31 I/O Management and Pipeline Implementation Plan

> **For agentic workers:** implementation proceeds inline in this branch with focused commits after backend, integration, and UI checkpoints.

**Goal:** Add fixed ME31 AI/AO profiles, safe AO reads and writes, gateway lifecycle actions, and persistent, explicitly armed AI-to-AO pipelines.

**Architecture:** Keep TCP ownership in `SessionRegistry` and frame construction/validation in `backend.modbus`. Add a device-specific AO service and a pipeline service that share a per-output-channel lock, persist definitions/runtime state in the existing `mappings` table, and restart disarmed. Keep API handlers authenticated and thin; keep the pipeline page in a separate React component.

**Tech Stack:** FastAPI, asyncio, SQLite migrations, React, TypeScript, TanStack Query, existing Modbus codecs and CSS tokens.

---

### Work units

1. Inspect latest `main`; preserve the existing schema and extend `mappings` through an idempotent migration. Add immutable ME31 input/output profiles and normalize compatible legacy nodes/channels without modifying measurements or calibration history.
2. Add ME31 output FC03 reads and allowlisted FC16 Float32 writes in the Modbus layer. Add a serialized `GatewaySession` transaction that validates FC16 acknowledgement and reads the integer setpoint block back. Cover TCP/RTU frames, CRC, range validation, invalid values and mismatch paths with local simulators.
3. Add gateway edit, disconnect and dependency-aware delete APIs. Force ME31 profiles/models and six channels on node registration; protect channel/profile invariants on patch APIs.
4. Add output command ownership, manual output API, pipeline validation/API, unique active destination ownership, and a bounded background executor. New pipelines are disabled; arming verifies source freshness and output readback/range. Startup clears runtime arming and prevents stale replay.
5. Replace the Mapping placeholder with a pipeline table, create/edit/detail forms, explicit enable/arm confirmation, status/history, and output-node manual-test controls. Add gateway actions and errors in Settings without changing the visual system.
6. Add API, migration, protocol, service and end-to-end simulator tests; run the full Python suite, TypeScript check/build, review migrations/concurrency/safety, document deployment and hardware-only commissioning, then commit on this feature branch.

