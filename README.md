# Industrial Modbus I/O Service

Backend-first initial milestone for commissioning LTE gateway TCP sessions, polling configured Modbus input registers, preserving analog measurements, and applying per-channel calibration and engineering scaling.

## Current implementation boundary

- Persistent SQLite development store with explicit tables for gateways, nodes, input/output channels, device profiles, register definitions, polling items, calibration revisions, measurements, communication events, audit records, and planned mappings. PostgreSQL migrations are not included yet.
- The ME31 profile follows User Manual V1.1 register quantities for input measurements, device information, trigger/configuration registers, and network parameters. Network parameter polling is opt-in and starts disabled. Device writes remain unavailable.
- Only Modbus read functions FC03 and FC04 are sent. Reboot, reset, device setting writes, and analog output writes are not implemented.
- Gateway session identity is provisioned explicitly through the API. A TCP source address or port is never treated as identity. Connections remain pending until an operator assigns them.
- `MODBUS_TCP`, `MODBUS_RTU`, and `TRANSPARENT_SERIAL` are explicit gateway modes. The latter two use RTU ADUs with CRC. Confirm the USR-G781 operating mode and ME31 conversion setting before commissioning; a successful TCP handshake alone proves no Modbus path.
- See [hardware verification notes](docs/hardware-verification.md) for the register-profile basis and USR-G781 framing choices.
- Output nodes and mapping records are represented, but mapping execution and output writes are disabled.
- API authentication is a shared bearer token for this initial milestone. User accounts, role separation, and token rotation workflows are not included; place the API behind HTTPS and restrict management access.
- The database currently uses SQLite as a development option. For production deployment, add a PostgreSQL migration and backup policy before relying on long measurement retention.

## Run locally

Python 3.11 or newer is recommended. Create a virtual environment, install `requirements.txt`, set `ADMIN_API_TOKEN`, and start the ASGI service:

```sh
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
export ADMIN_API_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(40))')"
export DATABASE_PATH=./data/iot-modbus.db
uvicorn backend.app:app --host 127.0.0.1 --port 8000
```

The gateway listener uses `TCP_LISTEN_HOST` and `TCP_LISTEN_PORT` (default `0.0.0.0:5001`). The HTTP API defaults to the host and port passed to Uvicorn. `/api/health` exposes process health; management routes require `Authorization: Bearer <ADMIN_API_TOKEN>`.

For the React console, run `cd web && npm install && npm run dev`. Vite forwards API and WebSocket traffic to `127.0.0.1:8000`. Build with `npm run build`; when `web/dist` exists, FastAPI serves it at `/` with the API on the same origin.

## Commission a gateway

1. Create a gateway with an explicit transport mode:

   ```sh
   curl -H "Authorization: Bearer $ADMIN_API_TOKEN" -H 'Content-Type: application/json' \
     -d '{"name":"North pump LTE","model":"USR-G781","transport_mode":"TRANSPARENT_SERIAL"}' \
     http://127.0.0.1:8000/api/gateways
   ```

2. Configure the USR-G781 to connect to the VPS TCP listener. Confirm whether it forwards Modbus TCP ADUs or serial RTU bytes from its configured mode and document the matching `transport_mode`.
3. Inspect `/api/gateways/sessions`. Assign the pending session to the intended gateway using `POST /api/gateways/sessions/{session_id}/assign/{gateway_id}`. This is a manual commissioning action because the hardware connection has no application identity handshake in this implementation.
4. Add an analog input node, assign its gateway and slave ID, then create polling items from the profile. An enabled item is scheduled by the service; disabling it stops future scheduled requests.

For transparent serial or RTU mode, serial settings are configured on gateway/device hardware. The software profile defaults to the requested 9600 8N1, but the TCP service cannot verify those physical settings from the socket.

## API surface

- `GET /api/overview`, `/api/gateways`, `/api/nodes`, `/api/device-profiles`
- `POST /api/gateways`, `POST /api/nodes`, `PATCH /api/nodes/{id}`
- `GET/POST /api/nodes/{id}/polling`, `PATCH/DELETE /api/polling/{id}`, `POST /api/polling/{id}/read`
- `PUT /api/nodes/{id}/polling/groups/{category}`
- `GET/PUT /api/nodes/{id}/calibration`, `POST /api/nodes/{id}/calibration/two-point`, `POST /api/calibration/preview`
- `GET /api/nodes/{id}/measurements/latest`, `/history`, `GET /api/logs`, `GET /api/mappings`
- `WS /ws/telemetry` sends the bearer token in its first JSON message: `{"type":"auth","token":"…"}`.

Calibration corrections are stored as immutable channel revisions. Raw register values and raw mA are retained on each measurement; calibrated mA and engineering values are stored separately. Scaling reports `OUT_OF_RANGE` and preserves the extrapolated value rather than silently clamping it.

## Deployment notes

Run Uvicorn as a dedicated unprivileged systemd service. Keep the TCP port reachable by the configured gateways, but expose the management API only through a reverse proxy with HTTPS and network access controls. Do not bind the management API to a public interface without that proxy. Back up the database and define retention/rotation before production use. Neither the installed credentials nor the gateway's serial/modbus mode can be inferred by this service.

## Validation

Run deterministic protocol and calibration tests with:

```sh
python -m unittest discover -s tests -v
```

These tests use encoded protocol fixtures; they do not confirm USR-G781 forwarding mode, serial wiring, ME31 register behavior, LTE reachability, or any analog measurement on physical hardware.
