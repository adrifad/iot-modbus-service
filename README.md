# Industrial Modbus I/O Service

Backend-first initial milestone for commissioning LTE gateway TCP sessions, polling configured Modbus input registers, preserving analog measurements, and applying per-channel calibration and engineering scaling.

## Current implementation boundary

- Persistent SQLite development store with explicit tables for gateways, nodes, input/output channels, device profiles, register definitions, polling items, calibration revisions, measurements, communication events, audit records, and planned mappings. PostgreSQL migrations are not included yet.
- Built-in device profiles are fixed to EBYTE ME31-XAXX0600 input and ME31-XXXA0006 output models. Each registered node receives exactly six AI or AO channels. Migrations normalize recognized legacy ME31 nodes without changing measurements or calibration revisions.
- The input profile follows ME31 V1.1. The output driver supports only FC03 range/setpoint reads and allowlisted FC16 Float32 setpoint writes to AO1–AO6. Writes validate the configured range, FC16 acknowledgement, and FC03 reported setpoint readback. Readback confirms the module's reported setpoint; it is not a physical current measurement.
- Gateway session identity can be provisioned explicitly through the API. A TCP source address or port is never treated as identity. Connections remain pending until an operator assigns them. For a USR-G781 in network-transparent mode, the service also supports its documented LINK registration packet using a per-gateway token; enable it only over TLS or a dedicated trusted VPN, since the registration token is not a substitute for a protected transport.
- `MODBUS_TCP`, `MODBUS_RTU`, and `TRANSPARENT_SERIAL` are explicit gateway modes. The latter two use RTU ADUs with CRC. Confirm the USR-G781 operating mode and ME31 conversion setting before commissioning; a successful TCP handshake alone proves no Modbus path.
- See [hardware verification notes](docs/hardware-verification.md) for the register-profile basis and USR-G781 framing choices.
- I/O pipelines execute in the backend and persist in the existing `mappings` table. New pipelines start disabled/disarmed. Enabling and arming are separate, authenticated operator actions. Backend restart disarms every pipeline and never replays stale commands. Gateway loss disarms affected pipelines after applying their configured source-loss policy. SAFE_SETPOINT records module acknowledgement and reported setpoint readback only; it does not prove physical current. HOLD_LAST sends no new value and is not an electrical fail-safe.
- Pipeline workers are bounded by `PIPELINE_MAX_CONCURRENCY` (default 8); Modbus operations sharing one gateway remain serialized. Repeated output write failures enter `OUTPUT_FAULT_LOCKOUT` after three failures by default and require operator re-arming. An ambiguous FC16 timeout is recorded as `WRITE_OUTCOME_UNKNOWN`; an RTU session is not reused after timeout/desynchronization.
- AO range read, FC16 acknowledgement and setpoint readback timeouts default to 5000 ms. These settings are persisted and adjustable by an authenticated administrator under Settings; valid range is 250–30000 ms. Environment values seed a new database only.
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
3. For first commissioning, inspect `/api/gateways/sessions` and assign the intended pending session using `POST /api/gateways/sessions/{session_id}/assign/{gateway_id}`. This operator association is persisted as gateway registration, but the shared listener never guesses identity for later connections.
4. Register an AI node on a `MODBUS_TCP` gateway or an AO node on a `TRANSPARENT_SERIAL` gateway. The server assigns the fixed model/profile and creates six channels.
5. Enable AI1–AI6 polling and verify actual input readings. For AO nodes, inspect the reported range and setpoints before any manual test.
6. Create an I/O pipeline. It stays disabled and disarmed until it has a fresh source measurement and a responding AO node. Enable the definition, then explicitly arm output control. The backend will write only when both steps are complete.

Manual AO testing requires the administrator token and a separate in-page confirmation. A destination channel owned by an armed pipeline cannot be manually written. To delete a gateway, first disarm/delete its active pipelines and reassign or remove all assigned nodes; historical communication/audit events remain.

For transparent serial or RTU mode, serial settings are configured on gateway/device hardware. The software profile defaults to the requested 9600 8N1, but the TCP service cannot verify those physical settings from the socket.

## Secure automatic reconnection

The verified G781 Modbus TCP-conversion path and W630 transparent RTU-over-TCP path do not carry a verified, non-disruptive identity handshake. The service never inserts a registration preamble into either Modbus stream and never identifies a gateway by public source IP or ephemeral TCP port.

Automatic recommissioning is available only through a dedicated listener bound to a private WireGuard interface address. The authenticated WireGuard peer is mapped to a unique private `/32`, a unique ingress ID, and a dedicated TCP port. The application verifies the listener's local address belongs to the configured `wg*` interface and matches the expected peer address. A private address without an authenticated tunnel and firewall isolation is not sufficient.

For a direct-USR-LTE installation, an additional network prerequisite remains: the gateway's outbound TCP stream must be routed through an authenticated VPN peer or a trusted site-side VPN relay. The installed G781/W630 firmware configuration has not been verified to provide its own WireGuard, mutual TLS, or compatible identity handshake. If the DTU connects directly over carrier LTE to the public VPS, keep it on the shared listener and commission new sessions manually; do not provision automatic ingress based on carrier IP addresses.

### VPS and network setup

1. Install WireGuard on the VPS and a VPN-capable site router/relay. Example VPS peer configuration (replace key placeholders with generated keys; never put private keys in this repository):

   ```ini
   # /etc/wireguard/wg0.conf
   [Interface]
   Address = 10.70.0.1/24
   ListenPort = 51820
   PrivateKey = <VPS_PRIVATE_KEY>

   [Peer]
   PublicKey = <SITE_GATEWAY_PUBLIC_KEY>
   AllowedIPs = 10.70.0.11/32
   ```

   Assign every gateway a distinct peer `/32` and key. A site relay that serves several DTUs needs per-device authenticated peers or equivalent network isolation and source binding; one shared NAT identity does not identify individual gateways.

   A site relay's matching peer section uses its own private key and the VPS public key, with a route to the VPS tunnel address:

   ```ini
   [Interface]
   Address = 10.70.0.11/24
   PrivateKey = <SITE_PRIVATE_KEY>

   [Peer]
   PublicKey = <VPS_PUBLIC_KEY>
   Endpoint = <VPS_PUBLIC_IP>:51820
   AllowedIPs = 10.70.0.1/32
   PersistentKeepalive = 25
   ```
2. Configure the site side with its peer key and route the specific DTU's TCP flow through the tunnel. If forwarding/NAT is used, make the VPS see that DTU's unique WireGuard peer address as the TCP source; WireGuard `AllowedIPs` must bind the address to the peer key. Configure the DTU's remote host as the VPN-routed VPS address and its dedicated listener port. Keep each unit's verified framing unchanged: G781 uses `MODBUS_TCP`; W630 uses `TRANSPARENT_SERIAL`.
3. Set `/etc/iot-modbus-service.env`:

   ```env
   GATEWAY_TRUSTED_INGRESS_ENABLED=true
   GATEWAY_TRUSTED_INGRESS_HOST=10.70.0.1
   GATEWAY_TRUSTED_INGRESS_INTERFACE=wg0
   GATEWAY_HEALTHCHECK_ENABLED=true
   GATEWAY_HEALTHCHECK_INTERVAL_SECONDS=30
   GATEWAY_HEALTHCHECK_TIMEOUT_MS=5000
   GATEWAY_HEALTHCHECK_FAILURE_THRESHOLD=3
   ```

   The `wg0` interface and address must already exist before the service starts. The service rejects wildcard, public, or non-WireGuard listener addresses.
4. Firewall each dedicated TCP port to accept traffic only on `wg0` and only from that gateway's WireGuard peer `/32`. Do not expose these ports on the public interface. Keep the shared port 5001 separate for initial/manual commissioning.
5. Configure the G781 to connect to the VPN-routed VPS address and its dedicated port using its existing Modbus TCP conversion mode. Configure the W630 to connect to the same private VPS address on its own dedicated port using its existing transparent RTU-over-TCP mode. Do not add identity bytes or change the framing. The USR units' direct LTE path has not been verified to route through WireGuard; if they cannot reach the VPN address through an authenticated relay, use manual commissioning instead.
6. In Settings → Gateway registry → Actions → Provision secure reconnect, register the unique peer address and a unique dedicated port for the gateway. For example, assign `10.70.0.11`, port `5101` to the G781 and a distinct WireGuard peer and port `5102` to the W630. The API reports `INGRESS_UNAVAILABLE` and refuses auto commissioning if the WireGuard interface prerequisite is missing.

   The authenticated administrator API equivalent is:

   ```sh
   curl -X PUT -H "Authorization: Bearer $ADMIN_API_TOKEN" -H 'Content-Type: application/json' \
     -d '{"enabled":true,"ingress_id":"g781-wg","listener_port":5101,"trusted_peer_ip":"10.70.0.11"}' \
     http://127.0.0.1:8000/api/gateways/gw-input-01/trusted-ingress
   ```
7. Disconnect and reconnect the DTUs through the VPN. Verify a new session generation, automatic commissioning, and a valid Modbus response. A reconnect never re-arms output pipelines; an operator must explicitly arm after checking source freshness and the fresh AO setpoint readback.

Example firewall rule (repeat with a unique port and peer per gateway):

```sh
sudo ufw allow in on wg0 from 10.70.0.11 to 10.70.0.1 port 5101 proto tcp
```

Keep the host firewall's default inbound policy closed and do not add a public-interface allow rule for the dedicated port.

Health checks default to read-only Modbus probes every 30 seconds after an idle period, with a 5-second request timeout and a three-failure degraded threshold. G781 probes use FC04 `0x0064`, quantity 6; W630 probes use FC03 range `0x0514`, quantity 6, and setpoint `0x0064`, quantity 6. No FC16 keepalives are sent. `TCP_IDLE_TIMEOUT_SECONDS` is deprecated and ignored; TCP keepalive supplements Modbus health checks.

## API surface

- `GET /api/overview`, `/api/gateways`, `/api/nodes`, `/api/device-profiles`
- `POST /api/gateways`, `PATCH/DELETE /api/gateways/{id}`, `POST /api/gateways/{id}/disconnect`, `PUT /api/gateways/{id}/trusted-ingress`, `POST /api/nodes`, `PATCH /api/nodes/{id}`
- `GET /api/nodes/{id}/outputs`, `POST /api/nodes/{id}/outputs/{channel}/set` (manual test requires `{ "target_ma": 12.5, "confirm": true }`)
- `GET/POST /api/pipelines`, `GET/PATCH/DELETE /api/pipelines/{id}`, `POST /api/pipelines/{id}/enable`, `/disable`, `/arm`, `/disarm`, `GET /api/pipelines/{id}/status`, `/history`
- `GET/POST /api/nodes/{id}/polling`, `PATCH/DELETE /api/polling/{id}`, `POST /api/polling/{id}/read`
- `PUT /api/nodes/{id}/polling/groups/{category}`
- `GET/PUT /api/nodes/{id}/calibration`, `POST /api/nodes/{id}/calibration/two-point`, `POST /api/calibration/preview`
- `GET /api/nodes/{id}/measurements/latest`, `/history`, `GET /api/logs`, and compatibility `GET /api/mappings`
- `WS /ws/telemetry` sends the bearer token in its first JSON message: `{"type":"auth","token":"…"}`.

Calibration corrections are stored as immutable channel revisions. Raw register values and raw mA are retained on each measurement; calibrated mA and engineering values are stored separately. Scaling reports `OUT_OF_RANGE` and preserves the extrapolated value rather than silently clamping it. A source-disconnect callback runs the same centralized failure transition as polling loss: SAFE_SETPOINT is attempted once when the output path remains commissioned; an unavailable destination is reported `FAILSAFE_UNCONFIRMED`. Intentional backend shutdown suppresses fail-safe output writes.

## Deployment notes

### Ubuntu systemd deployment

The supplied unit uses `StateDirectory=iot-modbus-service` and writes SQLite and its WAL files under `/var/lib/iot-modbus-service`. Do not change this to a relative database path when using the unit: `ProtectSystem=strict` intentionally keeps the application tree read-only.

```sh
sudo useradd --system --home /nonexistent --shell /usr/sbin/nologin iotmodbus
sudo install -d -o root -g root -m 0750 /etc/iot-modbus-service
sudo install -o root -g root -m 0600 .env.example /etc/iot-modbus-service.env
sudoedit /etc/iot-modbus-service.env  # set a strong ADMIN_API_TOKEN and deployment values
sudo install -o root -g root -m 0644 deploy/iot-modbus.service /etc/systemd/system/iot-modbus.service
sudo systemctl daemon-reload
sudo systemctl enable --now iot-modbus.service
sudo systemctl status iot-modbus.service
sudo journalctl -u iot-modbus.service -f
```

Deploy the code and its `.venv` at `/opt/iot-modbus-service` before starting the unit. Keep the TCP port reachable by configured gateways, but expose the management API only through a reverse proxy with HTTPS and network access controls. Do not bind the management API publicly without that proxy. Back up the database and define retention/rotation before production use. `systemd` creates and owns the state directory for `iotmodbus`; the service retains strict filesystem protection. Check `journalctl -u iot-modbus.service` after restarts. For a local development run, override `DATABASE_PATH=./data/iot-modbus.db` as shown above.

SQLite access is serialized by the service and uses WAL plus a 5-second busy timeout; this is suitable for a modest single-VPS deployment with one service process. Prefer PostgreSQL before running multiple backend workers/instances or when write volume, retention, concurrent readers, replication, or HA requirements exceed a single SQLite writer.

## Validation

Run deterministic protocol and calibration tests with:

```sh
python -m unittest discover -s tests -v
```

These tests use encoded protocol fixtures; they do not confirm USR-G781 forwarding mode, serial wiring, ME31 register behavior, LTE reachability, or any analog measurement on physical hardware.

## ME31 output commissioning

Before registering an output node, confirm the W630 passes transparent Modbus RTU bytes and use `TRANSPARENT_SERIAL`; input G781 nodes continue using their verified `MODBUS_TCP` mode. Verify the ME31 output slave ID, AO range registers at `0x0514`, and setpoint readback registers at `0x0064` on the actual bench device. First test a low-risk manual setpoint while a qualified operator observes the connected load. Confirm both FC16 acknowledgement and setpoint readback, then independently measure the analog output with a calibrated instrument. The application does not perform that physical current verification.

Create pipelines disabled. Verify the input reading, output range, source freshness and readback before enabling and arming. HOLD_LAST means no new write is sent when input data is invalid and the module may retain its prior setpoint. SAFE_SETPOINT requires a configured value and a reachable output gateway; a disconnected gateway cannot be reported as safe. After a gateway disconnect or backend restart, pipelines require an operator to arm them again. `OUTPUT_FAULT_LOCKOUT` also requires an operator to inspect communication and device state before re-arming. If an FC16 acknowledgement times out, the AO may have changed; the service records `WRITE_OUTCOME_UNKNOWN` and will not blindly retry that command. A desynchronized RTU-over-TCP session must be reconnected and manually commissioned before any subsequent output operation.
