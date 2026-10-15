"""FastAPI control plane and lifecycle for the Modbus monitoring service."""

import asyncio
import hashlib
import hmac
import ipaddress
import json
import math
import os
import secrets
import socket
import ssl
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from .gateway import GatewaySession
from .modbus import decode_registers
from .output import OutputDriver, OutputError
from .pipelines import PipelineError, PipelineService
from .profiles import PROFILES, register
from .scheduler import PollScheduler
from .store import Store, utc_now


class SessionRegistry:
    def __init__(self, store):
        self.store = store
        self.sessions: dict[str, GatewaySession] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        self.by_gateway: dict[str, str] = {}
        self.lock = asyncio.Lock()
        self.shutting_down = False

    def for_gateway(self, gateway_id):
        session = self.sessions.get(self.by_gateway.get(gateway_id, ""))
        return session if session and session.commissioned else None

    async def assign(self, session, gateway, *, identity_verified=False, commissioning_method="OPERATOR_ASSIGNMENT"):
        replaced = False
        async with self.lock:
            previous = self.for_gateway(gateway["id"])
            if previous is session:
                return
            if previous:
                previous.state = "RETIRED"
                self.by_gateway.pop(gateway["id"], None)
                await previous.retire()
                self.store.event("SESSION_REPLACED", "Gateway session explicitly replaced during commissioning", gateway_id=gateway["id"],protocol=previous.transport_mode)
                replaced = True
            await session.commission(gateway["id"], gateway["transport_mode"], identity_verified=identity_verified, commissioning_method=commissioning_method)
            self.by_gateway[gateway["id"]] = session.session_id
            old_connected = gateway.get("last_connected_at") is not None
            now = utc_now()
            self.store.run("UPDATE gateways SET last_connected_at=?,last_activity_at=?,reconnection_count=reconnection_count+?,updated_at=? WHERE id=?", (now,now,int(old_connected),now,gateway["id"]))
            self.store.event("RECONNECTED" if old_connected else "CONNECTED", f"Gateway session commissioned from {session.peer}", gateway_id=gateway["id"],protocol=session.transport_mode)
        if replaced and self.store is store:
            await pipelines.gateway_disconnected(gateway["id"], "previous session replaced", operator_initiated=False)

    async def shutdown(self):
        self.shutting_down = True
        sessions = list(self.sessions.values())
        for session in sessions:
            await session.retire()
        tasks = {task for task in self.tasks.values() if task is not asyncio.current_task() and not task.done()}
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=5)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        self.sessions.clear()
        self.tasks.clear()
        self.by_gateway.clear()


store = Store()
sessions = SessionRegistry(store)
scheduler = PollScheduler(store, sessions)
output_driver = OutputDriver(store, sessions)
pipelines = PipelineService(store, sessions, output_driver)
tcp_server = None


async def on_disconnect(session, error=None):
    if session.gateway_id:
        if sessions.by_gateway.get(session.gateway_id) == session.session_id:
            sessions.by_gateway.pop(session.gateway_id, None)
            reason = str(error or "Gateway TCP session closed")
            store.run("UPDATE gateways SET last_disconnected_at=?,last_disconnect_reason=?,connection_errors=connection_errors+? WHERE id=?", (utc_now(),reason,int(error is not None),session.gateway_id))
            store.event("SESSION_DESYNCHRONIZED" if session.desynchronized else "DISCONNECTED", reason, gateway_id=session.gateway_id, protocol=session.transport_mode)
            # Free the closed socket slot before any bounded fail-safe network
            # operation; this callback runs without the registry lock.
            sessions.sessions.pop(session.session_id, None)
            sessions.tasks.pop(session.session_id, None)
            if not sessions.shutting_down:
                await pipelines.gateway_disconnected(session.gateway_id, reason, operator_initiated=False)
    elif session.state != "RETIRED":
        store.event("PENDING_DISCONNECTED", "Uncommissioned TCP session closed", protocol="TCP")


async def on_invalid_gateway_frame(session, detail):
    store.event("INVALID_RESPONSE", detail, gateway_id=session.gateway_id, protocol=session.transport_mode)


async def on_unsolicited_gateway_frame(session, frame, detail):
    store.event("UNSOLICITED_FRAME", detail, gateway_id=session.gateway_id, protocol=session.transport_mode,
                direction="RX", response_hex=frame.hex(" "))


def _peer_allowed_for_identity(session):
    if session.is_tls:
        return True
    peer_ip = getattr(session, "peer_ip", "")
    try:
        address = ipaddress.ip_address(peer_ip)
        networks = [ipaddress.ip_network(value.strip(), strict=False) for value in os.getenv("GATEWAY_TRUSTED_CIDRS", "").split(",") if value.strip()]
        return any(address in network for network in networks)
    except ValueError:
        return False


async def authenticate_gateway_identity(session, identity):
    try:
        parts = identity.decode("ascii").split(" ")
        if len(parts) != 3 or parts[0] != "FIELDLINE/1":
            raise ValueError
        _, gateway_id, token = parts
        gateway = store.one("SELECT * FROM gateways WHERE id=?", (gateway_id,))
        if (not gateway or gateway["transport_mode"] != "TRANSPARENT_SERIAL" or not gateway.get("registration_token_hash")
                or not _peer_allowed_for_identity(session)
                or not hmac.compare_digest(hashlib.sha256(token.encode()).hexdigest(), gateway["registration_token_hash"])):
            raise ValueError
        await sessions.assign(session, gateway, identity_verified=True, commissioning_method="G781_LINK_REGISTRATION")
        store.audit("IDENTITY_COMMISSION","gateway",gateway_id,{"method":"G781_LINK_REGISTRATION","tls":session.is_tls},"gateway")
        return True
    except (UnicodeDecodeError, ValueError):
        store.event("IDENTITY_REJECTED", "Gateway registration packet was invalid or arrived outside a trusted TLS/VPN path", protocol="TCP")
        await session.retire()
        return False


def _item_freshness_seconds(item):
    default = max(1, int(os.getenv("DEFAULT_STALE_AFTER_SECONDS", "30")))
    return max(default, (int(item["interval_ms"]) * 2 + int(item["timeout_ms"])) / 1000)


def _item_health(item, now=None):
    now = now or datetime.now(timezone.utc)
    if not item["enabled"]:
        return "DISABLED"
    if not item.get("last_success_at"):
        return item["last_status"] if item["last_status"] in {"TIMEOUT","MODBUS_ERROR","INVALID_RESPONSE","DECODE_ERROR","MEASUREMENT_ERROR","DISCONNECTED"} else "UNKNOWN"
    try:
        last = datetime.fromisoformat(item["last_success_at"])
    except (TypeError, ValueError):
        return "UNKNOWN"
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    if (now - last).total_seconds() > _item_freshness_seconds(item):
        return "STALE"
    if item["last_status"] != "GOOD":
        return item["last_status"]
    return "RESPONDING"


def _node_modbus_health(node, items, gateway_connected):
    if node["node_type"] == "ANALOG_OUTPUT":
        if not node.get("gateway_id"):
            return "UNKNOWN"
        if not gateway_connected:
            return "DISCONNECTED"
        if not node.get("polling_enabled"):
            return "DISABLED"
        if not node.get("id"):
            return "UNKNOWN"
        recent=store.one("SELECT at,result FROM events WHERE node_id=? AND result IN ('OUTPUT_RANGE_READ','OUTPUT_SETPOINT_READ','OUTPUT_RANGE_CHECK','OUTPUT_READBACK','OUTPUT_WRITE_ACKNOWLEDGED','OUTPUT_READ_FAILED','OUTPUT_WRITE_FAILED','OUTPUT_READBACK_FAILED') ORDER BY id DESC LIMIT 1",(node["id"],))
        if not recent:
            return "UNKNOWN"
        if recent["result"] in {"OUTPUT_READ_FAILED","OUTPUT_WRITE_FAILED","OUTPUT_READBACK_FAILED"}:
            return "MODBUS_ERROR"
        try:
            timestamp=datetime.fromisoformat(recent["at"])
            if timestamp.tzinfo is None: timestamp=timestamp.replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc)-timestamp).total_seconds()>int(os.getenv("OUTPUT_STATUS_FRESHNESS_SECONDS","120")):
                return "STALE"
        except (TypeError,ValueError):
            return "UNKNOWN"
        return "RESPONDING"
    if not gateway_connected:
        return "DISCONNECTED" if node.get("gateway_id") else "UNKNOWN"
    if not node.get("polling_enabled"):
        return "DISABLED"
    enabled = [item for item in items if item["enabled"]]
    if not enabled:
        return "DISABLED"
    states = [_item_health(item) for item in enabled]
    for state in ("MODBUS_ERROR","INVALID_RESPONSE","DECODE_ERROR","MEASUREMENT_ERROR","POLL_ERROR","TIMEOUT","DISCONNECTED"):
        if state in states:
            return state
    if "STALE" in states:
        return "STALE"
    if "UNKNOWN" in states:
        return "UNKNOWN"
    return "RESPONDING"


def _measurement_quality(node, channel, measurement, items, channels):
    if not node.get("polling_enabled"):
        return "DISABLED"
    channel_record = next((row for row in channels if row["channel_type"] == "ANALOG_INPUT" and row["channel_index"] == channel), None)
    if not channel_record or not channel_record["enabled"]:
        return "DISABLED"
    mapped = []
    for item in items:
        try:
            metadata=json.loads(item.get("measurement_metadata_json") or "{}")
        except (TypeError,json.JSONDecodeError):
            continue
        if item["enabled"] and channel in metadata.get("channel_indices",[]):
            mapped.append(item)
    if not mapped:
        return "DISABLED"
    if not measurement:
        return "UNKNOWN"
    newest=max(mapped,key=lambda item:item.get("last_success_at") or "")
    state=_item_health(newest)
    if state in {"INVALID_RESPONSE","DECODE_ERROR","MODBUS_ERROR"}:
        return "INVALID_RESPONSE"
    if state in {"STALE","TIMEOUT","DISCONNECTED"}:
        return "STALE"
    if state in {"UNKNOWN","DISABLED"}:
        return state
    if measurement["quality"] == "OUT_OF_RANGE":
        return "OUT_OF_RANGE"
    try:
        measured=datetime.fromisoformat(measurement["measured_at"])
        if measured.tzinfo is None: measured=measured.replace(tzinfo=timezone.utc)
        if (datetime.now(timezone.utc)-measured).total_seconds()>_item_freshness_seconds(newest):
            return "STALE"
    except (TypeError,ValueError):
        return "UNKNOWN"
    return "GOOD"


async def on_tcp_client(reader, writer):
    pending = sum(1 for session in sessions.sessions.values() if not session.gateway_id and not session.closed)
    maximum = int(os.getenv("TCP_MAX_CONNECTIONS", "128"))
    if len(sessions.sessions) >= maximum or pending >= int(os.getenv("TCP_MAX_PENDING_SESSIONS", "32")):
        writer.close()
        try: await writer.wait_closed()
        except (ConnectionError,OSError): pass
        return
    peer_info = writer.get_extra_info("peername")
    peer_ip = str(peer_info[0]) if peer_info else "unknown"
    peer = f"[{peer_info[0]}]:{peer_info[1]}" if peer_info and ":" in str(peer_info[0]) else f"{peer_ip}:{peer_info[1]}" if peer_info else "unknown"
    sock = writer.get_extra_info("socket")
    if sock:
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, int(os.getenv("TCP_KEEPALIVE_IDLE_SECONDS", "60")))
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, int(os.getenv("TCP_KEEPALIVE_INTERVAL_SECONDS", "15")))
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, int(os.getenv("TCP_KEEPALIVE_COUNT", "4")))
        except (AttributeError, OSError, ValueError):
            pass
    session = GatewaySession(str(uuid4()), reader, writer, peer)
    session.peer_ip = peer_ip
    sessions.sessions[session.session_id] = session
    sessions.tasks[session.session_id] = asyncio.current_task()
    store.event("PENDING", f"TCP connection awaiting explicit gateway assignment from {peer}", protocol="TCP")
    await session.run_reader(on_disconnect, on_invalid_gateway_frame, authenticate_gateway_identity, on_unsolicited_gateway_frame)
    sessions.sessions.pop(session.session_id, None)
    sessions.tasks.pop(session.session_id, None)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global tcp_server
    sessions.shutting_down = False
    host = os.getenv("TCP_LISTEN_HOST", "0.0.0.0")
    port = int(os.getenv("TCP_LISTEN_PORT", "5001"))
    cert_file, key_file = os.getenv("TCP_TLS_CERT_FILE"), os.getenv("TCP_TLS_KEY_FILE")
    ssl_context = None
    if cert_file or key_file:
        if not cert_file or not key_file:
            raise RuntimeError("TCP_TLS_CERT_FILE and TCP_TLS_KEY_FILE must be configured together")
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
        ssl_context.load_cert_chain(cert_file, key_file)
    tcp_server = await asyncio.start_server(on_tcp_client, host, port, limit=65536, ssl=ssl_context)
    scheduler.start()
    pipelines.start()
    yield
    # Mark intentional shutdown before cancelling RTU transactions. Their
    # retired sessions must not trigger physical fail-safe writes.
    sessions.shutting_down = True
    await pipelines.stop()
    await scheduler.stop()
    tcp_server.close()
    await tcp_server.wait_closed()
    await sessions.shutdown()
    store.close()


app = FastAPI(title="Industrial Modbus I/O Service", version="0.1.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:5173").split(","), allow_methods=["GET","POST","PUT","PATCH","DELETE"], allow_headers=["Authorization","Content-Type"])


async def authorize(authorization: str | None = Header(default=None)):
    expected = os.getenv("ADMIN_API_TOKEN", "")
    if not expected:
        raise HTTPException(503, "ADMIN_API_TOKEN is not configured")
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not hmac.compare_digest(token, expected):
        raise HTTPException(401, "Valid bearer token required")
    return "admin"


class GatewayCreate(BaseModel):
    id: str | None = Field(default=None, min_length=1, max_length=100, pattern=r"^[A-Za-z0-9._-]+$")
    name: str = Field(min_length=1, max_length=100)
    model: str = Field(default="", max_length=100)
    transport_mode: str

    @field_validator("transport_mode")
    @classmethod
    def valid_mode(cls, value):
        if value not in {"MODBUS_TCP", "MODBUS_RTU", "TRANSPARENT_SERIAL"}:
            raise ValueError("unsupported transport mode")
        return value


class GatewayPatch(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    model: str | None = Field(default=None, max_length=100)
    transport_mode: str | None = None

    @field_validator("transport_mode")
    @classmethod
    def valid_optional_mode(cls,value):
        if value is not None and value not in {"MODBUS_TCP","MODBUS_RTU","TRANSPARENT_SERIAL"}:
            raise ValueError("unsupported transport mode")
        return value


class NodeCreate(BaseModel):
    id: str | None = Field(default=None, max_length=100)
    name: str = Field(min_length=1, max_length=100)
    node_type: str
    hardware_model: str = ""
    gateway_id: str | None = None
    slave_id: int = Field(default=1, ge=1, le=247)
    profile_id: str | None = None
    polling_enabled: bool = True

    @field_validator("node_type")
    @classmethod
    def valid_node_type(cls, value):
        if value not in {"ANALOG_INPUT", "ANALOG_OUTPUT"}:
            raise ValueError("node_type must be ANALOG_INPUT or ANALOG_OUTPUT")
        return value


class PipelineCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    source_node_id: str = Field(min_length=1, max_length=100)
    source_channel: int = Field(ge=1, le=6)
    destination_node_id: str = Field(min_length=1, max_length=100)
    destination_channel: int = Field(ge=1, le=6)
    source_value_type: str
    config: dict = Field(default_factory=dict)

    @field_validator("source_value_type")
    @classmethod
    def valid_source_value_type(cls, value):
        if value not in {"RAW_MA", "CALIBRATED_MA", "ENGINEERING"}:
            raise ValueError("source value must be RAW_MA, CALIBRATED_MA, or ENGINEERING")
        return value


class PipelineConfirm(BaseModel):
    confirm: bool = False


class OutputSetpointInput(BaseModel):
    target_ma: float
    confirm: bool = False

    @field_validator("target_ma", mode="before")
    @classmethod
    def reject_boolean_target(cls, value):
        if isinstance(value, bool):
            raise ValueError("target current must be a number, not a boolean")
        return value

    @field_validator("target_ma")
    @classmethod
    def finite_target(cls, value):
        if not math.isfinite(value):
            raise ValueError("target current must be finite")
        return value


class OutputTimeoutSettings(BaseModel):
    output_range_timeout_ms: int = Field(ge=250, le=30000)
    output_write_timeout_ms: int = Field(ge=250, le=30000)
    output_readback_timeout_ms: int = Field(ge=250, le=30000)

class PollItemCreate(BaseModel):
    id: str | None = Field(default=None, max_length=100)
    profile_id: str | None = None
    register_name: str = Field(min_length=1, max_length=120)
    function_code: int = Field(ge=3, le=4)
    address: int = Field(ge=0, le=65535)
    quantity: int = Field(ge=1, le=125)
    data_type: str = "uint16"
    byte_order: str = "big"
    word_order: str = "unverified"
    interval_ms: int = Field(default=1000, ge=250, le=86400000)
    timeout_ms: int = Field(default=1500, ge=100, le=60000)
    retries: int = Field(default=1, ge=0, le=5)
    scale: float = 1
    unit: str = ""
    enabled: bool = True

    @field_validator("data_type")
    @classmethod
    def valid_data_type(cls,value):
        if value not in {"uint16","int16","uint16_array","enum16_array","float32","float32_array","ascii","raw"}:
            raise ValueError("unsupported register data type")
        return value

    @field_validator("byte_order")
    @classmethod
    def valid_byte_order(cls,value):
        if value not in {"big","little"}: raise ValueError("byte order must be big or little")
        return value

    @field_validator("word_order")
    @classmethod
    def valid_word_order(cls,value):
        if value not in {"BIG_WORD_FIRST","LITTLE_WORD_FIRST","unverified"}: raise ValueError("unsupported word order")
        return value

    @field_validator("scale")
    @classmethod
    def valid_scale(cls,value):
        if not math.isfinite(value): raise ValueError("scale must be finite")
        return value


class CalibrationInput(BaseModel):
    channel: int = Field(ge=1, le=6)
    gain: float = Field(gt=0, le=10)
    offset_ma: float = Field(ge=-25, le=25)
    enabled: bool = True
    method: str = "GAIN_OFFSET"
    scaling: dict | None = None

    @field_validator("gain", "offset_ma")
    @classmethod
    def finite_calibration(cls, value):
        if not math.isfinite(value): raise ValueError("calibration values must be finite")
        return value


class ChannelCreate(BaseModel):
    channel_index: int = Field(ge=1, le=65535)
    label: str = Field(min_length=1, max_length=80)


class FloatVerificationInput(BaseModel):
    channel: int = Field(ge=1, le=6)
    reference_ma: float = Field(ge=0, le=30)
    tolerance_ma: float = Field(default=0.1, gt=0, le=5)
    byte_order: str
    word_order: str

    @field_validator("byte_order")
    @classmethod
    def valid_verification_byte_order(cls,value):
        if value not in {"big","little"}: raise ValueError("byte order must be big or little")
        return value

    @field_validator("word_order")
    @classmethod
    def valid_word_order(cls,value):
        if value not in {"BIG_WORD_FIRST","LITTLE_WORD_FIRST"}: raise ValueError("word order must be verified and selected")
        return value


class CalibrationCaptureInput(BaseModel):
    tolerance_ma: float = Field(default=0.05, gt=0, le=2)

    @field_validator("tolerance_ma")
    @classmethod
    def finite_capture_tolerance(cls, value):
        if not math.isfinite(value): raise ValueError("capture tolerance must be finite")
        return value


class PreviewInput(BaseModel):
    raw_ma: float
    gain: float = Field(gt=0, le=10)
    offset_ma: float = Field(ge=-25, le=25)
    enabled: bool = True
    scaling: dict | None = None

    @field_validator("raw_ma", "gain", "offset_ma")
    @classmethod
    def finite_preview(cls, value):
        if not math.isfinite(value): raise ValueError("preview values must be finite")
        return value


@app.get("/api/health")
async def health():
    return {"service": "UP", "poller": "RUNNING" if scheduler.running else "STOPPED", "time": utc_now()}


@app.get("/api/overview")
async def overview(_: str = Depends(authorize)):
    nodes = store.rows("SELECT * FROM nodes ORDER BY name")
    gateways = store.rows("SELECT * FROM gateways")
    latest = store.one("SELECT MAX(measured_at) AS at FROM measurements")
    connected_gateway_ids = {gateway["id"] for gateway in gateways if sessions.for_gateway(gateway["id"])}
    counts = {}
    for kind in ("ANALOG_INPUT", "ANALOG_OUTPUT"):
        typed = [node for node in nodes if node["node_type"] == kind]
        gateways_connected=sum(node["gateway_id"] in connected_gateway_ids for node in typed)
        assigned=sum(node["gateway_id"] is not None for node in typed)
        counts[kind.lower()] = {"total": len(typed), "connected": gateways_connected, "disconnected": assigned-gateways_connected,
                                "unassigned":len(typed)-assigned,"gateway_connected":gateways_connected,"modbus_responding":0,"unhealthy":0,"health_unknown":0}
    item_stats = store.one("SELECT SUM(CASE WHEN p.enabled=1 AND n.polling_enabled=1 THEN 1 ELSE 0 END) active FROM polling_items p JOIN nodes n ON n.id=p.node_id") or {}
    event_stats = store.one("SELECT SUM(CASE WHEN result IN ('SUCCESS','FLOAT_VERIFIED') THEN 1 ELSE 0 END) successful,SUM(CASE WHEN result IN ('TIMEOUT','DISCONNECTED','INVALID_RESPONSE','MODBUS_ERROR','DECODE_ERROR','POLL_ERROR','MEASUREMENT_PROCESSING_ERROR') THEN 1 ELSE 0 END) failed,AVG(CASE WHEN result='SUCCESS' THEN latency_ms END) avg_latency_ms FROM events") or {}
    node_summary = []
    for node in nodes:
        last = store.one("SELECT MAX(measured_at) at FROM measurements WHERE node_id=?", (node["id"],))
        last_modbus=store.one("SELECT MAX(at) at FROM events WHERE node_id=? AND result IN ('OUTPUT_RANGE_READ','OUTPUT_SETPOINT_READ','OUTPUT_RANGE_CHECK','OUTPUT_READBACK','OUTPUT_WRITE_ACKNOWLEDGED')",(node["id"],)) if node["node_type"]=="ANALOG_OUTPUT" else {"at":None}
        item_count = store.one("SELECT COUNT(*) count FROM channels WHERE node_id=? AND enabled=1", (node["id"],))["count"]
        items=store.rows("SELECT * FROM polling_items WHERE node_id=?",(node["id"],))
        gateway_connected=node["gateway_id"] in connected_gateway_ids
        health_state=_node_modbus_health(node,items,gateway_connected)
        bucket=counts[node["node_type"].lower()]
        if health_state=="RESPONDING": bucket["modbus_responding"]+=1
        elif health_state not in {"UNKNOWN","DISABLED"}: bucket["unhealthy"]+=1
        else: bucket["health_unknown"]+=1
        node_summary.append({**node,"gateway_state":"CONNECTED" if gateway_connected else "DISCONNECTED" if node["gateway_id"] else "UNKNOWN",
                             "gateway_connected":gateway_connected,"modbus_health":health_state,"modbus_healthy":health_state=="RESPONDING",
                             "last_measurement_at":last["at"],"last_modbus_at":last_modbus["at"],"enabled_channel_count":item_count})
    return {"inputs":counts["analog_input"],"outputs":counts["analog_output"],"gateway_connections":{"total":len(gateways),"connected":len(connected_gateway_ids)},"polling":{"active":item_stats.get("active") or 0,"successful":event_stats.get("successful") or 0,"failed":event_stats.get("failed") or 0,"average_response_ms":event_stats.get("avg_latency_ms"),"state":"RUNNING" if scheduler.running else "STOPPED"},"last_measurement_at":latest["at"] if latest else None,"nodes":node_summary,"recent_events":store.rows("SELECT * FROM events ORDER BY id DESC LIMIT 8")}


@app.get("/api/gateways")
async def list_gateways(_: str = Depends(authorize)):
    result=[]
    for row in store.rows("SELECT * FROM gateways ORDER BY name"):
        public=_gateway_public(row)
        diagnostic=store.one("SELECT at,result,detail FROM events WHERE gateway_id=? ORDER BY id DESC LIMIT 1",(row["id"],))
        public["last_diagnostic"]=diagnostic
        session=sessions.for_gateway(row["id"])
        public["connection_state"]="CONNECTED" if session else "DISCONNECTED"
        public["commissioning_state"]="COMMISSIONED" if session else "PENDING_OR_DISCONNECTED"
        public["identity_state"]="VERIFIED" if session and session.identity_verified else "OPERATOR_ASSOCIATED" if session else "NOT_VERIFIED"
        public["commissioning_method"]=session.commissioning_method if session else None
        public["automatic_commissioning_available"]=bool(row.get("model","").upper().find("G781")>=0 and row.get("identity_method")=="G781_LINK_REGISTRATION" and row.get("transport_mode")=="TRANSPARENT_SERIAL")
        public["manual_intervention_required"]=session is None
        public["last_modbus_transaction"]=store.one("SELECT at,result,function_code,address,quantity,latency_ms,detail FROM events WHERE gateway_id=? AND function_code IS NOT NULL ORDER BY id DESC LIMIT 1",(row["id"],))
        result.append(public)
    return result


def _gateway_public(row):
    return {key:value for key,value in row.items() if key != "registration_token_hash"}


@app.post("/api/gateways", status_code=201)
async def create_gateway(data: GatewayCreate, actor=Depends(authorize)):
    gateway_id, now = data.id or str(uuid4()), utc_now()
    try:
        store.run("INSERT INTO gateways(id,name,model,transport_mode,created_at,updated_at) VALUES(?,?,?,?,?,?)", (gateway_id,data.name,data.model,data.transport_mode,now,now))
    except Exception as exc:
        raise HTTPException(409, "Gateway ID already exists or configuration is invalid") from exc
    store.audit("CREATE","gateway",gateway_id,data.model_dump(),actor)
    return _gateway_public(store.one("SELECT * FROM gateways WHERE id=?", (gateway_id,)))


@app.post("/api/gateways/{gateway_id}/registration-token")
async def rotate_gateway_registration_token(gateway_id: str, actor=Depends(authorize)):
    gateway=store.one("SELECT * FROM gateways WHERE id=?",(gateway_id,))
    if not gateway: raise HTTPException(404,"Gateway not found")
    if gateway["transport_mode"]!="TRANSPARENT_SERIAL":
        raise HTTPException(422,"G781 LINK registration packets are only enabled for verified network-transparent RTU framing")
    token=secrets.token_urlsafe(32)
    store.run("UPDATE gateways SET registration_token_hash=?,identity_method='G781_LINK_REGISTRATION',updated_at=? WHERE id=?",(hashlib.sha256(token.encode()).hexdigest(),utc_now(),gateway_id))
    store.audit("ROTATE_GATEWAY_REGISTRATION_TOKEN","gateway",gateway_id,{"identity_method":"G781_LINK_REGISTRATION"},actor)
    packet=f"FIELDLINE/1 {gateway_id} {token}\r\n"
    packet_hex=packet.encode().hex()
    return {"gateway_id":gateway_id,"registration_packet":packet,"token_hex":packet_hex,"send_mode":"LINK",
            "configuration_commands":["AT+REGEN=ON","AT+REGTP=USER",f"AT+REGDT={packet_hex}","AT+REGSND=LINK"],
            "secret_returned_once":True,"tls_or_trusted_vpn_required":True}


@app.patch("/api/gateways/{gateway_id}")
async def patch_gateway(gateway_id: str, data: GatewayPatch, actor=Depends(authorize)):
    existing=store.one("SELECT * FROM gateways WHERE id=?",(gateway_id,))
    if not existing: raise HTTPException(404,"Gateway not found")
    changes=data.model_dump(exclude_unset=True)
    if "transport_mode" in changes and changes["transport_mode"] != existing["transport_mode"]:
        if sessions.for_gateway(gateway_id):
            raise HTTPException(409,"Disconnect the commissioned session before changing transport framing")
        assigned = store.rows("SELECT id,node_type FROM nodes WHERE gateway_id=?", (gateway_id,))
        expected = {"ANALOG_INPUT": "MODBUS_TCP", "ANALOG_OUTPUT": "TRANSPARENT_SERIAL"}
        incompatible = [node["id"] for node in assigned if expected.get(node["node_type"]) != changes["transport_mode"]]
        if incompatible:
            raise HTTPException(409,"Transport cannot change while assigned ME31 nodes require the current framing; reassign those nodes first")
        dependent = store.one("SELECT COUNT(*) count FROM mappings p JOIN nodes s ON s.id=p.source_node_id JOIN nodes d ON d.id=p.destination_node_id WHERE p.enabled=1 AND (s.gateway_id=? OR d.gateway_id=?)", (gateway_id,gateway_id))
        if dependent and dependent["count"]:
            raise HTTPException(409,"Disable dependent pipelines before changing gateway transport")
    if changes:
        store.run(f"UPDATE gateways SET {','.join(key+'=?' for key in changes)},updated_at=? WHERE id=?",(*changes.values(),utc_now(),gateway_id))
        store.audit("UPDATE","gateway",gateway_id,changes,actor)
    return _gateway_public(store.one("SELECT * FROM gateways WHERE id=?",(gateway_id,)))


@app.post("/api/gateways/{gateway_id}/disconnect")
async def disconnect_gateway(gateway_id: str, actor=Depends(authorize)):
    gateway = store.one("SELECT id FROM gateways WHERE id=?", (gateway_id,))
    if not gateway:
        raise HTTPException(404, "Gateway not found")
    session = sessions.for_gateway(gateway_id)
    if not session:
        raise HTTPException(409, "Gateway has no active commissioned session")
    async with sessions.lock:
        if sessions.by_gateway.get(gateway_id) == session.session_id:
            sessions.by_gateway.pop(gateway_id, None)
        await session.retire()
        store.run("UPDATE gateways SET last_disconnected_at=?,last_disconnect_reason=?,updated_at=? WHERE id=?", (utc_now(),"Operator initiated disconnect",utc_now(),gateway_id))
    await pipelines.gateway_disconnected(gateway_id, "Operator initiated disconnect", operator_initiated=True)
    store.event("OPERATOR_DISCONNECT", "Gateway session intentionally disconnected by an administrator", gateway_id=gateway_id, protocol=session.transport_mode)
    store.audit("DISCONNECT_SESSION", "gateway", gateway_id, {"session_id": session.session_id}, actor)
    return {"gateway_id": gateway_id, "session_id": session.session_id, "state": "DISCONNECTED"}


@app.delete("/api/gateways/{gateway_id}", status_code=204)
async def delete_gateway(gateway_id: str, actor=Depends(authorize)):
    gateway = store.one("SELECT * FROM gateways WHERE id=?", (gateway_id,))
    if not gateway:
        raise HTTPException(404, "Gateway not found")
    nodes = store.rows("SELECT id,name,node_type FROM nodes WHERE gateway_id=? ORDER BY name", (gateway_id,))
    pipelines_for_gateway = store.rows("""SELECT DISTINCT p.id,p.name,p.enabled,p.armed FROM mappings p
        JOIN nodes s ON s.id=p.source_node_id OR s.id=p.destination_node_id
        WHERE s.gateway_id=? ORDER BY p.name""", (gateway_id,))
    active = [pipeline for pipeline in pipelines_for_gateway if pipeline["enabled"] or pipeline["armed"]]
    if nodes or active:
        node_names = ", ".join(node["name"] for node in nodes) or "none"
        pipeline_names = ", ".join(pipeline["name"] for pipeline in pipelines_for_gateway) or "none"
        raise HTTPException(409, f"Gateway has dependent nodes ({node_names}) and pipelines ({pipeline_names}). Disable/delete affected pipelines, then reassign or remove the nodes before deleting this gateway.")
    session = sessions.for_gateway(gateway_id)
    if session:
        async with sessions.lock:
            sessions.by_gateway.pop(gateway_id, None)
            await session.retire()
    with store.transaction() as db:
        # Communication and audit history intentionally have no cascading FK.
        db.execute("DELETE FROM gateways WHERE id=?", (gateway_id,))
        db.execute("INSERT INTO audit_log(at,actor,action,entity_type,entity_id,detail_json) VALUES(?,?,?,?,?,?)",
                   (utc_now(),actor,"DELETE","gateway",gateway_id,json.dumps({"name":gateway["name"],"registration_revoked":True},sort_keys=True)))
    store.event("GATEWAY_DELETED", f"Gateway {gateway['name']} deleted; registration credentials revoked", gateway_id=gateway_id)


@app.get("/api/gateways/sessions")
async def gateway_sessions(_: str = Depends(authorize)):
    return [{"session_id":s.session_id,"peer":s.peer,"gateway_id":s.gateway_id,"transport_mode":s.transport_mode,"connected":s.connected,"commissioned":s.commissioned,"identity_verified":s.identity_verified,"commissioning_method":s.commissioning_method,"state":s.state,"last_activity":s.last_activity,"request_count":s.request_count} for s in sessions.sessions.values() if not s.closed]


@app.post("/api/gateways/sessions/{session_id}/assign/{gateway_id}")
async def assign_session(session_id: str, gateway_id: str, actor=Depends(authorize)):
    session = sessions.sessions.get(session_id)
    gateway = store.one("SELECT * FROM gateways WHERE id=?", (gateway_id,))
    if not session or session.closed:
        raise HTTPException(404, "Pending TCP session not found")
    if session.commissioned:
        raise HTTPException(409,"TCP session is already commissioned")
    if not gateway:
        raise HTTPException(404, "Gateway not found")
    await sessions.assign(session,gateway)
    store.audit("ASSIGN_SESSION","gateway",gateway_id,{"session_id":session_id,"peer":session.peer},actor)
    return {"gateway_id":gateway_id,"session_id":session_id,"transport_mode":session.transport_mode,"peer":session.peer}


@app.get("/api/nodes")
async def list_nodes(type: str | None = Query(default=None), search: str | None = None, _: str = Depends(authorize)):
    nodes = store.rows("SELECT * FROM nodes ORDER BY name")
    connected = {gateway_id for gateway_id in sessions.by_gateway if sessions.for_gateway(gateway_id)}
    for node in nodes:
        node["enabled_channel_count"] = store.one("SELECT COUNT(*) count FROM channels WHERE node_id=? AND enabled=1",(node["id"],))["count"]
        latest=store.one("SELECT MAX(measured_at) at FROM measurements WHERE node_id=?",(node["id"],))
        node["last_measurement_at"]=latest["at"]
        last_modbus=store.one("SELECT MAX(at) at FROM events WHERE node_id=? AND result IN ('OUTPUT_RANGE_READ','OUTPUT_SETPOINT_READ','OUTPUT_RANGE_CHECK','OUTPUT_READBACK','OUTPUT_WRITE_ACKNOWLEDGED')",(node["id"],)) if node["node_type"]=="ANALOG_OUTPUT" else {"at":None}
        node["last_modbus_at"]=last_modbus["at"]
        node["gateway_connected"]=node["gateway_id"] in connected
        node["gateway_state"]="CONNECTED" if node["gateway_connected"] else "DISCONNECTED" if node["gateway_id"] else "UNKNOWN"
        node["modbus_health"]=_node_modbus_health(node,store.rows("SELECT * FROM polling_items WHERE node_id=?",(node["id"],)),node["gateway_connected"])
        node["modbus_healthy"]=node["modbus_health"]=="RESPONDING"
    if type:
        nodes = [node for node in nodes if node["node_type"] == type]
    if search:
        nodes = [node for node in nodes if search.casefold() in (node["name"] + node["hardware_model"] + node["id"]).casefold()]
    return nodes


@app.post("/api/nodes", status_code=201)
async def create_node(data: NodeCreate, actor=Depends(authorize)):
    profile_id, hardware_model, expected_transport = (
        ("ebyte-me31-xaxx0600-v1.1", "EBYTE ME31-XAXX0600", "MODBUS_TCP")
        if data.node_type == "ANALOG_INPUT" else
        ("ebyte-me31-xxxa0006-v1.1", "EBYTE ME31-XXXA0006", "TRANSPARENT_SERIAL")
    )
    if data.profile_id not in (None, profile_id):
        raise HTTPException(422,"Node type is fixed to its corresponding ME31 device profile")
    if data.hardware_model and data.hardware_model.strip().upper().replace("EBYTE ", "") != hardware_model.removeprefix("EBYTE "):
        raise HTTPException(422,"Hardware model is fixed by the selected ME31 node type")
    gateway = store.one("SELECT id,transport_mode FROM gateways WHERE id=?",(data.gateway_id,)) if data.gateway_id else None
    if data.gateway_id and not gateway:
        raise HTTPException(422,"Assigned gateway does not exist")
    if gateway and gateway["transport_mode"] != expected_transport:
        raise HTTPException(422,f"{hardware_model} requires a {expected_transport} gateway")
    node_id, now = data.id or str(uuid4()), utc_now()
    try:
        with store.transaction() as db:
            db.execute("INSERT INTO nodes(id,name,node_type,hardware_model,gateway_id,slave_id,profile_id,polling_enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",(node_id,data.name,data.node_type,hardware_model,data.gateway_id,data.slave_id,profile_id,int(data.polling_enabled),now,now))
            prefix, channel_type = ("AI","ANALOG_INPUT") if data.node_type == "ANALOG_INPUT" else ("AO","ANALOG_OUTPUT")
            for channel in range(1,7):
                db.execute("INSERT INTO channels(id,node_id,channel_index,channel_type,label,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                    (str(uuid4()),node_id,channel,channel_type,f"{prefix}{channel}",now,now))
    except Exception as exc:
        raise HTTPException(409,"Node ID already exists or configuration conflicts with the fixed six-channel ME31 profile") from exc
    store.audit("CREATE","node",node_id,{**data.model_dump(),"hardware_model":hardware_model,"profile_id":profile_id},actor)
    return store.one("SELECT * FROM nodes WHERE id=?",(node_id,))


@app.get("/api/nodes/{node_id}")
async def get_node(node_id: str, _: str = Depends(authorize)):
    node = store.one("SELECT * FROM nodes WHERE id=?",(node_id,))
    if not node:
        raise HTTPException(404,"Node not found")
    node["polling_items"] = store.rows("SELECT * FROM polling_items WHERE node_id=? ORDER BY register_name",(node_id,))
    node["latest_measurements"] = store.rows("SELECT * FROM measurements WHERE node_id=? ORDER BY channel",(node_id,))
    return node


@app.get("/api/nodes/{node_id}/channels")
async def get_channels(node_id: str, _: str = Depends(authorize)):
    if not store.one("SELECT id FROM nodes WHERE id=?",(node_id,)):
        raise HTTPException(404,"Node not found")
    return store.rows("SELECT * FROM channels WHERE node_id=? ORDER BY channel_type,channel_index",(node_id,))


@app.post("/api/nodes/{node_id}/channels",status_code=201)
async def create_output_channel(node_id: str, data: ChannelCreate, actor=Depends(authorize)):
    node=store.one("SELECT id,node_type FROM nodes WHERE id=?",(node_id,))
    if not node: raise HTTPException(404,"Node not found")
    if node["node_type"]!="ANALOG_OUTPUT": raise HTTPException(422,"Input channels are defined by the input device profile")
    raise HTTPException(409,"ME31-XXXA0006 nodes always contain six provisioned AO channels; edit an existing channel label instead")


@app.patch("/api/channels/{channel_id}")
async def patch_channel(channel_id: str, changes: dict, actor=Depends(authorize)):
    channel=store.one("SELECT * FROM channels WHERE id=?",(channel_id,))
    if not channel: raise HTTPException(404,"Channel not found")
    if set(changes)-{"label","enabled","config"}: raise HTTPException(422,"Unsupported channel fields")
    if "label" in changes and (not isinstance(changes["label"],str) or not changes["label"].strip() or len(changes["label"])>80): raise HTTPException(422,"Channel label must be 1–80 characters")
    if "config" in changes and not isinstance(changes["config"],dict): raise HTTPException(422,"Channel config must be an object")
    if "enabled" in changes and not isinstance(changes["enabled"],bool): raise HTTPException(422,"Channel enabled state must be boolean")
    if changes.get("enabled") is False:
        refs=store.one("SELECT COUNT(*) count FROM mappings WHERE enabled=1 AND ((source_node_id=? AND source_channel=?) OR (destination_node_id=? AND destination_channel=?))",(channel["node_id"],channel["channel_index"],channel["node_id"],channel["channel_index"]))
        if refs and refs["count"]: raise HTTPException(409,"Disable dependent pipelines before disabling this channel")
    sets=[];values=[]
    if "label" in changes: sets.append("label=?");values.append(changes["label"].strip())
    if "enabled" in changes: sets.append("enabled=?");values.append(int(bool(changes["enabled"])))
    if "config" in changes: sets.append("config_json=?");values.append(json.dumps(changes["config"],sort_keys=True))
    if sets:
        values.extend([utc_now(),channel_id]);store.run(f"UPDATE channels SET {','.join(sets)},updated_at=? WHERE id=?",values)
        store.audit("UPDATE","channel",channel_id,changes,actor)
    return store.one("SELECT * FROM channels WHERE id=?",(channel_id,))


@app.patch("/api/nodes/{node_id}")
async def patch_node(node_id: str, changes: dict, actor=Depends(authorize)):
    existing = store.one("SELECT * FROM nodes WHERE id=?",(node_id,))
    if not existing:
        raise HTTPException(404,"Node not found")
    allowed = {"name","hardware_model","gateway_id","slave_id","profile_id","polling_enabled"}
    if set(changes)-allowed:
        raise HTTPException(422,"Unsupported node fields")
    if "name" in changes and (not isinstance(changes["name"],str) or not changes["name"].strip() or len(changes["name"])>100):
        raise HTTPException(422,"Node name must be 1–100 characters")
    if "hardware_model" in changes and (not isinstance(changes["hardware_model"],str) or len(changes["hardware_model"])>100):
        raise HTTPException(422,"Hardware model must be 100 characters or fewer")
    if "gateway_id" in changes and changes["gateway_id"] is not None and not isinstance(changes["gateway_id"],str):
        raise HTTPException(422,"Gateway ID must be text or null")
    fixed_profile, fixed_model, expected_transport = ("ebyte-me31-xaxx0600-v1.1","EBYTE ME31-XAXX0600","MODBUS_TCP") if existing["node_type"]=="ANALOG_INPUT" else ("ebyte-me31-xxxa0006-v1.1","EBYTE ME31-XXXA0006","TRANSPARENT_SERIAL")
    if "profile_id" in changes and changes["profile_id"] != fixed_profile:
        raise HTTPException(422,"ME31 device profile cannot be changed")
    if "hardware_model" in changes and changes["hardware_model"].strip().upper().replace("EBYTE ", "") != fixed_model.removeprefix("EBYTE "):
        raise HTTPException(422,"ME31 hardware model cannot be changed")
    if "polling_enabled" in changes and not isinstance(changes["polling_enabled"],bool):
        raise HTTPException(422,"polling_enabled must be boolean")
    if "gateway_id" in changes and changes["gateway_id"] and not store.one("SELECT id FROM gateways WHERE id=?",(changes["gateway_id"],)):
        raise HTTPException(422,"Assigned gateway does not exist")
    if "gateway_id" in changes and changes["gateway_id"]:
        assigned = store.one("SELECT transport_mode FROM gateways WHERE id=?",(changes["gateway_id"],))
        if assigned["transport_mode"] != expected_transport:
            raise HTTPException(422,f"This ME31 model requires a {expected_transport} gateway")
    if "slave_id" in changes and (not isinstance(changes["slave_id"],int) or isinstance(changes["slave_id"],bool) or not 1 <= changes["slave_id"] <= 247):
        raise HTTPException(422,"Slave ID must be 1–247")
    if set(changes) & {"gateway_id","polling_enabled"}:
        dependencies=store.one("SELECT COUNT(*) count FROM mappings WHERE enabled=1 AND (source_node_id=? OR destination_node_id=?)",(node_id,node_id))
        if dependencies and dependencies["count"]:
            raise HTTPException(409,"Disable dependent pipelines before reassigning or disabling this node")
    normalized = {key:(int(value) if key=="polling_enabled" else value.strip() if key in {"name","hardware_model"} and isinstance(value,str) else value) for key,value in changes.items()}
    normalized["profile_id"] = fixed_profile
    normalized["hardware_model"] = fixed_model
    if normalized:
        store.run(f"UPDATE nodes SET {','.join(key+'=?' for key in normalized)},updated_at=? WHERE id=?",(*normalized.values(),utc_now(),node_id))
        store.audit("UPDATE","node",node_id,changes,actor)
        if set(changes)&{"polling_enabled","gateway_id","slave_id"}:
            scheduler.cancel_node(node_id)
    return store.one("SELECT * FROM nodes WHERE id=?",(node_id,))


@app.get("/api/device-profiles")
async def profiles(_: str = Depends(authorize)):
    return PROFILES


@app.get("/api/nodes/{node_id}/polling")
async def get_polling(node_id: str, _: str = Depends(authorize)):
    if not store.one("SELECT id FROM nodes WHERE id=?",(node_id,)):
        raise HTTPException(404,"Node not found")
    return store.rows("SELECT * FROM polling_items WHERE node_id=? ORDER BY register_name",(node_id,))


@app.put("/api/nodes/{node_id}/polling/groups/{category}")
async def toggle_polling_group(node_id: str, category: str, enabled: bool = Query(...), actor=Depends(authorize)):
    if not store.one("SELECT id FROM nodes WHERE id=?",(node_id,)):
        raise HTTPException(404,"Node not found")
    definitions={item["name"]:item["category"] for profile in PROFILES for item in profile["registers"]}
    selected=[name for name,group in definitions.items() if group==category]
    if not selected and category != "Custom Registers":
        raise HTTPException(404,"Polling group not found")
    if category == "Custom Registers":
        ids=store.rows("SELECT id FROM polling_items WHERE node_id=? AND profile_id IS NULL",(node_id,))
        store.run("UPDATE polling_items SET enabled=?,updated_at=? WHERE node_id=? AND profile_id IS NULL",(int(enabled),utc_now(),node_id))
        for item in ids:
            if enabled: scheduler.due[item["id"]]=0
            else: scheduler.cancel_item(item["id"])
        store.audit("TOGGLE_POLLING_GROUP","node",node_id,{"group":category,"enabled":enabled,"items":[item["id"] for item in ids]},actor)
        return {"node_id":node_id,"group":category,"enabled":enabled,"updated_items":len(ids)}
    marks=','.join('?' for _ in selected)
    ids=store.rows(f"SELECT id FROM polling_items WHERE node_id=? AND register_name IN ({marks})",(node_id,*selected))
    if enabled:
        unverified=store.one(f"SELECT COUNT(*) count FROM polling_items WHERE node_id=? AND register_name IN ({marks}) AND data_type LIKE 'float%' AND float_verified=0",(node_id,*selected))
        if unverified and unverified["count"]:
            raise HTTPException(409,"Verify the ME31 float decoder before enabling this polling group")
    store.run(f"UPDATE polling_items SET enabled=?,updated_at=? WHERE node_id=? AND register_name IN ({marks})",(int(enabled),utc_now(),node_id,*selected))
    for item in ids:
        if enabled: scheduler.due[item["id"]]=0
        else: scheduler.cancel_item(item["id"])
    store.audit("TOGGLE_POLLING_GROUP","node",node_id,{"group":category,"enabled":enabled,"items":[item["id"] for item in ids]},actor)
    return {"node_id":node_id,"group":category,"enabled":enabled,"updated_items":len(ids)}


@app.post("/api/nodes/{node_id}/polling", status_code=201)
async def add_polling(node_id: str, data: PollItemCreate, actor=Depends(authorize)):
    node=store.one("SELECT * FROM nodes WHERE id=?",(node_id,))
    if not node:
        raise HTTPException(404,"Node not found")
    if node["node_type"] != "ANALOG_INPUT":
        raise HTTPException(422,"Output-node polling requires a verified device profile")
    if data.profile_id:
        if data.profile_id != node["profile_id"]:
            raise HTTPException(422,"Polling profile must match the node's fixed ME31 hardware profile")
        definition=register(data.profile_id,data.register_name)
        if not definition or not definition.get("readable") or definition.get("restricted_action"):
            raise HTTPException(422,"Register profile entry is unavailable for polling")
        if definition.get("admin_only") and data.enabled:
            raise HTTPException(422,"Administrator-only profile items must be added disabled")
        if definition["function_code"] != data.function_code or definition["address"] != data.address or definition.get("quantity") != data.quantity:
            raise HTTPException(422,"Polling settings do not match the selected profile definition")
        if definition.get("data_type") != data.data_type:
            raise HTTPException(422,"Polling data type does not match the selected profile definition")
        if data.data_type.startswith("float") and definition.get("quantity") != data.quantity:
            raise HTTPException(422,"Float polling quantity must match the register profile")
    if data.data_type.startswith("float"):
        if data.quantity % 2:
            raise HTTPException(422,"Float32 polling quantity must be even")
        if data.enabled:
            raise HTTPException(422,"Float polling must be verified against a known reference before it is enabled")
        if data.word_order not in {"BIG_WORD_FIRST", "LITTLE_WORD_FIRST", "unverified"}:
            raise HTTPException(422,"Unsupported float word order")
    item_id,now=data.id or str(uuid4()),utc_now()
    measurement_metadata=definition.get("measurement",{}) if data.profile_id else {}
    store.run("INSERT INTO polling_items(id,node_id,profile_id,register_name,function_code,address,quantity,data_type,byte_order,word_order,interval_ms,timeout_ms,retries,scale,unit,measurement_metadata_json,float_verified,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(item_id,node_id,data.profile_id,data.register_name,data.function_code,data.address,data.quantity,data.data_type,data.byte_order,data.word_order,data.interval_ms,data.timeout_ms,data.retries,data.scale,data.unit,json.dumps(measurement_metadata),0,int(data.enabled),now,now))
    store.audit("CREATE","polling_item",item_id,data.model_dump(),actor)
    return store.one("SELECT * FROM polling_items WHERE id=?",(item_id,))


@app.patch("/api/polling/{item_id}")
async def patch_polling(item_id: str, changes: dict, actor=Depends(authorize)):
    item=store.one("SELECT * FROM polling_items WHERE id=?",(item_id,))
    if not item:
        raise HTTPException(404,"Polling item not found")
    allowed={"enabled","interval_ms","timeout_ms","retries","scale","unit","byte_order","word_order"}
    if set(changes)-allowed:
        raise HTTPException(422,"Polling register identity cannot be changed in-place")
    if "enabled" in changes and not isinstance(changes["enabled"],bool): raise HTTPException(422,"enabled must be boolean")
    if "byte_order" in changes and changes["byte_order"] not in {"big","little"}: raise HTTPException(422,"Byte order must be big or little")
    if "word_order" in changes and changes["word_order"] not in {"BIG_WORD_FIRST","LITTLE_WORD_FIRST","unverified"}: raise HTTPException(422,"Unsupported word order")
    if ("byte_order" in changes or "word_order" in changes) and item["data_type"].startswith("float"):
        changes["float_verified"] = False
        changes["enabled"] = False
    if changes.get("enabled") and item["data_type"].startswith("float"):
        if changes.get("word_order",item["word_order"]) == "unverified" or not item["float_verified"]:
            raise HTTPException(422,"Verify float byte and word order against a known reference before enabling polling")
    if "enabled" in changes and changes["enabled"] and item["profile_id"]:
        definition=register(item["profile_id"],item["register_name"])
        if definition and definition.get("admin_only") and actor!="admin": raise HTTPException(403,"Administrator authorization required")
    if "interval_ms" in changes and (not isinstance(changes["interval_ms"],int) or isinstance(changes["interval_ms"],bool) or not 250 <= changes["interval_ms"] <= 86400000): raise HTTPException(422,"Interval must be 250 ms–24 h")
    if "timeout_ms" in changes and (not isinstance(changes["timeout_ms"],int) or isinstance(changes["timeout_ms"],bool) or not 100 <= changes["timeout_ms"] <= 60000): raise HTTPException(422,"Timeout must be 100–60000 ms")
    if "retries" in changes and (not isinstance(changes["retries"],int) or isinstance(changes["retries"],bool) or not 0 <= changes["retries"] <= 5): raise HTTPException(422,"Retries must be 0–5")
    if "scale" in changes and (not isinstance(changes["scale"],(int,float)) or isinstance(changes["scale"],bool) or not math.isfinite(changes["scale"])): raise HTTPException(422,"Scale must be a finite number")
    if "unit" in changes and (not isinstance(changes["unit"],str) or len(changes["unit"])>32): raise HTTPException(422,"Unit must be 32 characters or fewer")
    normalized={key:(int(value) if key in {"enabled","float_verified"} else value) for key,value in changes.items()}
    if normalized:
        store.run(f"UPDATE polling_items SET {','.join(key+'=?' for key in normalized)},updated_at=? WHERE id=?",(*normalized.values(),utc_now(),item_id))
        store.audit("UPDATE","polling_item",item_id,changes,actor)
        if changes.get("enabled") is False:
            scheduler.cancel_item(item_id)
        elif changes.get("enabled") is True:
            scheduler.due[item_id]=0
    return store.one("SELECT * FROM polling_items WHERE id=?",(item_id,))


@app.delete("/api/polling/{item_id}", status_code=204)
async def delete_polling(item_id: str, actor=Depends(authorize)):
    item=store.one("SELECT * FROM polling_items WHERE id=?",(item_id,))
    if not item: raise HTTPException(404,"Polling item not found")
    scheduler.cancel_item(item_id)
    store.run("DELETE FROM polling_items WHERE id=?",(item_id,));store.audit("DELETE","polling_item",item_id,{},actor)


@app.post("/api/polling/{item_id}/read")
async def read_once(item_id: str, _: str = Depends(authorize)):
    item=store.one("SELECT p.*,n.gateway_id,n.slave_id,n.polling_enabled FROM polling_items p JOIN nodes n ON n.id=p.node_id WHERE p.id=?",(item_id,))
    if not item: raise HTTPException(404,"Polling item not found")
    if not item["polling_enabled"]: raise HTTPException(409,"Node polling is disabled")
    await scheduler.read_once(item)
    return store.one("SELECT * FROM polling_items WHERE id=?",(item_id,))


@app.post("/api/polling/{item_id}/verify-float")
async def verify_float_order(item_id: str, data: FloatVerificationInput, actor=Depends(authorize)):
    item=store.one("SELECT p.*,n.gateway_id,n.slave_id FROM polling_items p JOIN nodes n ON n.id=p.node_id WHERE p.id=?",(item_id,))
    if not item: raise HTTPException(404,"Polling item not found")
    if item["data_type"]!="float32_array" or item["quantity"]!=12 or item["function_code"]!=4:
        raise HTTPException(422,"Reference verification requires the six-channel ME31 Float32 input block")
    if item["enabled"]: raise HTTPException(409,"Disable float polling before changing verified decoding settings")
    session=sessions.for_gateway(item["gateway_id"])
    if session is None: raise HTTPException(409,"No commissioned gateway session is available")
    try:
        registers,frame,request,latency=await session.read_registers(item["slave_id"],4,item["address"],12,item["timeout_ms"])
        decoded=decode_registers(registers,"float32_array",byte_order=data.byte_order,word_order=data.word_order)
    except Exception as exc:
        store.event("FLOAT_VERIFICATION_ERROR",str(exc),gateway_id=item["gateway_id"],node_id=item["node_id"],direction="RX",protocol=session.transport_mode,function_code=4,address=item["address"],quantity=12)
        raise HTTPException(502,f"Float verification read failed: {exc}") from exc
    observed=decoded[data.channel-1]*item["scale"]
    packet=json.dumps([value*item["scale"] for value in decoded],allow_nan=False)
    now=utc_now()
    details={"channel":data.channel,"reference_ma":data.reference_ma,"observed_ma":observed,"tolerance_ma":data.tolerance_ma,"byte_order":data.byte_order,"word_order":data.word_order}
    if abs(observed-data.reference_ma)>data.tolerance_ma:
        store.event("FLOAT_VERIFICATION_FAILED",f"AI{data.channel} decoded {observed:.6g} mA; reference {data.reference_ma:.6g} mA",gateway_id=item["gateway_id"],node_id=item["node_id"],direction="RX",protocol=session.transport_mode,function_code=4,address=item["address"],quantity=12,latency_ms=latency,request_hex=request.hex(" "),response_hex=frame.hex(" "))
        return {"verified":False,**details}
    store.run("UPDATE polling_items SET byte_order=?,word_order=?,float_verified=1,last_value=?,last_success_at=?,last_latency_ms=?,last_status='GOOD',updated_at=? WHERE id=?",(data.byte_order,data.word_order,packet,now,latency,now,item_id))
    store.event("FLOAT_VERIFIED",f"AI{data.channel} matched known reference",gateway_id=item["gateway_id"],node_id=item["node_id"],direction="RX",protocol=session.transport_mode,function_code=4,address=item["address"],quantity=12,latency_ms=latency,request_hex=request.hex(" "),response_hex=frame.hex(" "))
    store.audit("VERIFY_FLOAT_ORDER","polling_item",item_id,details,actor)
    return {"verified":True,**details,"byte_order":data.byte_order,"word_order":data.word_order}


@app.post("/api/calibration/preview")
async def calibration_preview(data: PreviewInput, _: str = Depends(authorize)):
    from .calibration import Calibration, Scaling, calibrate, scale
    corrected=calibrate(data.raw_ma,Calibration(data.gain,data.offset_ma,data.enabled))
    engineering,quality=None,"GOOD"
    if data.scaling:
        try: engineering,quality=scale(corrected,Scaling(**data.scaling))
        except (TypeError,ValueError) as exc: raise HTTPException(422,str(exc))
    return {"raw_ma":data.raw_ma,"calibrated_ma":corrected,"engineering_value":engineering,"quality":quality,"preview":True}


@app.get("/api/nodes/{node_id}/calibration")
async def get_calibration(node_id: str, channel: int | None = None, _: str = Depends(authorize)):
    if not store.one("SELECT id FROM nodes WHERE id=? AND node_type='ANALOG_INPUT'",(node_id,)): raise HTTPException(404,"Analog input node not found")
    query="SELECT * FROM calibrations WHERE node_id=?";params=[node_id]
    if channel is not None: query+=" AND channel=?";params.append(channel)
    return store.rows(query+" ORDER BY channel,revision DESC",params)


@app.post("/api/nodes/{node_id}/calibration/{channel}/capture")
async def capture_calibration_sample(node_id: str, channel: int, data: CalibrationCaptureInput, _: str = Depends(authorize)):
    node=store.one("SELECT * FROM nodes WHERE id=? AND node_type='ANALOG_INPUT'",(node_id,))
    if not node: raise HTTPException(404,"Analog input node not found")
    if channel not in range(1,7): raise HTTPException(422,"Channel must be AI1–AI6")
    latest=store.one("SELECT * FROM measurements WHERE node_id=? AND channel=? ORDER BY id DESC LIMIT 1",(node_id,channel))
    items=store.rows("SELECT * FROM polling_items WHERE node_id=?",(node_id,))
    channels=store.rows("SELECT * FROM channels WHERE node_id=?",(node_id,))
    quality=_measurement_quality(node,channel,latest,items,channels)
    if quality!="GOOD": raise HTTPException(409,f"Cannot capture {quality.lower()} measurement")
    samples=store.rows("SELECT raw_ma,measured_at FROM measurements WHERE node_id=? AND channel=? ORDER BY id DESC LIMIT 3",(node_id,channel))
    if len(samples)<3: raise HTTPException(409,"Capture requires three recent valid samples")
    try:
        values=[float(sample["raw_ma"]) for sample in samples]
        times=[datetime.fromisoformat(sample["measured_at"]) for sample in samples]
        now=datetime.now(timezone.utc)
        if any(timestamp.tzinfo is None for timestamp in times): times=[timestamp.replace(tzinfo=timezone.utc) if timestamp.tzinfo is None else timestamp for timestamp in times]
        sample_age=(now-min(times)).total_seconds()
        item=max((item for item in items if item["enabled"] and channel in json.loads(item["measurement_metadata_json"] or "{}").get("channel_indices",[])),key=lambda row:row.get("last_success_at") or "",default=None)
        if not item or sample_age>_item_freshness_seconds(item): raise HTTPException(409,"Channel samples are stale or polling is disabled")
        if (max(times)-min(times)).total_seconds()>(int(item["interval_ms"])*2+int(item["timeout_ms"]))/1000:
            raise HTTPException(409,"The three samples are not consecutive recent polling results")
    except (TypeError,ValueError,json.JSONDecodeError):
        raise HTTPException(409,"Channel samples are invalid")
    if not all(math.isfinite(value) for value in values): raise HTTPException(409,"Channel samples contain invalid values")
    spread=max(values)-min(values)
    if spread>data.tolerance_ma: raise HTTPException(409,f"Channel is not stable: sample spread {spread:.4f} mA exceeds {data.tolerance_ma:.4f} mA")
    captured=sorted(values)[1]
    return {"node_id":node_id,"channel":channel,"raw_ma":captured,"samples":len(values),"spread_ma":spread,"quality":"GOOD","captured_at":latest["measured_at"]}


@app.put("/api/nodes/{node_id}/calibration")
async def save_calibration(node_id: str, data: CalibrationInput, actor=Depends(authorize)):
    if not store.one("SELECT id FROM nodes WHERE id=? AND node_type='ANALOG_INPUT'",(node_id,)): raise HTTPException(404,"Analog input node not found")
    if data.scaling:
        from .calibration import Scaling, scale
        try: scale(0,Scaling(**data.scaling))
        except (TypeError,ValueError) as exc: raise HTTPException(422,str(exc))
    with store.transaction() as db:
        current=db.execute("SELECT MAX(revision) value FROM calibrations WHERE node_id=? AND channel=?",(node_id,data.channel)).fetchone()["value"] or 0
        revision=current+1
        now=utc_now()
        db.execute("INSERT INTO calibrations(node_id,channel,revision,gain,offset_ma,enabled,scaling_json,method,modified_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",(node_id,data.channel,revision,data.gain,data.offset_ma,int(data.enabled),json.dumps(data.scaling) if data.scaling else None,data.method,actor,now))
        db.execute("INSERT INTO audit_log(at,actor,action,entity_type,entity_id,detail_json) VALUES(?,?,?,?,?,?)",(now,actor,"CALIBRATION_CHANGED","channel",f"{node_id}:{data.channel}",json.dumps(data.model_dump(),sort_keys=True)))
    return store.one("SELECT * FROM calibrations WHERE node_id=? AND channel=? AND revision=?",(node_id,data.channel,revision))


@app.post("/api/nodes/{node_id}/calibration/two-point")
async def save_two_point(node_id: str, data: dict, actor=Depends(authorize)):
    if not store.one("SELECT id FROM nodes WHERE id=? AND node_type='ANALOG_INPUT'",(node_id,)): raise HTTPException(404,"Analog input node not found")
    try:
        from .calibration import two_point
        channel=int(data["channel"]); computed=two_point(float(data["raw1"]),float(data["reference1"]),float(data["raw2"]),float(data["reference2"]))
        if channel not in range(1,7): raise ValueError("channel must be 1–6")
    except (KeyError,TypeError,ValueError) as exc: raise HTTPException(422,str(exc))
    result=CalibrationInput(channel=channel,gain=computed.gain,offset_ma=computed.offset_ma,enabled=True,method="TWO_POINT",scaling=data.get("scaling"))
    return await save_calibration(node_id,result,actor)


@app.post("/api/nodes/{node_id}/calibration/{channel}/revert/{revision}")
async def revert_calibration(node_id: str, channel: int, revision: int, actor=Depends(authorize)):
    previous=store.one("SELECT * FROM calibrations WHERE node_id=? AND channel=? AND revision=?",(node_id,channel,revision))
    if not previous: raise HTTPException(404,"Calibration revision not found")
    data=CalibrationInput(channel=channel,gain=previous["gain"],offset_ma=previous["offset_ma"],enabled=bool(previous["enabled"]),method="REVERT",scaling=json.loads(previous["scaling_json"]) if previous["scaling_json"] else None)
    return await save_calibration(node_id,data,actor)


@app.get("/api/nodes/{node_id}/measurements/latest")
async def latest_measurements(node_id: str, _: str = Depends(authorize)):
    node=store.one("SELECT * FROM nodes WHERE id=?",(node_id,))
    if not node: raise HTTPException(404,"Node not found")
    values=store.rows("SELECT m.* FROM measurements m JOIN (SELECT channel,MAX(id) id FROM measurements WHERE node_id=? GROUP BY channel) latest ON latest.id=m.id ORDER BY m.channel",(node_id,))
    items=store.rows("SELECT * FROM polling_items WHERE node_id=?",(node_id,))
    channels=store.rows("SELECT * FROM channels WHERE node_id=?",(node_id,))
    for value in values:
        value["quality"]=_measurement_quality(node,value["channel"],value,items,channels)
    return values


@app.get("/api/nodes/{node_id}/measurements/history")
async def measurement_history(node_id: str, since: str | None = None, limit: int = Query(default=1000,ge=1,le=20000), _: str = Depends(authorize)):
    if not store.one("SELECT id FROM nodes WHERE id=?",(node_id,)): raise HTTPException(404,"Node not found")
    if since: return store.rows("SELECT * FROM measurements WHERE node_id=? AND measured_at>=? ORDER BY id DESC LIMIT ?",(node_id,since,limit))
    return store.rows("SELECT * FROM measurements WHERE node_id=? ORDER BY id DESC LIMIT ?",(node_id,limit))


@app.get("/api/logs")
async def logs(limit: int = Query(default=100,ge=1,le=1000), offset: int = Query(default=0,ge=0), result: str | None = None, _: str = Depends(authorize)):
    if result: return store.rows("SELECT * FROM events WHERE result=? ORDER BY id DESC LIMIT ? OFFSET ?",(result,limit,offset))
    return store.rows("SELECT * FROM events ORDER BY id DESC LIMIT ? OFFSET ?",(limit,offset))


@app.get("/api/nodes/{node_id}/outputs")
async def output_state(node_id: str, _: str = Depends(authorize)):
    node=store.one("SELECT * FROM nodes WHERE id=?",(node_id,))
    if not node: raise HTTPException(404,"Output node not found")
    if node["node_type"]!="ANALOG_OUTPUT": raise HTTPException(422,"Node is not an analog output")
    try:
        state=await output_driver.inspect(node_id)
    except OutputError as exc:
        raise HTTPException(422,str(exc)) from exc
    except Exception as exc:
        raise HTTPException(503,f"Output read failed: {exc}") from exc
    for channel in state["channels"]:
        recent=store.one("SELECT * FROM output_commands WHERE node_id=? AND channel=? ORDER BY id DESC LIMIT 1",(node_id,channel["channel"]))
        channel["last_write"] = recent
        channel["active_pipeline"] = store.one("SELECT id,name,status FROM mappings WHERE destination_node_id=? AND destination_channel=? AND enabled=1 AND armed=1",(node_id,channel["channel"]))
    return state


@app.post("/api/nodes/{node_id}/outputs/{channel}/set")
async def manual_output_set(node_id: str, channel: int, data: OutputSetpointInput, actor=Depends(authorize)):
    if not data.confirm:
        raise HTTPException(422,"Confirm the physical output test before sending an AO command")
    try:
        result=await output_driver.set_output(node_id,channel,data.target_ma,source=f"MANUAL:{actor}")
    except OutputError as exc:
        code=409 if "locked" in str(exc).lower() or "active pipeline" in str(exc).lower() else 422
        raise HTTPException(code,str(exc)) from exc
    except (ConnectionError,TimeoutError) as exc:
        raise HTTPException(503,str(exc)) from exc
    except Exception as exc:
        raise HTTPException(502,f"AO command did not complete and read back successfully: {exc}") from exc
    store.audit("MANUAL_OUTPUT_SET","node",node_id,{"channel":channel,"target_ma":data.target_ma,"acknowledged_ma":result["acknowledged_ma"],"operator_confirmation":True},actor)
    return result


@app.get("/api/pipelines")
async def list_pipelines(_: str = Depends(authorize)):
    return pipelines.list()


@app.post("/api/pipelines",status_code=201)
async def create_pipeline(data: PipelineCreate, actor=Depends(authorize)):
    try:
        result=pipelines.create(data.model_dump())
    except PipelineError as exc:
        raise HTTPException(422,str(exc)) from exc
    except Exception as exc:
        raise HTTPException(409,f"Pipeline could not be saved: {exc}") from exc
    store.audit("CREATE_PIPELINE","pipeline",result["id"],{"initial_state":"DISABLED_DISARMED"},actor)
    return result


@app.get("/api/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str, _: str = Depends(authorize)):
    result=pipelines.get(pipeline_id)
    if not result: raise HTTPException(404,"Pipeline not found")
    return result


@app.patch("/api/pipelines/{pipeline_id}")
async def patch_pipeline(pipeline_id: str, changes: dict, actor=Depends(authorize)):
    allowed={"name","source_node_id","source_channel","destination_node_id","destination_channel","source_value_type","config"}
    if set(changes)-allowed: raise HTTPException(422,"Only pipeline definition fields can be edited")
    try:
        result=pipelines.update(pipeline_id,changes)
    except KeyError as exc: raise HTTPException(404,str(exc)) from exc
    except PipelineError as exc: raise HTTPException(409,str(exc)) from exc
    except Exception as exc: raise HTTPException(422,f"Pipeline configuration is invalid: {exc}") from exc
    store.audit("UPDATE_PIPELINE","pipeline",pipeline_id,changes,actor)
    return result


@app.delete("/api/pipelines/{pipeline_id}",status_code=204)
async def delete_pipeline(pipeline_id: str, actor=Depends(authorize)):
    try: await pipelines.delete(pipeline_id)
    except KeyError as exc: raise HTTPException(404,str(exc)) from exc
    except PipelineError as exc: raise HTTPException(409,str(exc)) from exc
    store.audit("DELETE_PIPELINE","pipeline",pipeline_id,{},actor)


@app.post("/api/pipelines/{pipeline_id}/enable")
async def enable_pipeline(pipeline_id: str, data: PipelineConfirm, actor=Depends(authorize)):
    try: result=await pipelines.enable(pipeline_id,data.confirm)
    except KeyError as exc: raise HTTPException(404,str(exc)) from exc
    except PipelineError as exc: raise HTTPException(409,str(exc)) from exc
    store.audit("ENABLE_PIPELINE","pipeline",pipeline_id,{"confirmed":data.confirm},actor)
    return result


@app.post("/api/pipelines/{pipeline_id}/disable")
async def disable_pipeline(pipeline_id: str, actor=Depends(authorize)):
    try: result=await pipelines.disarm(pipeline_id,disable=True)
    except KeyError as exc: raise HTTPException(404,str(exc)) from exc
    store.audit("DISABLE_PIPELINE","pipeline",pipeline_id,{},actor)
    return result


@app.post("/api/pipelines/{pipeline_id}/arm")
async def arm_pipeline(pipeline_id: str, data: PipelineConfirm, actor=Depends(authorize)):
    try: result=await pipelines.arm(pipeline_id,data.confirm)
    except KeyError as exc: raise HTTPException(404,str(exc)) from exc
    except (PipelineError,OutputError) as exc: raise HTTPException(409,str(exc)) from exc
    except Exception as exc: raise HTTPException(503,f"Pipeline cannot be armed until destination verification succeeds: {exc}") from exc
    store.audit("ARM_PIPELINE","pipeline",pipeline_id,{"operator_confirmation":data.confirm},actor)
    return result


@app.post("/api/pipelines/{pipeline_id}/disarm")
async def disarm_pipeline(pipeline_id: str, actor=Depends(authorize)):
    try: result=await pipelines.disarm(pipeline_id)
    except KeyError as exc: raise HTTPException(404,str(exc)) from exc
    store.audit("DISARM_PIPELINE","pipeline",pipeline_id,{},actor)
    return result


@app.get("/api/pipelines/{pipeline_id}/status")
async def pipeline_status(pipeline_id: str, _: str = Depends(authorize)):
    result=pipelines.get(pipeline_id)
    if not result: raise HTTPException(404,"Pipeline not found")
    return result


@app.get("/api/pipelines/{pipeline_id}/history")
async def pipeline_history(pipeline_id: str, limit: int = Query(default=100,ge=1,le=500), _: str = Depends(authorize)):
    try: return pipelines.history(pipeline_id,limit)
    except KeyError as exc: raise HTTPException(404,str(exc)) from exc


@app.get("/api/mappings")
async def mappings(_: str = Depends(authorize)):
    # Compatibility shape for existing Mapping page/API clients.
    return {"state":"PIPELINE_ENGINE_AVAILABLE_OUTPUTS_REQUIRE_OPERATOR_ARMING","items":pipelines.list()}


@app.get("/api/settings")
async def settings(_: str = Depends(authorize)):
    return {"tcp_listener":{"host":os.getenv("TCP_LISTEN_HOST","0.0.0.0"),"port":int(os.getenv("TCP_LISTEN_PORT","5001"))},"polling":{"minimum_interval_ms":250,"gateway_minimum_request_interval_ms":int(os.getenv("MODBUS_MIN_REQUEST_INTERVAL_MS","50")),"default_timeout_ms":1500,"max_retries":5},"freshness_seconds":int(os.getenv("DEFAULT_STALE_AFTER_SECONDS","30")),"event_retention_days":int(os.getenv("EVENT_RETENTION_DAYS","90")),"measurement_retention_days":int(os.getenv("MEASUREMENT_RETENTION_DAYS","365")),"output_timeouts":{"range_ms":int(store.setting("output_range_timeout_ms","5000")),"write_ms":int(store.setting("output_write_timeout_ms","5000")),"readback_ms":int(store.setting("output_readback_timeout_ms","5000"))},"hardware_writes":"ME31_AO_ONLY_AUTHORIZED_MANUAL_OR_ARMED_PIPELINE"}


@app.put("/api/settings/output-timeouts")
async def update_output_timeouts(data: OutputTimeoutSettings, actor=Depends(authorize)):
    values = data.model_dump()
    with store.transaction() as db:
        for key, value in values.items():
            db.execute("INSERT INTO system_settings(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at", (key,str(value),utc_now()))
    store.audit("UPDATE", "system_settings", "output_timeouts", values, actor)
    return {"range_ms":values["output_range_timeout_ms"],"write_ms":values["output_write_timeout_ms"],"readback_ms":values["output_readback_timeout_ms"]}


@app.websocket("/ws/telemetry")
async def telemetry_socket(websocket: WebSocket):
    expected=os.getenv("ADMIN_API_TOKEN","")
    if not expected:
        await websocket.close(code=4401);return
    await websocket.accept()
    try:
        auth=await asyncio.wait_for(websocket.receive_json(),timeout=5)
        token=auth.get("token","") if isinstance(auth,dict) else ""
        if not isinstance(auth,dict) or auth.get("type")!="auth" or not isinstance(token,str) or not hmac.compare_digest(token,expected):
            await websocket.close(code=4401);return
        await websocket.send_json({"type":"authenticated","at":utc_now()})
        last_signature=None
        last_heartbeat=0.0
        while True:
            latest=store.rows("SELECT m.* FROM measurements m JOIN (SELECT node_id,channel,MAX(id) id FROM measurements GROUP BY node_id,channel) v ON v.id=m.id ORDER BY m.node_id,m.channel")
            nodes={row["id"]:row for row in store.rows("SELECT * FROM nodes WHERE node_type='ANALOG_INPUT'")}
            for measurement in latest:
                node=nodes.get(measurement["node_id"])
                if node:
                    items=store.rows("SELECT * FROM polling_items WHERE node_id=?",(node["id"],))
                    channels=store.rows("SELECT * FROM channels WHERE node_id=?",(node["id"],))
                    measurement["quality"]=_measurement_quality(node,measurement["channel"],measurement,items,channels)
            signature=tuple((row["id"],row["quality"]) for row in latest)
            monotonic=time.monotonic()
            if signature!=last_signature:
                await websocket.send_json({"type":"telemetry","at":utc_now(),"measurements":latest})
                last_signature=signature
                last_heartbeat=monotonic
            elif monotonic-last_heartbeat>=10:
                await websocket.send_json({"type":"heartbeat","at":utc_now()})
                last_heartbeat=monotonic
            await asyncio.sleep(1)
    except asyncio.TimeoutError:
        try: await websocket.close(code=4408)
        except RuntimeError: pass
    except (WebSocketDisconnect,RuntimeError,ValueError):
        return


web_dist = Path(__file__).resolve().parent.parent / "web" / "dist"
if web_dist.is_dir():
    app.mount("/", StaticFiles(directory=web_dist, html=True), name="web")
