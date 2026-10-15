"""FastAPI control plane and lifecycle for the Modbus monitoring service."""

import asyncio
import hmac
import json
import math
import os
import secrets
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
from .profiles import PROFILES, register
from .scheduler import PollScheduler
from .store import Store, utc_now


class SessionRegistry:
    def __init__(self, store):
        self.store = store
        self.sessions: dict[str, GatewaySession] = {}

    def for_gateway(self, gateway_id):
        return next((s for s in self.sessions.values() if s.gateway_id == gateway_id and not s.closed), None)


store = Store()
sessions = SessionRegistry(store)
scheduler = PollScheduler(store, sessions)
tcp_server = None


async def on_disconnect(session, error=None):
    if session.gateway_id:
        if error:
            store.run("UPDATE gateways SET last_disconnected_at=?,connection_errors=connection_errors+1 WHERE id=?", (utc_now(),session.gateway_id))
        else:
            store.run("UPDATE gateways SET last_disconnected_at=? WHERE id=?", (utc_now(),session.gateway_id))
        store.event("INVALID_RESPONSE" if error else "DISCONNECTED", error or "Gateway TCP session closed", gateway_id=session.gateway_id, protocol=session.transport_mode)


async def on_invalid_gateway_frame(session, detail):
    store.event("INVALID_RESPONSE", detail, gateway_id=session.gateway_id, protocol=session.transport_mode)


async def on_tcp_client(reader, writer):
    pending = sum(1 for session in sessions.sessions.values() if not session.gateway_id and not session.closed)
    maximum = int(os.getenv("TCP_MAX_CONNECTIONS", "128"))
    if len(sessions.sessions) >= maximum or pending >= int(os.getenv("TCP_MAX_PENDING_SESSIONS", "32")):
        writer.close()
        try: await writer.wait_closed()
        except (ConnectionError,OSError): pass
        return
    peer_info = writer.get_extra_info("peername")
    peer = f"{peer_info[0]}:{peer_info[1]}" if peer_info else "unknown"
    session = GatewaySession(str(uuid4()), reader, writer, peer)
    sessions.sessions[session.session_id] = session
    store.event("PENDING", f"TCP connection awaiting explicit gateway assignment from {peer}", protocol="TCP")
    await session.run_reader(on_disconnect, on_invalid_gateway_frame)
    sessions.sessions.pop(session.session_id, None)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global tcp_server
    host = os.getenv("TCP_LISTEN_HOST", "0.0.0.0")
    port = int(os.getenv("TCP_LISTEN_PORT", "5001"))
    tcp_server = await asyncio.start_server(on_tcp_client, host, port, limit=65536)
    scheduler.start()
    yield
    await scheduler.stop()
    tcp_server.close()
    await tcp_server.wait_closed()
    store.db.close()


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
    id: str | None = Field(default=None, max_length=100)
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
    connected_gateway_ids = {s.gateway_id for s in sessions.sessions.values() if s.gateway_id and not s.closed}
    freshness = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() - int(os.getenv("DEFAULT_STALE_AFTER_SECONDS", "30")), timezone.utc).isoformat()
    connected_modbus = {row["node_id"] for row in store.rows("SELECT node_id FROM polling_items WHERE last_status='GOOD' AND enabled=1 AND last_success_at IS NOT NULL AND last_success_at >= ?", (freshness,))}
    counts = {}
    for kind in ("ANALOG_INPUT", "ANALOG_OUTPUT"):
        typed = [node for node in nodes if node["node_type"] == kind]
        counts[kind.lower()] = {"total": len(typed), "connected": sum(node["gateway_id"] in connected_gateway_ids for node in typed), "disconnected": sum(node["gateway_id"] not in connected_gateway_ids for node in typed)}
    item_stats = store.one("SELECT SUM(CASE WHEN p.enabled=1 AND n.polling_enabled=1 THEN 1 ELSE 0 END) active FROM polling_items p JOIN nodes n ON n.id=p.node_id") or {}
    event_stats = store.one("SELECT SUM(CASE WHEN result='SUCCESS' THEN 1 ELSE 0 END) successful,SUM(CASE WHEN result IN ('TIMEOUT','DISCONNECTED','INVALID_RESPONSE','MODBUS_ERROR') THEN 1 ELSE 0 END) failed,AVG(CASE WHEN result='SUCCESS' THEN latency_ms END) avg_latency_ms FROM events") or {}
    node_summary = []
    for node in nodes:
        last = store.one("SELECT MAX(measured_at) at FROM measurements WHERE node_id=?", (node["id"],))
        item_count = store.one("SELECT COUNT(*) count FROM channels WHERE node_id=? AND enabled=1", (node["id"],))["count"]
        node_summary.append({**node,"gateway_connected":node["gateway_id"] in connected_gateway_ids,"modbus_healthy":node["id"] in connected_modbus,"last_measurement_at":last["at"],"enabled_channel_count":item_count})
    return {"inputs":counts["analog_input"],"outputs":counts["analog_output"],"gateway_connections":{"total":len(gateways),"connected":len(connected_gateway_ids)},"polling":{"active":item_stats.get("active") or 0,"successful":event_stats.get("successful") or 0,"failed":event_stats.get("failed") or 0,"average_response_ms":event_stats.get("avg_latency_ms"),"state":"RUNNING" if scheduler.running else "STOPPED"},"last_measurement_at":latest["at"] if latest else None,"nodes":node_summary,"recent_events":store.rows("SELECT * FROM events ORDER BY id DESC LIMIT 8")}


@app.get("/api/gateways")
async def list_gateways(_: str = Depends(authorize)):
    return store.rows("SELECT * FROM gateways ORDER BY name")


@app.post("/api/gateways", status_code=201)
async def create_gateway(data: GatewayCreate, actor=Depends(authorize)):
    gateway_id, now = data.id or str(uuid4()), utc_now()
    try:
        store.run("INSERT INTO gateways(id,name,model,transport_mode,created_at,updated_at) VALUES(?,?,?,?,?,?)", (gateway_id,data.name,data.model,data.transport_mode,now,now))
    except Exception as exc:
        raise HTTPException(409, "Gateway ID already exists or configuration is invalid") from exc
    store.audit("CREATE","gateway",gateway_id,data.model_dump(),actor)
    return store.one("SELECT * FROM gateways WHERE id=?", (gateway_id,))


@app.patch("/api/gateways/{gateway_id}")
async def patch_gateway(gateway_id: str, data: GatewayPatch, actor=Depends(authorize)):
    existing=store.one("SELECT * FROM gateways WHERE id=?",(gateway_id,))
    if not existing: raise HTTPException(404,"Gateway not found")
    changes=data.model_dump(exclude_unset=True)
    if "transport_mode" in changes and changes["transport_mode"] != existing["transport_mode"] and sessions.for_gateway(gateway_id):
        raise HTTPException(409,"Disconnect the commissioned session before changing transport framing")
    if changes:
        store.db.execute(f"UPDATE gateways SET {','.join(key+'=?' for key in changes)},updated_at=? WHERE id=?",(*changes.values(),utc_now(),gateway_id));store.db.commit()
        store.audit("UPDATE","gateway",gateway_id,changes,actor)
    return store.one("SELECT * FROM gateways WHERE id=?",(gateway_id,))


@app.get("/api/gateways/sessions")
async def gateway_sessions(_: str = Depends(authorize)):
    return [{"session_id":s.session_id,"peer":s.peer,"gateway_id":s.gateway_id,"transport_mode":s.transport_mode,"connected":not s.closed,"last_activity":getattr(s,"last_activity",None)} for s in sessions.sessions.values()]


@app.post("/api/gateways/sessions/{session_id}/assign/{gateway_id}")
async def assign_session(session_id: str, gateway_id: str, actor=Depends(authorize)):
    session = sessions.sessions.get(session_id)
    gateway = store.one("SELECT * FROM gateways WHERE id=?", (gateway_id,))
    if not session or session.closed:
        raise HTTPException(404, "Pending TCP session not found")
    if not gateway:
        raise HTTPException(404, "Gateway not found")
    if sessions.for_gateway(gateway_id):
        raise HTTPException(409, "Gateway already has an active session")
    session.gateway_id = gateway_id
    session.transport_mode = gateway["transport_mode"]
    session.parser = None
    now = utc_now()
    store.run("UPDATE gateways SET last_connected_at=?,last_activity_at=?,updated_at=? WHERE id=?", (now,now,now,gateway_id))
    store.event("CONNECTED", f"Gateway session commissioned from {session.peer}", gateway_id=gateway_id, protocol=session.transport_mode)
    store.audit("ASSIGN_SESSION","gateway",gateway_id,{"session_id":session_id,"peer":session.peer},actor)
    return {"gateway_id":gateway_id,"session_id":session_id,"transport_mode":session.transport_mode,"peer":session.peer}


@app.get("/api/nodes")
async def list_nodes(type: str | None = Query(default=None), search: str | None = None, _: str = Depends(authorize)):
    nodes = store.rows("SELECT * FROM nodes ORDER BY name")
    connected = {session.gateway_id for session in sessions.sessions.values() if session.gateway_id and not session.closed}
    freshness=datetime.fromtimestamp(datetime.now(timezone.utc).timestamp()-int(os.getenv("DEFAULT_STALE_AFTER_SECONDS","30")),timezone.utc).isoformat()
    for node in nodes:
        node["enabled_channel_count"] = store.one("SELECT COUNT(*) count FROM channels WHERE node_id=? AND enabled=1",(node["id"],))["count"]
        latest=store.one("SELECT MAX(measured_at) at FROM measurements WHERE node_id=?",(node["id"],))
        node["last_measurement_at"]=latest["at"]
        node["gateway_connected"]=node["gateway_id"] in connected
        node["modbus_healthy"]=bool(store.one("SELECT id FROM polling_items WHERE node_id=? AND enabled=1 AND last_status='GOOD' AND last_success_at>=? LIMIT 1",(node["id"],freshness)))
    if type:
        nodes = [node for node in nodes if node["node_type"] == type]
    if search:
        nodes = [node for node in nodes if search.casefold() in (node["name"] + node["hardware_model"] + node["id"]).casefold()]
    return nodes


@app.post("/api/nodes", status_code=201)
async def create_node(data: NodeCreate, actor=Depends(authorize)):
    if data.profile_id and not any(profile["id"] == data.profile_id for profile in PROFILES):
        raise HTTPException(422,"Device profile does not exist")
    if data.gateway_id and not store.one("SELECT id FROM gateways WHERE id=?",(data.gateway_id,)):
        raise HTTPException(422,"Assigned gateway does not exist")
    node_id, now = data.id or str(uuid4()), utc_now()
    store.run("INSERT INTO nodes(id,name,node_type,hardware_model,gateway_id,slave_id,profile_id,polling_enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",(node_id,data.name,data.node_type,data.hardware_model,data.gateway_id,data.slave_id,data.profile_id,int(data.polling_enabled),now,now))
    if data.node_type == "ANALOG_INPUT":
        for channel in range(1,7):
            store.run("INSERT INTO channels(id,node_id,channel_index,channel_type,label,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                      (str(uuid4()),node_id,channel,"ANALOG_INPUT",f"AI{channel}",now,now))
    store.audit("CREATE","node",node_id,data.model_dump(),actor)
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
    now=utc_now();channel_id=str(uuid4())
    try:
        store.run("INSERT INTO channels(id,node_id,channel_index,channel_type,label,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",(channel_id,node_id,data.channel_index,"ANALOG_OUTPUT",data.label,now,now))
    except Exception as exc: raise HTTPException(409,"Output channel index is already configured") from exc
    store.audit("CREATE","channel",channel_id,data.model_dump(),actor)
    return store.one("SELECT * FROM channels WHERE id=?",(channel_id,))


@app.patch("/api/channels/{channel_id}")
async def patch_channel(channel_id: str, changes: dict, actor=Depends(authorize)):
    channel=store.one("SELECT * FROM channels WHERE id=?",(channel_id,))
    if not channel: raise HTTPException(404,"Channel not found")
    if set(changes)-{"label","enabled","config"}: raise HTTPException(422,"Unsupported channel fields")
    if "label" in changes and (not isinstance(changes["label"],str) or not changes["label"].strip() or len(changes["label"])>80): raise HTTPException(422,"Channel label must be 1–80 characters")
    if "config" in changes and not isinstance(changes["config"],dict): raise HTTPException(422,"Channel config must be an object")
    if "enabled" in changes and not isinstance(changes["enabled"],bool): raise HTTPException(422,"Channel enabled state must be boolean")
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
    if "profile_id" in changes and changes["profile_id"] is not None and changes["profile_id"] not in [profile["id"] for profile in PROFILES]:
        raise HTTPException(422,"Device profile does not exist")
    if "polling_enabled" in changes and not isinstance(changes["polling_enabled"],bool):
        raise HTTPException(422,"polling_enabled must be boolean")
    if "gateway_id" in changes and changes["gateway_id"] and not store.one("SELECT id FROM gateways WHERE id=?",(changes["gateway_id"],)):
        raise HTTPException(422,"Assigned gateway does not exist")
    if "slave_id" in changes and (not isinstance(changes["slave_id"],int) or isinstance(changes["slave_id"],bool) or not 1 <= changes["slave_id"] <= 247):
        raise HTTPException(422,"Slave ID must be 1–247")
    normalized = {key:(int(value) if key=="polling_enabled" else value.strip() if key in {"name","hardware_model"} and isinstance(value,str) else value) for key,value in changes.items()}
    if normalized:
        store.db.execute(f"UPDATE nodes SET {','.join(key+'=?' for key in normalized)},updated_at=? WHERE id=?",(*normalized.values(),utc_now(),node_id)); store.db.commit()
        store.audit("UPDATE","node",node_id,changes,actor)
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
        store.audit("TOGGLE_POLLING_GROUP","node",node_id,{"group":category,"enabled":enabled,"items":[item["id"] for item in ids]},actor)
        return {"node_id":node_id,"group":category,"enabled":enabled,"updated_items":len(ids)}
    marks=','.join('?' for _ in selected)
    ids=store.rows(f"SELECT id FROM polling_items WHERE node_id=? AND register_name IN ({marks})",(node_id,*selected))
    store.run(f"UPDATE polling_items SET enabled=?,updated_at=? WHERE node_id=? AND register_name IN ({marks})",(int(enabled),utc_now(),node_id,*selected))
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
        definition=register(data.profile_id,data.register_name)
        if not definition or not definition.get("readable") or definition.get("restricted_action"):
            raise HTTPException(422,"Register profile entry is unavailable for polling")
        if definition.get("admin_only") and data.enabled:
            raise HTTPException(422,"Administrator-only profile items must be added disabled")
        if definition["function_code"] != data.function_code or definition["address"] != data.address or definition.get("quantity") != data.quantity:
            raise HTTPException(422,"Polling settings do not match the selected profile definition")
        if definition.get("data_type") != data.data_type:
            raise HTTPException(422,"Polling data type does not match the selected profile definition")
        if definition.get("word_order") == "unverified" and data.data_type.startswith("float"):
            raise HTTPException(422,"Float word order requires real-device verification")
    if data.data_type.startswith("float") and data.word_order not in {"BIG_WORD_FIRST", "LITTLE_WORD_FIRST"}:
        raise HTTPException(422,"Float word order must be explicitly verified and selected")
    item_id,now=data.id or str(uuid4()),utc_now()
    store.run("INSERT INTO polling_items(id,node_id,profile_id,register_name,function_code,address,quantity,data_type,byte_order,word_order,interval_ms,timeout_ms,retries,scale,unit,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(item_id,node_id,data.profile_id,data.register_name,data.function_code,data.address,data.quantity,data.data_type,data.byte_order,data.word_order,data.interval_ms,data.timeout_ms,data.retries,data.scale,data.unit,int(data.enabled),now,now))
    store.audit("CREATE","polling_item",item_id,data.model_dump(),actor)
    return store.one("SELECT * FROM polling_items WHERE id=?",(item_id,))


@app.patch("/api/polling/{item_id}")
async def patch_polling(item_id: str, changes: dict, actor=Depends(authorize)):
    item=store.one("SELECT * FROM polling_items WHERE id=?",(item_id,))
    if not item:
        raise HTTPException(404,"Polling item not found")
    allowed={"enabled","interval_ms","timeout_ms","retries","scale","unit"}
    if set(changes)-allowed:
        raise HTTPException(422,"Polling register identity cannot be changed in-place")
    if "enabled" in changes and not isinstance(changes["enabled"],bool): raise HTTPException(422,"enabled must be boolean")
    if "enabled" in changes and changes["enabled"] and item["profile_id"]:
        definition=register(item["profile_id"],item["register_name"])
        if definition and definition.get("admin_only") and actor!="admin": raise HTTPException(403,"Administrator authorization required")
    if "interval_ms" in changes and (not isinstance(changes["interval_ms"],int) or isinstance(changes["interval_ms"],bool) or not 250 <= changes["interval_ms"] <= 86400000): raise HTTPException(422,"Interval must be 250 ms–24 h")
    if "timeout_ms" in changes and (not isinstance(changes["timeout_ms"],int) or isinstance(changes["timeout_ms"],bool) or not 100 <= changes["timeout_ms"] <= 60000): raise HTTPException(422,"Timeout must be 100–60000 ms")
    if "retries" in changes and (not isinstance(changes["retries"],int) or isinstance(changes["retries"],bool) or not 0 <= changes["retries"] <= 5): raise HTTPException(422,"Retries must be 0–5")
    if "scale" in changes and (not isinstance(changes["scale"],(int,float)) or isinstance(changes["scale"],bool) or not math.isfinite(changes["scale"])): raise HTTPException(422,"Scale must be a finite number")
    if "unit" in changes and (not isinstance(changes["unit"],str) or len(changes["unit"])>32): raise HTTPException(422,"Unit must be 32 characters or fewer")
    normalized={key:(int(value) if key=="enabled" else value) for key,value in changes.items()}
    if normalized:
        store.db.execute(f"UPDATE polling_items SET {','.join(key+'=?' for key in normalized)},updated_at=? WHERE id=?",(*normalized.values(),utc_now(),item_id));store.db.commit()
        store.audit("UPDATE","polling_item",item_id,changes,actor)
    return store.one("SELECT * FROM polling_items WHERE id=?",(item_id,))


@app.delete("/api/polling/{item_id}", status_code=204)
async def delete_polling(item_id: str, actor=Depends(authorize)):
    item=store.one("SELECT * FROM polling_items WHERE id=?",(item_id,))
    if not item: raise HTTPException(404,"Polling item not found")
    store.run("DELETE FROM polling_items WHERE id=?",(item_id,));store.audit("DELETE","polling_item",item_id,{},actor)


@app.post("/api/polling/{item_id}/read")
async def read_once(item_id: str, _: str = Depends(authorize)):
    item=store.one("SELECT p.*,n.gateway_id,n.slave_id,n.polling_enabled FROM polling_items p JOIN nodes n ON n.id=p.node_id WHERE p.id=?",(item_id,))
    if not item: raise HTTPException(404,"Polling item not found")
    if not item["polling_enabled"]: raise HTTPException(409,"Node polling is disabled")
    await scheduler.read_once(item)
    return store.one("SELECT * FROM polling_items WHERE id=?",(item_id,))


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


@app.put("/api/nodes/{node_id}/calibration")
async def save_calibration(node_id: str, data: CalibrationInput, actor=Depends(authorize)):
    if not store.one("SELECT id FROM nodes WHERE id=? AND node_type='ANALOG_INPUT'",(node_id,)): raise HTTPException(404,"Analog input node not found")
    if data.scaling:
        from .calibration import Scaling, scale
        try: scale(0,Scaling(**data.scaling))
        except (TypeError,ValueError) as exc: raise HTTPException(422,str(exc))
    revision=(store.one("SELECT MAX(revision) value FROM calibrations WHERE node_id=? AND channel=?",(node_id,data.channel))["value"] or 0)+1
    store.run("INSERT INTO calibrations(node_id,channel,revision,gain,offset_ma,enabled,scaling_json,method,modified_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",(node_id,data.channel,revision,data.gain,data.offset_ma,int(data.enabled),json.dumps(data.scaling) if data.scaling else None,data.method,actor,utc_now()))
    store.audit("CALIBRATION_CHANGED","channel",f"{node_id}:{data.channel}",data.model_dump(),actor)
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
    if not store.one("SELECT id FROM nodes WHERE id=?",(node_id,)): raise HTTPException(404,"Node not found")
    values=store.rows("SELECT m.* FROM measurements m JOIN (SELECT channel,MAX(id) id FROM measurements WHERE node_id=? GROUP BY channel) latest ON latest.id=m.id ORDER BY m.channel",(node_id,))
    freshness=datetime.fromtimestamp(datetime.now(timezone.utc).timestamp()-int(os.getenv("DEFAULT_STALE_AFTER_SECONDS","30")),timezone.utc).isoformat()
    node=store.one("SELECT polling_enabled FROM nodes WHERE id=?",(node_id,))
    items=store.rows("SELECT register_name,enabled,last_status FROM polling_items WHERE node_id=?",(node_id,))
    for value in values:
        if value["measured_at"] < freshness:
            value["quality"]="STALE"
        if not node["polling_enabled"] or not any(item["enabled"] and item["register_name"] in {f"AI{value['channel']} current","AI current block"} for item in items):
            value["quality"]="DISABLED"
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


@app.get("/api/mappings")
async def mappings(_: str = Depends(authorize)):
    rows=store.rows("SELECT * FROM mappings ORDER BY name")
    for row in rows: row["config_json"]=json.loads(row["config_json"])
    return {"state":"PLANNED_OUTPUT_INTEGRATION_DISABLED","items":rows}


@app.get("/api/settings")
async def settings(_: str = Depends(authorize)):
    return {"tcp_listener":{"host":os.getenv("TCP_LISTEN_HOST","0.0.0.0"),"port":int(os.getenv("TCP_LISTEN_PORT","5001"))},"polling":{"minimum_interval_ms":250,"gateway_minimum_request_interval_ms":int(os.getenv("MODBUS_MIN_REQUEST_INTERVAL_MS","50")),"default_timeout_ms":1500,"max_retries":5},"freshness_seconds":int(os.getenv("DEFAULT_STALE_AFTER_SECONDS","30")),"event_retention_days":int(os.getenv("EVENT_RETENTION_DAYS","90")),"measurement_retention_days":int(os.getenv("MEASUREMENT_RETENTION_DAYS","365")),"hardware_writes":"DISABLED_UNTIL_PROFILE_VERIFIED"}


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
        while True:
            latest=store.rows("SELECT m.* FROM measurements m JOIN (SELECT node_id,channel,MAX(id) id FROM measurements GROUP BY node_id,channel) v ON v.id=m.id ORDER BY m.node_id,m.channel")
            await websocket.send_json({"type":"telemetry","at":utc_now(),"measurements":latest})
            await asyncio.sleep(1)
    except (WebSocketDisconnect,RuntimeError):
        return


web_dist = Path(__file__).resolve().parent.parent / "web" / "dist"
if web_dist.is_dir():
    app.mount("/", StaticFiles(directory=web_dist, html=True), name="web")
