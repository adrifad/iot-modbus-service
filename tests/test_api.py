import os
import asyncio
import hashlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from uuid import uuid4

from fastapi.testclient import TestClient

from backend import app as service
from backend.gateway import GatewaySession
from backend.store import Store


class ApiTests(unittest.TestCase):
    def test_management_auth_and_persistent_polling_configuration(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Store(os.path.join(temp, "service.db"))
            if service.store is not database:
                service.store.close()
            service.store = database
            service.scheduler.store = database
            service.sessions.store = database
            service.output_driver.store = database
            service.pipelines.store = database
            token = "test-administrator-token"
            with patch.dict(os.environ, {"ADMIN_API_TOKEN": token, "TCP_LISTEN_HOST": "127.0.0.1", "TCP_LISTEN_PORT": "0"}):
                with TestClient(service.app) as client:
                    self.assertEqual(client.get("/api/health").status_code, 200)
                    self.assertEqual(client.get("/api/overview").status_code, 401)
                    headers = {"Authorization": f"Bearer {token}"}
                    saved_timeouts = client.put("/api/settings/output-timeouts", headers=headers, json={
                        "output_range_timeout_ms": 4200, "output_write_timeout_ms": 5000, "output_readback_timeout_ms": 6100
                    })
                    self.assertEqual(saved_timeouts.status_code, 200, saved_timeouts.text)
                    self.assertEqual(saved_timeouts.json(), {"range_ms": 4200, "write_ms": 5000, "readback_ms": 6100})
                    self.assertEqual(database.setting("output_readback_timeout_ms"), "6100")
                    invalid_timeouts = client.put("/api/settings/output-timeouts", headers=headers, json={
                        "output_range_timeout_ms": 100, "output_write_timeout_ms": 5000, "output_readback_timeout_ms": 5000
                    })
                    self.assertEqual(invalid_timeouts.status_code, 422)
                    gateway_id, node_id = str(uuid4()), str(uuid4())
                    created_gateway = client.post("/api/gateways", headers=headers, json={
                        "id": gateway_id, "name": "Bench gateway", "model": "USR-G781", "transport_mode": "MODBUS_TCP"
                    })
                    self.assertEqual(created_gateway.status_code, 201)
                    transparent_id = str(uuid4())
                    transparent = client.post("/api/gateways", headers=headers, json={
                        "id": transparent_id, "name": "Transparent gateway", "model": "USR-G781", "transport_mode": "TRANSPARENT_SERIAL"
                    })
                    self.assertEqual(transparent.status_code, 201)
                    registration = client.post(f"/api/gateways/{transparent_id}/registration-token", headers=headers, json={})
                    self.assertEqual(registration.status_code, 200, registration.text)
                    packet = registration.json()["registration_packet"]
                    token_value = packet.split(" ")[2].strip()
                    self.assertEqual(database.one("SELECT registration_token_hash FROM gateways WHERE id=?", (transparent_id,))["registration_token_hash"], hashlib.sha256(token_value.encode()).hexdigest())
                    self.assertNotIn(token_value, str(database.one("SELECT * FROM gateways WHERE id=?", (transparent_id,))))

                    class IdentityWriter:
                        def get_extra_info(self, _name): return None
                        def close(self): pass
                        async def wait_closed(self): pass

                    async def identify_after_nat_reconnect():
                        first = GatewaySession("identity-first", asyncio.StreamReader(), IdentityWriter(), "192.0.2.10:4000", peer_ip="192.0.2.10")
                        first.state = "UNCOMMISSIONED"
                        service.sessions.sessions[first.session_id] = first
                        accepted = await service.authenticate_gateway_identity(first, packet.strip().encode("ascii"))
                        self.assertTrue(accepted)
                        second = GatewaySession("identity-reconnect", asyncio.StreamReader(), IdentityWriter(), "192.0.2.99:50000", peer_ip="192.0.2.99")
                        second.state = "UNCOMMISSIONED"
                        service.sessions.sessions[second.session_id] = second
                        accepted = await service.authenticate_gateway_identity(second, packet.strip().encode("ascii"))
                        self.assertTrue(accepted)
                        self.assertEqual(service.sessions.for_gateway(transparent_id), second)
                        self.assertEqual(first.state, "RETIRED")
                        outsider = GatewaySession("identity-outsider", asyncio.StreamReader(), IdentityWriter(), "203.0.113.77:60000", peer_ip="203.0.113.77")
                        outsider.state = "UNCOMMISSIONED"
                        self.assertFalse(await service.authenticate_gateway_identity(outsider, packet.strip().encode("ascii")))
                        self.assertIsNone(outsider.gateway_id)
                        invalid_identity = GatewaySession("identity-invalid-token", asyncio.StreamReader(), IdentityWriter(), "192.0.2.20:51000", peer_ip="192.0.2.20")
                        invalid_identity.state = "UNCOMMISSIONED"
                        wrong_packet = packet.replace(token_value, "invalid-token")
                        self.assertFalse(await service.authenticate_gateway_identity(invalid_identity, wrong_packet.strip().encode("ascii")))
                        self.assertIsNone(invalid_identity.gateway_id)
                    with patch.dict(os.environ, {"GATEWAY_TRUSTED_CIDRS": "192.0.2.0/24"}):
                        asyncio.run(identify_after_nat_reconnect())
                    self.assertEqual(database.one("SELECT reconnection_count FROM gateways WHERE id=?", (transparent_id,))["reconnection_count"], 1)
                    created_node = client.post("/api/nodes", headers=headers, json={
                        "id": node_id, "name": "Bench input", "node_type": "ANALOG_INPUT", "hardware_model": "EBYTE ME31-XAXX0600",
                        "gateway_id": gateway_id, "slave_id": 1, "profile_id": "ebyte-me31-xaxx0600-v1.1"
                    })
                    self.assertEqual(created_node.status_code, 201)
                    item = client.post(f"/api/nodes/{node_id}/polling", headers=headers, json={
                        "profile_id": "ebyte-me31-xaxx0600-v1.1", "register_name": "AI1 current",
                        "function_code": 4, "address": 100, "quantity": 1, "data_type": "uint16",
                        "interval_ms": 1000, "timeout_ms": 1500, "retries": 1, "scale": 0.001, "unit": "mA"
                    })
                    self.assertEqual(item.status_code, 201, item.text)
                    item_id = item.json()["id"]
                    self.assertEqual(client.patch(f"/api/polling/{item_id}", headers=headers, json={"enabled": False}).json()["enabled"], 0)
                    self.assertEqual(client.get(f"/api/nodes/{node_id}/polling", headers=headers).json()[0]["register_name"], "AI1 current")
                    custom = client.post(f"/api/nodes/{node_id}/polling", headers=headers, json={
                        "register_name": "Custom pressure raw", "function_code": 4, "address": 400, "quantity": 1,
                        "data_type": "uint16", "interval_ms": 2000, "timeout_ms": 1500, "retries": 1, "scale": 1, "unit": ""
                    })
                    self.assertEqual(custom.status_code, 201, custom.text)
                    custom_group = client.put(f"/api/nodes/{node_id}/polling/groups/Custom%20Registers?enabled=false", headers=headers)
                    self.assertEqual(custom_group.status_code, 200, custom_group.text)
                    self.assertEqual(custom_group.json()["updated_items"], 1)
                    self.assertEqual(client.patch(f"/api/polling/{custom.json()['id']}", headers=headers, json={"enabled": True}).json()["enabled"], 1)
                    network = client.post(f"/api/nodes/{node_id}/polling", headers=headers, json={
                        "profile_id": "ebyte-me31-xaxx0600-v1.1", "register_name": "Local IP address",
                        "function_code": 3, "address": 2203, "quantity": 2, "data_type": "raw",
                        "interval_ms": 86400000, "timeout_ms": 1500, "retries": 0, "scale": 1, "unit": "", "enabled": False
                    })
                    self.assertEqual(network.status_code, 201, network.text)
                    self.assertEqual(network.json()["enabled"], 0)

                    float_item = client.post(f"/api/nodes/{node_id}/polling", headers=headers, json={
                        "profile_id": "ebyte-me31-xaxx0600-v1.1", "register_name": "AI float block",
                        "function_code": 4, "address": 0x00C8, "quantity": 12, "data_type": "float32_array",
                        "word_order": "unverified", "interval_ms": 1000, "timeout_ms": 1500, "retries": 1,
                        "scale": 1, "unit": "mA", "enabled": False
                    })
                    self.assertEqual(float_item.status_code, 201, float_item.text)
                    self.assertEqual(float_item.json()["float_verified"], 0)
                    self.assertEqual(client.patch(f"/api/polling/{float_item.json()['id']}", headers=headers, json={"enabled": True}).status_code, 422)
                    group_enable = client.put(f"/api/nodes/{node_id}/polling/groups/Analog%20Measurements?enabled=true", headers=headers, json={})
                    self.assertEqual(group_enable.status_code, 409)

                    class FloatSession:
                        transport_mode = "MODBUS_TCP"
                        async def read_registers(self, *_args):
                            return [0x4148, 0x0000] * 6, bytes.fromhex("00 01 00 00 00 1B 01 04 18") + bytes.fromhex("41 48 00 00") * 6, bytes.fromhex("00 01 00 00 00 06 01 04 00 C8 00 0C"), 2.5
                    with patch.object(service.sessions, "for_gateway", return_value=FloatSession()):
                        verified = client.post(f"/api/polling/{float_item.json()['id']}/verify-float", headers=headers, json={
                            "channel": 1, "reference_ma": 12.5, "tolerance_ma": 0.01,
                            "byte_order": "big", "word_order": "BIG_WORD_FIRST"
                        })
                    self.assertEqual(verified.status_code, 200, verified.text)
                    self.assertTrue(verified.json()["verified"])
                    self.assertEqual(client.patch(f"/api/polling/{float_item.json()['id']}", headers=headers, json={"enabled": True}).json()["enabled"], 1)
                    changed_decoder=client.patch(f"/api/polling/{float_item.json()['id']}",headers=headers,json={"word_order":"LITTLE_WORD_FIRST"})
                    self.assertEqual(changed_decoder.status_code,200,changed_decoder.text)
                    self.assertEqual(changed_decoder.json()["enabled"],0)
                    self.assertEqual(changed_decoder.json()["float_verified"],0)
                    self.assertEqual(client.patch(f"/api/polling/{float_item.json()['id']}",headers=headers,json={"enabled":True}).status_code,422)

                    ai2 = client.post(f"/api/nodes/{node_id}/polling", headers=headers, json={
                        "profile_id": "ebyte-me31-xaxx0600-v1.1", "register_name": "AI2 current",
                        "function_code": 4, "address": 0x0065, "quantity": 1, "data_type": "uint16",
                        "interval_ms": 1000, "timeout_ms": 1500, "retries": 1, "scale": 0.001, "unit": "mA"
                    })
                    self.assertEqual(ai2.status_code, 201, ai2.text)
                    sample_at = datetime.now(timezone.utc).isoformat()
                    database.run("UPDATE polling_items SET last_status='GOOD',last_success_at=? WHERE id=?", (sample_at, ai2.json()["id"]))
                    for raw in (12.001, 12.000, 12.002):
                        database.run("INSERT INTO measurements(node_id,channel,register_value,raw_registers_json,raw_ma,calibrated_ma,quality,measured_at) VALUES(?,?,?,?,?,?,?,?)",
                                     (node_id, 2, 12000, "[12000]", raw, raw, "GOOD", sample_at))
                    capture = client.post(f"/api/nodes/{node_id}/calibration/2/capture", headers=headers, json={"tolerance_ma": 0.01})
                    self.assertEqual(capture.status_code, 200, capture.text)
                    self.assertAlmostEqual(capture.json()["raw_ma"], 12.001)
                    self.assertEqual(capture.json()["samples"], 3)
                    unstable = client.post(f"/api/nodes/{node_id}/calibration/2/capture", headers=headers, json={"tolerance_ma": 0.0005})
                    self.assertEqual(unstable.status_code, 409)
                    revision_one = client.put(f"/api/nodes/{node_id}/calibration", headers=headers, json={"channel": 2, "gain": 1.02, "offset_ma": -0.1, "enabled": True, "method": "GAIN_OFFSET"})
                    self.assertEqual(revision_one.status_code, 200, revision_one.text)
                    revision_two = client.post(f"/api/nodes/{node_id}/calibration/2/revert/{revision_one.json()['revision']}", headers=headers, json={})
                    self.assertEqual(revision_two.status_code, 200, revision_two.text)
                    self.assertEqual(revision_two.json()["revision"], revision_one.json()["revision"] + 1)
                    self.assertAlmostEqual(revision_two.json()["gain"], 1.02)
                    self.assertAlmostEqual(database.one("SELECT raw_ma FROM measurements WHERE node_id=? AND channel=2 ORDER BY id DESC LIMIT 1", (node_id,))["raw_ma"], 12.002)
                    stale_at = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
                    database.run("UPDATE polling_items SET last_success_at=? WHERE id=?", (stale_at, ai2.json()["id"]))
                    database.run("UPDATE measurements SET measured_at=? WHERE node_id=? AND channel=2", (stale_at, node_id))
                    stale = client.post(f"/api/nodes/{node_id}/calibration/2/capture", headers=headers, json={"tolerance_ma": 0.01})
                    self.assertEqual(stale.status_code, 409)
                    overview = client.get("/api/overview", headers=headers).json()
                    self.assertEqual(overview["inputs"]["total"], 1)
                    self.assertEqual(overview["inputs"]["gateway_connected"], 0)
                    self.assertEqual(overview["inputs"]["disconnected"], 1)
                    self.assertEqual(overview["inputs"]["unassigned"], 0)
                    self.assertEqual(overview["inputs"]["modbus_responding"], 0)
                    self.assertEqual(overview["outputs"]["total"], 0)
                    with client.websocket_connect("/ws/telemetry") as websocket:
                        websocket.send_json({"type": "auth", "token": token})
                        self.assertEqual(websocket.receive_json()["type"], "authenticated")

    def test_fixed_me31_profiles_gateway_lifecycle_and_pipeline_ownership(self):
        with tempfile.TemporaryDirectory() as temp:
            database=Store(os.path.join(temp,"pipelines.db"))
            if service.store is not database:
                try: service.store.close()
                except Exception: pass
            service.store=database
            service.scheduler.store=database
            service.sessions.store=database
            service.sessions.sessions.clear();service.sessions.tasks.clear();service.sessions.by_gateway.clear()
            service.output_driver.store=database;service.pipelines.store=database
            token="test-pipeline-administrator-token"
            with patch.dict(os.environ,{"ADMIN_API_TOKEN":token,"TCP_LISTEN_HOST":"127.0.0.1","TCP_LISTEN_PORT":"0"}):
                with TestClient(service.app) as client:
                    headers={"Authorization":f"Bearer {token}"}
                    input_gateway=client.post("/api/gateways",headers=headers,json={"name":"Input G781","model":"USR-G781","transport_mode":"MODBUS_TCP"}).json()
                    output_gateway=client.post("/api/gateways",headers=headers,json={"name":"Output W630","model":"USR-W630","transport_mode":"TRANSPARENT_SERIAL"}).json()
                    input_node=client.post("/api/nodes",headers=headers,json={"name":"ME31 AI","node_type":"ANALOG_INPUT","gateway_id":input_gateway["id"],"slave_id":1}).json()
                    output_response=client.post("/api/nodes",headers=headers,json={"name":"ME31 AO","node_type":"ANALOG_OUTPUT","gateway_id":output_gateway["id"],"slave_id":1})
                    self.assertEqual(output_response.status_code,201,output_response.text)
                    output_node=output_response.json()
                    self.assertEqual(output_node["hardware_model"],"EBYTE ME31-XXXA0006")
                    self.assertEqual(output_node["profile_id"],"ebyte-me31-xxxa0006-v1.1")
                    channels=client.get(f"/api/nodes/{output_node['id']}/channels",headers=headers).json()
                    self.assertEqual([channel["label"] for channel in channels],[f"AO{i}" for i in range(1,7)])
                    invalid=client.post("/api/nodes",headers=headers,json={"name":"Wrong model","node_type":"ANALOG_OUTPUT","hardware_model":"Other AO","gateway_id":output_gateway["id"]})
                    self.assertEqual(invalid.status_code,422)
                    no_manual_confirm=client.post(f"/api/nodes/{output_node['id']}/outputs/1/set",headers=headers,json={"target_ma":4})
                    self.assertEqual(no_manual_confirm.status_code,422)

                    definition={"name":"AI1 to AO2","source_node_id":input_node["id"],"source_channel":1,
                        "destination_node_id":output_node["id"],"destination_channel":2,"source_value_type":"CALIBRATED_MA",
                        "config":{"transformation":"DIRECT","update_interval_ms":1000,"deadband_ma":0.05,"stale_timeout_ms":10000,
                            "timeout_ms":2000,"retries":1,"fail_safe_policy":"HOLD_LAST"}}
                    first=client.post("/api/pipelines",headers=headers,json=definition)
                    self.assertEqual(first.status_code,201,first.text)
                    first_id=first.json()["id"]
                    self.assertFalse(first.json()["enabled"])
                    self.assertFalse(first.json()["armed"])
                    compatibility=client.get("/api/mappings",headers=headers).json()
                    self.assertIn("items",compatibility)
                    self.assertEqual(compatibility["items"][0]["config_json"]["transformation"],"DIRECT")
                    second=client.post("/api/pipelines",headers=headers,json={**definition,"name":"AI3 to AO2","source_channel":3})
                    self.assertEqual(second.status_code,201,second.text)
                    self.assertEqual(client.post(f"/api/pipelines/{first_id}/enable",headers=headers,json={"confirm":True}).status_code,200)
                    second_enabled=client.post(f"/api/pipelines/{second.json()['id']}/enable",headers=headers,json={"confirm":True})
                    self.assertEqual(second_enabled.status_code,200,second_enabled.text)
                    self.assertFalse(second_enabled.json()["armed"])
                    self.assertEqual(client.post(f"/api/pipelines/{first_id}/disable",headers=headers).status_code,200)
                    self.assertEqual(client.post(f"/api/pipelines/{second.json()['id']}/enable",headers=headers,json={"confirm":True}).status_code,200)
                    self.assertEqual(client.patch(f"/api/gateways/{output_gateway['id']}",headers=headers,json={"transport_mode":"MODBUS_TCP"}).status_code,409)
                    blocked=client.delete(f"/api/gateways/{output_gateway['id']}",headers=headers)
                    self.assertEqual(blocked.status_code,409)
                    self.assertIn("ME31 AO",blocked.json()["detail"])

                    unused=client.post("/api/gateways",headers=headers,json={"name":"Unused","model":"USR-W630","transport_mode":"TRANSPARENT_SERIAL"}).json()
                    class PendingWriter:
                        def get_extra_info(self,_name): return None
                        def close(self): pass
                        async def wait_closed(self): pass
                    pending=GatewaySession("pending-session",None,PendingWriter(),"192.0.2.10:4000")
                    pending.state="UNCOMMISSIONED"
                    service.sessions.sessions[pending.session_id]=pending
                    assigned=client.post(f"/api/gateways/sessions/{pending.session_id}/assign/{unused['id']}",headers=headers,json={})
                    self.assertEqual(assigned.status_code,200,assigned.text)
                    self.assertTrue(pending.commissioned)
                    already=client.post(f"/api/gateways/sessions/{pending.session_id}/assign/{unused['id']}",headers=headers,json={})
                    self.assertEqual(already.status_code,409)
                    disconnected=client.post(f"/api/gateways/{unused['id']}/disconnect",headers=headers,json={})
                    self.assertEqual(disconnected.status_code,200,disconnected.text)
                    self.assertIsNone(service.sessions.for_gateway(unused["id"]))
                    self.assertEqual(client.delete(f"/api/gateways/{unused['id']}",headers=headers).status_code,204)
                    self.assertIsNone(database.one("SELECT id FROM gateways WHERE id=?",(unused["id"],)))


if __name__ == "__main__":
    unittest.main()
