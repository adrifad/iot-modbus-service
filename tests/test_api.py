import os
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

from fastapi.testclient import TestClient

from backend import app as service
from backend.store import Store


class ApiTests(unittest.TestCase):
    def test_management_auth_and_persistent_polling_configuration(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Store(os.path.join(temp, "service.db"))
            service.store = database
            service.scheduler.store = database
            service.sessions.store = database
            token = "test-administrator-token"
            with patch.dict(os.environ, {"ADMIN_API_TOKEN": token, "TCP_LISTEN_HOST": "127.0.0.1", "TCP_LISTEN_PORT": "0"}):
                with TestClient(service.app) as client:
                    self.assertEqual(client.get("/api/health").status_code, 200)
                    self.assertEqual(client.get("/api/overview").status_code, 401)
                    headers = {"Authorization": f"Bearer {token}"}
                    gateway_id, node_id = str(uuid4()), str(uuid4())
                    created_gateway = client.post("/api/gateways", headers=headers, json={
                        "id": gateway_id, "name": "Bench gateway", "model": "USR-G781", "transport_mode": "MODBUS_TCP"
                    })
                    self.assertEqual(created_gateway.status_code, 201)
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
                    self.assertEqual(client.get("/api/overview", headers=headers).json()["inputs"]["total"], 1)


if __name__ == "__main__":
    unittest.main()
