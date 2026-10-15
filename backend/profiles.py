"""Built-in register profile. Addresses and semantics follow the supplied ME31 V1.1 brief."""

ME31 = {
    "id": "ebyte-me31-xaxx0600-v1.1",
    "name": "EBYTE ME31-XAXX0600",
    "registers": [
        *[{
            "name": f"AI{channel} current",
            "address": 0x0063 + channel,
            "function_code": 4,
            "quantity": 1,
            "data_type": "uint16",
            "unit": "mA",
            "scale": 0.001,
            "category": "Analog Measurements",
            "default_interval_ms": 1000,
            "readable": True,
            "writable": False,
            "measurement": {"type": "analog_current", "channel_indices": [channel], "conversion": "register_scale"},
            "description": "Unsigned current value in microampere; converted to mA by dividing by 1000.",
        } for channel in range(1, 7)],
        {
            "name": "AI current block",
            "address": 0x0064,
            "function_code": 4,
            "quantity": 6,
            "data_type": "uint16_array",
            "unit": "mA",
            "scale": 0.001,
            "category": "Analog Measurements",
            "default_interval_ms": 1000,
            "readable": True,
            "writable": False,
            "measurement": {"type": "analog_current", "channel_indices": [1, 2, 3, 4, 5, 6], "conversion": "register_scale"},
            "description": "AI1–AI6; six input registers.",
        },
        {
            "name": "AI float block",
            "address": 0x00C8,
            "function_code": 4,
            "quantity": 12,
            "data_type": "float32_array",
            "unit": "mA",
            "scale": 1,
            "byte_order": "big",
            "word_order": "unverified",
            "default_enabled": False,
            "category": "Analog Measurements",
            "default_interval_ms": 1000,
            "readable": True,
            "writable": False,
            "measurement": {"type": "analog_current", "channel_indices": [1, 2, 3, 4, 5, 6], "conversion": "register_scale", "registers_per_channel": 2},
            "description": "IEEE 754 float32, two registers per channel. Word order requires real-device verification.",
        },
        {"name":"AI filtering","address":0x04B0,"function_code":3,"quantity":1,"data_type":"uint16","unit":"","range":[1,16],"default":6,"category":"Analog Configuration","default_interval_ms":10000,"readable":True,"writable":True,"write_functions":[6,16],"description":"Shared by all AI channels; default 6. Higher values are more stable and slower to respond."},
        {"name":"AI sampling range","address":0x04B2,"function_code":3,"quantity":6,"data_type":"enum16_array","values":{"0":"0–20 mA","1":"4–20 mA"},"category":"Analog Configuration","default_interval_ms":10000,"readable":True,"writable":True,"write_functions":[6,16]},
        {"name":"AI trigger high thresholds","address":0x1F40,"function_code":3,"quantity":6,"data_type":"uint16_array","unit":"microampere","range":[0,20000],"category":"Trigger Configuration","default_interval_ms":10000,"readable":True,"writable":True,"write_functions":[6,16]},
        {"name":"AI trigger low thresholds","address":0x1F72,"function_code":3,"quantity":6,"data_type":"uint16_array","unit":"microampere","range":[0,20000],"category":"Trigger Configuration","default_interval_ms":10000,"readable":True,"writable":True,"write_functions":[6,16]},
        {"name":"AI trigger mode","address":0x1FA4,"function_code":3,"quantity":6,"data_type":"enum16_array","values":{"0":"Disabled","1":"Rising trigger","2":"Descending trigger","3":"Bilateral trigger"},"category":"Trigger Configuration","default_interval_ms":10000,"readable":True,"writable":True,"write_functions":[6,16]},
        {"name":"Device model","address":0x07D0,"function_code":3,"quantity":12,"data_type":"ascii","category":"Device Information","default_interval_ms":86400000,"readable":True,"writable":False},
        {"name":"Firmware version","address":0x07DC,"function_code":3,"quantity":1,"data_type":"uint16","category":"Device Information","default_interval_ms":86400000,"readable":True,"writable":False},
        {"name":"Module name","address":0x07DE,"function_code":3,"quantity":10,"data_type":"ascii","category":"Device Information","default_interval_ms":86400000,"readable":True,"writable":True,"write_functions":[16]},
        {"name":"Modbus slave address","address":0x07E8,"function_code":3,"quantity":1,"data_type":"uint16","category":"Device Information","default_interval_ms":86400000,"readable":True,"writable":True,"admin_only":True},
        {"name":"Serial baud rate","address":0x0834,"function_code":3,"quantity":1,"data_type":"uint16","category":"Device Information","default_interval_ms":86400000,"readable":True,"writable":True,"admin_only":True},
        {"name":"Serial parity","address":0x0836,"function_code":3,"quantity":1,"data_type":"uint16","category":"Device Information","default_interval_ms":86400000,"readable":True,"writable":True,"admin_only":True},
        {"name":"Module reboot","address":0x07EA,"function_code":6,"quantity":1,"data_type":"action","category":"Device Information","default_interval_ms":0,"readable":False,"writable":True,"restricted_action":True},
        {"name":"Factory reset","address":0x07E9,"function_code":6,"quantity":1,"data_type":"action","category":"Device Information","default_interval_ms":0,"readable":False,"writable":True,"restricted_action":True},
    ] + [{"name": name,"address": address,"function_code":3,"quantity":quantity,"data_type":dtype,"category":"Network Configuration","default_interval_ms":86400000,"readable":True,"writable":writable,"admin_only":True,"default_enabled":False,"write_functions":[6,16] if writable else [],"description":"ME31 network configuration. Changes are disruptive and administrator-only; this is not USR-G781 cellular telemetry."} for name,address,quantity,dtype,writable in [
        ("MAC address",0x0898,3,"raw",False),("Local IP address",0x089B,2,"raw",True),("Local port",0x089D,1,"uint16",True),("Subnet mask",0x089E,2,"raw",True),("Gateway address",0x08A0,2,"raw",True),("DHCP mode",0x08A2,1,"uint16",True),("Target IP / domain",0x08A3,64,"ascii",True),("Target port",0x08E3,1,"uint16",True),("DNS server",0x08E4,2,"raw",True),("Operating mode",0x08E6,1,"uint16",True),("Active upload interval",0x08E7,1,"uint16",True),("Modbus TCP/RTU conversion",0x08E8,1,"uint16",True),("Modbus address filtering",0x08E9,1,"uint16",True)
    ]]
}

PROFILES = [ME31]

ME31_OUTPUT = {
    "id": "ebyte-me31-xxxa0006-v1.1",
    "name": "EBYTE ME31-XXXA0006",
    "registers": [
        {
            "name": "AO output ranges",
            "address": 0x0514,
            "function_code": 3,
            "quantity": 6,
            "data_type": "enum16_array",
            "values": {"0": "0–20 mA", "1": "4–20 mA"},
            "category": "Analog Output Configuration",
            "readable": True,
            "writable": False,
        },
        {
            "name": "AO setpoint readback",
            "address": 0x0064,
            "function_code": 3,
            "quantity": 6,
            "data_type": "uint16_array",
            "unit": "mA",
            "scale": 0.001,
            "category": "Analog Output Measurements",
            "readable": True,
            "writable": False,
            "description": "Reported setpoint in microampere; this is not a physical current measurement.",
        },
        *[
            {
                "name": f"AO{channel} setpoint command",
                "address": (channel - 1) * 2,
                "function_code": 16,
                "quantity": 2,
                "data_type": "float32",
                "byte_order": "big",
                "word_order": "BIG_WORD_FIRST",
                "unit": "mA",
                "category": "Analog Output Commands",
                "readable": False,
                "writable": True,
                "restricted_action": True,
            }
            for channel in range(1, 7)
        ],
    ],
}

PROFILES.append(ME31_OUTPUT)

def register(profile_id: str, name: str):
    profile = next((p for p in PROFILES if p["id"] == profile_id), None)
    if profile is None:
        return None
    return next((item for item in profile["registers"] if item["name"] == name), None)
