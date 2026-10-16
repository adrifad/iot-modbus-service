# Hardware and protocol verification notes

## EBYTE ME31-XAXX0600

The built-in profile follows the ME31-XAXX0600 User Manual V1.1 register table. The integer analog block starts at `0x0064`, is FC04, six registers, and reports microamperes; the float block starts at `0x00C8`, is FC04, twelve registers, and reports float32 values in mA. Float word order remains unverified in the profile and cannot be scheduled until an operator selects an order after testing a real response.

The manual lists the filter setting as one shared holding register, six independent sampling range registers, and six-register blocks for each trigger threshold and trigger mode. The profile also includes the module and network addresses with the manual quantities. Network fields describe the ME31 itself, not the LTE gateway. They are administrator-only and added to polling disabled; this service does not write those settings.

The manufacturer's product listing confirms six 0–20/4–20 mA inputs, Modbus TCP and Modbus RTU support, 9600 baud default, and the slave default of 1. Confirm the deployed module's settings before polling. [Manufacturer product page](https://www.ebyte.com/product/2046.html)

## USR-G781

The G781 supports a network-transparent mode and a Modbus TCP-to-RTU conversion mode. In conversion mode, the VPS sends Modbus TCP frames and the G781 converts them for its serial device. In network-transparent mode, the VPS sends RTU frames and validates their CRC. Select the corresponding gateway profile after inspecting the G781's saved configuration; the project does not infer the mode from a connected socket. [USR-G781 manual](https://www.usr.cn/Down/USR-G781_V1.0.10.pdf)

The G781 software manual describes custom USER registration data and LINK send mode for network-transparent operation. The service keeps the optional `FIELDLINE/1` packet disabled by default (`GATEWAY_LINK_IDENTITY_ENABLED=false`); it is unavailable for the verified G781 Modbus TCP-conversion and W630 transparent RTU deployments. Do not enable it until the exact firmware and framing have been bench-verified to preserve the Modbus stream. The secure reconnect method for these two transports is an authenticated WireGuard ingress, with the site routing prerequisite documented in README. [USR-G781 software manual](https://www.usr.cn/Down/USR-G781-software-V1.0.10.pdf)

The actual LTE unit, serial wiring, baud/parity, conversion setting, slave ID, and measured response have not been tested in this workspace. The protocol integration test uses a local Modbus TCP simulator and proves only software framing, response validation, persistence, and calibration behavior.

## EBYTE ME31-XXXA0006 and USR-W630

The output configuration in the application follows the operator-provided bench verification: ME31-XXXA0006 behind a USR-W630 transparent serial-over-TCP session, RTU ADUs with CRC, slave ID 1. Input G781 sessions remain `MODBUS_TCP` with MBAP framing; these transports must stay configured independently.

- AO ranges: FC03 `0x0514`, quantity 6; 0 = 0–20 mA, 1 = 4–20 mA.
- Reported setpoints: FC03 `0x0064`, quantity 6; integer microamperes divided by 1000 gives mA.
- AO1–AO6 writes: FC16 to `0x0000`, `0x0002`, `0x0004`, `0x0006`, `0x0008`, `0x000A`; exactly two registers, IEEE-754 Float32 big-word-first. The driver accepts no other output write address or function.
- Every write is serialized with other transactions on the gateway, validates the FC16 echo, then reads the integer setpoint block and checks the selected channel against the target within 0.05 mA.

The ME31-XXXA0006 English V1.1 manual register table identifies the six AO range registers at `0x0514` as RW and lists FC06 and FC10 as write functions. The application implements only the narrow FC06 single-register write for an individual channel, validates its echo and requires a full FC03 range-block readback. [Manual copy with register table](https://manuals.plus/ebyte/me31-xxxa0006-network-i-o-networking-module-manual.pdf) · [Manufacturer product page](https://www.ebyte.com/product/2047.html). Verify the exact deployed hardware revision and whether the range setting persists across power cycles before relying on it. Automated simulation is not hardware validation.

The reported setpoint register is device readback, not an independent measurement of physical output current. The FC16 map and frame encoding were supplied as already verified by the operator, but this checkout has not performed a physical W630/ME31 write. Automated tests use a local RTU simulator and never connect to industrial hardware. Commission with a current-limited/known-safe load and a calibrated external meter before enabling live output pipelines.

## Bench checklist for AO range configuration and recovery

Run this only with an authorized operator and a safe test load:

1. Confirm G781 AI communication using the verified MBAP/Modbus TCP framing and FC04 `0x0064`, quantity 6.
2. Confirm W630 passes transparent RTU ADUs with valid CRC over its dedicated session; do not configure it as Modbus TCP conversion.
3. Confirm both default AO polling items are enabled and display reported setpoints plus range codes for AO1–AO6.
4. Compare AO setpoint register values at `0x0064` with the configured output setpoints. Label them reported setpoints; independently measure current with a calibrated meter.
5. Confirm range values at `0x0514` map as expected. Unknown values must remain UNKNOWN.
6. Verify FC06 support for the exact device firmware. With the load in a safe test condition, explicitly authorize a single-channel range change and confirm FC06 acknowledgement plus FC03 full-block readback.
7. Verify the selected range remains configured after a controlled power cycle if persistence is required by the installation.
8. Independently measure physical current before and after any range change. FC03 readback alone is not electrical verification.
9. Interrupt and restore G781 and W630 connectivity separately through the authenticated WireGuard router/relay. Verify automatic commissioning, a new session generation, and polling recovery without manual Assign.
10. Confirm reconnect issues no FC16 and no automatic range write. Pipelines must remain disarmed until an operator checks source freshness and current AO readback, then explicitly re-arms them.
