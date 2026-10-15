# Hardware and protocol verification notes

## EBYTE ME31-XAXX0600

The built-in profile follows the ME31-XAXX0600 User Manual V1.1 register table. The integer analog block starts at `0x0064`, is FC04, six registers, and reports microamperes; the float block starts at `0x00C8`, is FC04, twelve registers, and reports float32 values in mA. Float word order remains unverified in the profile and cannot be scheduled until an operator selects an order after testing a real response.

The manual lists the filter setting as one shared holding register, six independent sampling range registers, and six-register blocks for each trigger threshold and trigger mode. The profile also includes the module and network addresses with the manual quantities. Network fields describe the ME31 itself, not the USR-G781 cellular gateway. They are administrator-only and added to polling disabled; device writes are not implemented.

The manufacturer's product listing confirms six 0–20/4–20 mA inputs, Modbus TCP and Modbus RTU support, 9600 baud default, and the slave default of 1. Confirm the deployed module's settings before polling. [Manufacturer product page](https://www.ebyte.com/product/2046.html)

## USR-G781

The G781 supports a network-transparent mode and a Modbus TCP-to-RTU conversion mode. In conversion mode, the VPS sends Modbus TCP frames and the G781 converts them for its serial device. In network-transparent mode, the VPS sends RTU frames and validates their CRC. Select the corresponding gateway profile after inspecting the G781's saved configuration; the project does not infer the mode from a connected socket. [USR-G781 manual](https://www.usr.cn/Down/USR-G781_V1.0.10.pdf)

The actual LTE unit, serial wiring, baud/parity, conversion setting, slave ID, and measured response have not been tested in this workspace. The protocol integration test uses a local Modbus TCP simulator and proves only software framing, response validation, persistence, and calibration behavior.
