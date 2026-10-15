export type Gateway = { id: string; name: string; model: string; transport_mode: string; connection_state?: string; commissioning_state?: string; identity_state?: string; commissioning_method?: string | null; automatic_commissioning_available?: boolean; manual_intervention_required?: boolean; reconnection_count?: number; last_disconnect_reason?: string | null; last_activity_at?: string | null; last_connected_at?: string | null; last_disconnected_at?: string | null; last_diagnostic?: { at: string; result: string; detail: string } | null; last_modbus_transaction?: { at: string; result: string; function_code: number; address: number; quantity: number; latency_ms: number; detail: string } | null }
export type Node = { id: string; name: string; node_type: 'ANALOG_INPUT' | 'ANALOG_OUTPUT'; hardware_model: string; gateway_id: string | null; slave_id: number; profile_id: string | null; polling_enabled: number; gateway_connected?: boolean; gateway_state?: string; modbus_health?: string; modbus_healthy?: boolean; last_measurement_at?: string | null; last_modbus_at?: string | null; enabled_channel_count?: number }
export type PollItem = { id: string; node_id: string; profile_id: string | null; register_name: string; function_code: number; address: number; quantity: number; data_type: string; byte_order: string; word_order: string; float_verified: number; interval_ms: number; timeout_ms: number; retries: number; scale: number; unit: string; enabled: number; last_value: string | null; last_success_at: string | null; last_latency_ms: number | null; last_status: string; error_count: number }
export type Measurement = { id: number; node_id: string; channel: number; register_value: number; raw_registers_json: string; raw_ma: number; calibrated_ma: number; engineering_value: number | null; engineering_unit: string | null; quality: string; measured_at: string }
export type Overview = { inputs: { total: number; connected: number; disconnected: number; unassigned: number; gateway_connected: number; modbus_responding: number; unhealthy: number; health_unknown: number }; outputs: { total: number; connected: number; disconnected: number; unassigned: number; gateway_connected: number; modbus_responding: number; unhealthy: number; health_unknown: number }; gateway_connections: { total: number; connected: number }; polling: { active: number; successful: number; failed: number; average_response_ms: number | null; state: string }; last_measurement_at: string | null; nodes: Node[]; recent_events: EventRow[] }
export type EventRow = { id: number; at: string; gateway_id: string | null; node_id: string | null; pipeline_id?: string | null; source_node_id?: string | null; source_channel?: number | null; destination_node_id?: string | null; destination_channel?: number | null; classification?: string | null; direction: string | null; protocol: string | null; function_code: number | null; address: number | null; quantity: number | null; latency_ms: number | null; result: string; detail: string; request_hex?: string; response_hex?: string }

let token = sessionStorage.getItem('fieldline-token') ?? ''
export function setToken(value: string) { token = value; value ? sessionStorage.setItem('fieldline-token', value) : sessionStorage.removeItem('fieldline-token') }
export function getToken() { return token }

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(path, { ...init, headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}), ...init.headers } })
  if (!response.ok) {
    const message = await response.json().catch(() => ({}))
    if (response.status === 401) { setToken(''); window.dispatchEvent(new Event('fieldline-auth-expired')) }
    throw new Error(message.detail || `Request failed (${response.status})`)
  }
  if (response.status === 204) return undefined as T
  return response.json()
}

export const post = <T = unknown>(path: string, body: unknown) => api<T>(path, { method: 'POST', body: JSON.stringify(body) })
export const put = (path: string, body: unknown) => api(path, { method: 'PUT', body: JSON.stringify(body) })
export const patch = (path: string, body: unknown) => api(path, { method: 'PATCH', body: JSON.stringify(body) })
