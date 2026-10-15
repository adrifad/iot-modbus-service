import { FormEvent, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, Node, patch, post } from './api'

type Pipeline = {
  id: string
  name: string
  source_node_id: string
  source_channel: number
  destination_node_id: string
  destination_channel: number
  source_value_type: 'RAW_MA' | 'CALIBRATED_MA' | 'ENGINEERING'
  config: Record<string, string | number>
  enabled: boolean
  armed: boolean
  status: string
  last_source_value: number | null
  last_requested_value: number | null
  last_acknowledged_value: number | null
  last_success_at: string | null
  last_error: string | null
}

type Definition = {
  name: string
  source_node_id: string
  source_channel: number
  destination_node_id: string
  destination_channel: number
  source_value_type: Pipeline['source_value_type']
  config: Record<string, string | number>
}

const initial: Definition = {
  name: '', source_node_id: '', source_channel: 1, destination_node_id: '', destination_channel: 1,
  source_value_type: 'CALIBRATED_MA',
  config: { transformation: 'DIRECT', update_interval_ms: 1000, deadband_ma: 0.05, stale_timeout_ms: 10000,
    timeout_ms: 2000, retries: 1, fail_safe_policy: 'HOLD_LAST' },
}

const date = (value: string | null) => value ? new Date(value).toLocaleString() : 'No confirmed write'
const fixed = (value: number | null) => value == null ? '—' : `${value.toFixed(3)} mA`

function statusTone(status: string) {
  if (status === 'RUNNING') return 'good'
  if (['DISABLED', 'DISARMED', 'WAITING_FOR_SOURCE'].includes(status)) return ''
  return 'warning'
}

export default function PipelinePage() {
  const client = useQueryClient()
  const pipelines = useQuery({ queryKey: ['pipelines'], queryFn: () => api<Pipeline[]>('/api/pipelines'), refetchInterval: 2000 })
  const inputs = useQuery({ queryKey: ['nodes', 'ANALOG_INPUT'], queryFn: () => api<Node[]>('/api/nodes?type=ANALOG_INPUT'), refetchInterval: 2000 })
  const outputs = useQuery({ queryKey: ['nodes', 'ANALOG_OUTPUT'], queryFn: () => api<Node[]>('/api/nodes?type=ANALOG_OUTPUT'), refetchInterval: 2000 })
  const [form, setForm] = useState<Definition>(initial)
  const [editing, setEditing] = useState<string | null>(null)
  const [detail, setDetail] = useState<string | null>(null)
  const history = useQuery({
    queryKey: ['pipeline-history', detail],
    queryFn: () => api<Record<string, unknown>[]>(`/api/pipelines/${detail}/history`),
    enabled: !!detail,
  })
  const [error, setError] = useState('')
  const refresh = () => { client.invalidateQueries({ queryKey: ['pipelines'] }); client.invalidateQueries({ queryKey: ['overview'] }); client.invalidateQueries({ queryKey: ['outputs'] }) }
  const save = useMutation({
    mutationFn: () => editing ? patch(`/api/pipelines/${editing}`, form) : post('/api/pipelines', form),
    onSuccess: () => { refresh(); setEditing(null); setForm(initial); setError('') },
    onError: (cause) => setError((cause as Error).message),
  })
  const action = useMutation({
    mutationFn: ({ id, verb, confirm = false }: { id: string; verb: string; confirm?: boolean }) => post(`/api/pipelines/${id}/${verb}`, { confirm }),
    onSuccess: refresh,
    onError: (cause) => setError((cause as Error).message),
  })
  const remove = useMutation({
    mutationFn: (id: string) => api<void>(`/api/pipelines/${id}`, { method: 'DELETE' }),
    onSuccess: () => { refresh(); setDetail(null) },
    onError: (cause) => setError((cause as Error).message),
  })
  const sourceNodes = inputs.data || []
  const destinationNodes = outputs.data || []
  const selectedSource = sourceNodes.find(node => node.id === form.source_node_id) || sourceNodes[0]
  const selectedDestination = destinationNodes.find(node => node.id === form.destination_node_id) || destinationNodes[0]
  const rows = pipelines.data || []
  const detailRow = rows.find(row => row.id === detail)
  const sourceDetail = detailRow ? sourceNodes.find(node => node.id === detailRow.source_node_id) : undefined
  const destinationDetail = detailRow ? destinationNodes.find(node => node.id === detailRow.destination_node_id) : undefined
  const sourceMeasurements = useQuery({
    queryKey: ['pipeline-source', detailRow?.source_node_id],
    queryFn: () => api<{ channel: number; quality: string; measured_at: string; raw_ma: number; calibrated_ma: number; engineering_value: number | null }[]>(`/api/nodes/${detailRow!.source_node_id}/measurements/latest`),
    enabled: !!detailRow,
    refetchInterval: detailRow ? 2000 : false,
  })
  const sourceMeasurement = sourceMeasurements.data?.find(item => item.channel === detailRow?.source_channel)
  const channelNames = useMemo(() => new Map([...sourceNodes, ...destinationNodes].map(node => [node.id, node.name])), [sourceNodes, destinationNodes])
  const setConfig = (key: string, value: string | number) => setForm(old => ({ ...old, config: { ...old.config, [key]: value } }))

  function beginEdit(row: Pipeline) {
    setEditing(row.id)
    setForm({ name: row.name, source_node_id: row.source_node_id, source_channel: row.source_channel,
      destination_node_id: row.destination_node_id, destination_channel: row.destination_channel,
      source_value_type: row.source_value_type, config: { ...row.config } })
    setError('')
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }

  function submit(event: FormEvent) {
    event.preventDefault()
    setError('')
    save.mutate()
  }

  function control(row: Pipeline, verb: string) {
    const prompts: Record<string, string> = {
      enable: 'Enable this pipeline configuration? It will remain disarmed until separately armed.',
      arm: `Arm output control for ${row.name}? This authorizes backend FC16 writes to AO${row.destination_channel}.`,
    }
    if (prompts[verb] && !window.confirm(prompts[verb])) return
    action.mutate({ id: row.id, verb, confirm: !!prompts[verb] })
  }

  return <>
    <div className="page-head"><div><h1>I/O pipelines</h1><p>Backend-managed AI to AO control. Each output needs an explicit enable and arm action.</p></div></div>
    <div className="notice info">Pipeline definitions survive restarts, but armed control is cleared on service restart or gateway loss. HOLD_LAST stops new commands; the module may retain its last electrical output.</div>
    {error && <p className="error" role="alert">{error}</p>}

    <section className="panel" style={{ marginTop: 16 }}>
      <div className="panel-head"><div><div className="panel-title">{editing ? 'Edit pipeline' : 'Create pipeline'}</div><div className="panel-note" style={{ marginTop: 3 }}>New definitions are always saved disabled and disarmed.</div></div></div>
      <form className="panel-body" onSubmit={submit}>
        <div className="form-grid">
          <div className="field"><label htmlFor="pipeline-name">Pipeline name</label><input id="pipeline-name" className="input" maxLength={100} required value={form.name} onChange={e => setForm(old => ({ ...old, name: e.target.value }))} /></div>
          <div className="field"><label htmlFor="source-node">Input node</label><select id="source-node" className="select" required value={form.source_node_id} onChange={e => setForm(old => ({ ...old, source_node_id: e.target.value }))}><option value="">Select input node</option>{sourceNodes.map(node => <option key={node.id} value={node.id}>{node.name} · {node.gateway_state}</option>)}</select></div>
          <div className="field"><label htmlFor="source-channel">Input channel</label><select id="source-channel" className="select" value={form.source_channel} onChange={e => setForm(old => ({ ...old, source_channel: Number(e.target.value) }))}>{Array.from({ length: 6 }, (_, i) => <option key={i} value={i + 1}>AI{i + 1}</option>)}</select></div>
          <div className="field"><label htmlFor="source-value">Source value</label><select id="source-value" className="select" value={form.source_value_type} onChange={e => setForm(old => ({ ...old, source_value_type: e.target.value as Definition['source_value_type'] }))}><option value="RAW_MA">Raw current (mA)</option><option value="CALIBRATED_MA">Calibrated current (mA)</option><option value="ENGINEERING">Engineering value</option></select></div>
          <div className="field"><label htmlFor="destination-node">Output node</label><select id="destination-node" className="select" required value={form.destination_node_id} onChange={e => setForm(old => ({ ...old, destination_node_id: e.target.value }))}><option value="">Select output node</option>{destinationNodes.map(node => <option key={node.id} value={node.id}>{node.name} · {node.gateway_state}</option>)}</select></div>
          <div className="field"><label htmlFor="destination-channel">Output channel</label><select id="destination-channel" className="select" value={form.destination_channel} onChange={e => setForm(old => ({ ...old, destination_channel: Number(e.target.value) }))}>{Array.from({ length: 6 }, (_, i) => <option key={i} value={i + 1}>AO{i + 1}</option>)}</select></div>
          <div className="field"><label htmlFor="transform">Output transformation</label><select id="transform" className="select" value={String(form.config.transformation || 'DIRECT')} onChange={e => setConfig('transformation', e.target.value)}><option value="DIRECT">Direct current</option><option value="GAIN_OFFSET">Gain and offset</option><option value="TWO_POINT">Two-point scaling</option></select></div>
          <div className="field"><label htmlFor="interval">Update interval (ms)</label><input id="interval" className="input" type="number" min={250} max={60000} value={Number(form.config.update_interval_ms || 1000)} onChange={e => setConfig('update_interval_ms', Number(e.target.value))} /></div>
          <div className="field"><label htmlFor="deadband">Deadband (mA)</label><input id="deadband" className="input" type="number" min={0} max={25} step={0.01} value={Number(form.config.deadband_ma || 0)} onChange={e => setConfig('deadband_ma', Number(e.target.value))} /></div>
          <div className="field"><label htmlFor="stale">Maximum source age (ms)</label><input id="stale" className="input" type="number" min={500} max={3600000} value={Number(form.config.stale_timeout_ms || 10000)} onChange={e => setConfig('stale_timeout_ms', Number(e.target.value))} /></div>
          <div className="field"><label htmlFor="timeout">Write timeout (ms)</label><input id="timeout" className="input" type="number" min={100} max={10000} value={Number(form.config.timeout_ms || 2000)} onChange={e => setConfig('timeout_ms', Number(e.target.value))} /></div>
          <div className="field"><label htmlFor="retries">Retries</label><input id="retries" className="input" type="number" min={0} max={3} value={Number(form.config.retries ?? 1)} onChange={e => setConfig('retries', Number(e.target.value))} /></div>
          {form.config.transformation === 'GAIN_OFFSET' && <><div className="field"><label htmlFor="gain">Gain</label><input id="gain" className="input" type="number" step="0.0001" value={Number(form.config.gain ?? 1)} onChange={e => setConfig('gain', Number(e.target.value))} /></div><div className="field"><label htmlFor="offset">Offset (mA)</label><input id="offset" className="input" type="number" step="0.001" value={Number(form.config.offset_ma ?? 0)} onChange={e => setConfig('offset_ma', Number(e.target.value))} /></div></>}
          {form.config.transformation === 'TWO_POINT' && <><div className="field"><label htmlFor="in-min">Input minimum</label><input id="in-min" className="input" type="number" step="0.001" value={Number(form.config.input_min ?? 4)} onChange={e => setConfig('input_min', Number(e.target.value))} /></div><div className="field"><label htmlFor="in-max">Input maximum</label><input id="in-max" className="input" type="number" step="0.001" value={Number(form.config.input_max ?? 20)} onChange={e => setConfig('input_max', Number(e.target.value))} /></div><div className="field"><label htmlFor="out-min">Output minimum (mA)</label><input id="out-min" className="input" type="number" step="0.001" value={Number(form.config.output_min_ma ?? 4)} onChange={e => setConfig('output_min_ma', Number(e.target.value))} /></div><div className="field"><label htmlFor="out-max">Output maximum (mA)</label><input id="out-max" className="input" type="number" step="0.001" value={Number(form.config.output_max_ma ?? 20)} onChange={e => setConfig('output_max_ma', Number(e.target.value))} /></div></>}
          <div className="field"><label htmlFor="failsafe">Invalid-source policy</label><select id="failsafe" className="select" value={String(form.config.fail_safe_policy || 'HOLD_LAST')} onChange={e => setConfig('fail_safe_policy', e.target.value)}><option value="HOLD_LAST">HOLD_LAST · retain last device setpoint</option><option value="SAFE_SETPOINT">SAFE_SETPOINT · attempt configured mA</option></select></div>
          {form.config.fail_safe_policy === 'SAFE_SETPOINT' && <div className="field"><label htmlFor="safe-point">Safe setpoint (mA)</label><input id="safe-point" className="input" type="number" min={0} max={20} step={0.01} value={Number(form.config.safe_setpoint_ma ?? 4)} onChange={e => setConfig('safe_setpoint_ma', Number(e.target.value))} /></div>}
        </div>
        {form.source_value_type === 'ENGINEERING' && form.config.transformation === 'DIRECT' && <p className="error">Engineering values need an explicit conversion into mA.</p>}
        <p className="panel-note" style={{ marginTop: 10 }}>{selectedSource?.name || 'Input'} AI{form.source_channel} → Calibration → Transformation → FC16 → {selectedDestination?.name || 'Output'} AO{form.destination_channel}</p>
        <div className="form-actions"><button type="button" className="button" onClick={() => { setEditing(null); setForm(initial); setError('') }}>Reset form</button><button className="button primary" disabled={save.isPending || !sourceNodes.length || !destinationNodes.length || (form.source_value_type === 'ENGINEERING' && form.config.transformation === 'DIRECT')}>{save.isPending ? 'Saving…' : editing ? 'Save changes' : 'Create disabled pipeline'}</button></div>
      </form>
    </section>

    <section className="panel" style={{ marginTop: 18 }}>
      <div className="panel-head"><div><div className="panel-title">Registered pipelines</div><div className="panel-note" style={{ marginTop: 3 }}>Only armed pipelines send output commands.</div></div></div>
      {pipelines.isLoading ? <div className="empty"><strong>Loading pipelines</strong>Reading persistent backend configuration.</div> : pipelines.isError ? <div className="empty"><strong>Could not load pipelines</strong>{(pipelines.error as Error).message}</div> : rows.length ? <div className="table-wrap"><table><thead><tr><th>Name</th><th>Source</th><th>Value</th><th>Destination</th><th>Target / confirmed</th><th>Status</th><th>Last update</th><th>Actions</th></tr></thead><tbody>{rows.map(row => <tr key={row.id}>
        <td><button className="text-button" onClick={() => setDetail(row.id)}>{row.name}</button></td>
        <td>{channelNames.get(row.source_node_id) || row.source_node_id.slice(0, 8)} · AI{row.source_channel}</td>
        <td>{row.source_value_type.replace('_', ' ')}</td>
        <td>{channelNames.get(row.destination_node_id) || row.destination_node_id.slice(0, 8)} · AO{row.destination_channel}</td>
        <td>{fixed(row.last_requested_value)} / {fixed(row.last_acknowledged_value)}</td>
        <td><span className={`state ${statusTone(row.status)}`}><i className="status-dot" />{row.armed ? 'ARMED · ' : ''}{row.status}</span></td>
        <td>{date(row.last_success_at)}</td>
        <td><div className="action-row"><button className="button small" onClick={() => setDetail(row.id)}>Details</button>{!row.enabled && <button className="button small" onClick={() => control(row, 'enable')}>Enable</button>}{row.enabled && !row.armed && <button className="button small primary" onClick={() => control(row, 'arm')}>Arm</button>}{row.armed && <button className="button small" onClick={() => control(row, 'disarm')}>Disarm</button>}{row.enabled && <button className="button small" onClick={() => control(row, 'disable')}>Disable</button>}<button className="button small" disabled={row.enabled || row.armed} onClick={() => beginEdit(row)}>Edit</button><button className="button small danger" disabled={row.armed} onClick={() => window.confirm(`Delete pipeline “${row.name}”?`) && remove.mutate(row.id)}>Delete</button></div></td>
      </tr>)}</tbody></table></div> : <div className="empty"><strong>No pipelines configured</strong>Create a disabled pipeline, then verify devices and explicitly enable and arm it.</div>}
    </section>

    {detailRow && <div className="modal-backdrop" onMouseDown={event => event.target === event.currentTarget && setDetail(null)}><section className="modal" role="dialog" aria-modal="true" aria-label={`${detailRow.name} details`}><div className="modal-head"><strong>{detailRow.name}</strong><button className="icon-button" onClick={() => setDetail(null)} aria-label="Close details">×</button></div><div className="modal-body">
      <div className="notice info">{channelNames.get(detailRow.source_node_id) || detailRow.source_node_id} AI{detailRow.source_channel} → Calibration → {String(detailRow.config.transformation || 'DIRECT')} → FC16 → {channelNames.get(detailRow.destination_node_id) || detailRow.destination_node_id} AO{detailRow.destination_channel}</div>
      <div className="health-list"><div className="health-line"><span>Runtime state</span><strong>{detailRow.status}{detailRow.armed ? ' · ARMED' : ''}</strong></div><div className="health-line"><span>Source gateway</span><strong>{sourceDetail?.gateway_state || 'UNKNOWN'} · Modbus {sourceDetail?.modbus_health || 'UNKNOWN'}</strong></div><div className="health-line"><span>Source sample</span><strong>{sourceMeasurement ? `${sourceMeasurement.quality} · ${date(sourceMeasurement.measured_at)}` : 'No measurement available'}</strong></div><div className="health-line"><span>Raw / calibrated input</span><strong>{sourceMeasurement ? `${fixed(sourceMeasurement.raw_ma)} / ${fixed(sourceMeasurement.calibrated_ma)}` : fixed(detailRow.last_source_value)}</strong></div><div className="health-line"><span>Output gateway</span><strong>{destinationDetail?.gateway_state || 'UNKNOWN'} · Modbus {destinationDetail?.modbus_health || 'UNKNOWN'}</strong></div><div className="health-line"><span>Transformed target</span><strong>{fixed(detailRow.last_requested_value)}</strong></div><div className="health-line"><span>Last acknowledged setpoint readback</span><strong>{fixed(detailRow.last_acknowledged_value)}</strong></div><div className="health-line"><span>Last confirmed update</span><strong>{date(detailRow.last_success_at)}</strong></div></div>
      {detailRow.last_error && <p className="error" role="alert">{detailRow.last_error}</p>}
      <h3>Recent output commands</h3>{history.isLoading ? <p className="panel-note">Loading command history…</p> : history.data?.length ? <div className="table-wrap"><table><thead><tr><th>Time</th><th>Source</th><th>Target</th><th>Readback</th><th>Result</th><th>Detail</th></tr></thead><tbody>{history.data.map((item, index) => <tr key={String(item.id || index)}><td>{date(String(item.at || ''))}</td><td>{String(item.source || '')}</td><td>{fixed(item.target_ma == null ? null : Number(item.target_ma))}</td><td>{fixed(item.readback_ma == null ? null : Number(item.readback_ma))}</td><td>{String(item.result || '')}</td><td>{String(item.detail || '')}</td></tr>)}</tbody></table></div> : <p className="panel-note">No pipeline output commands have been issued.</p>}
    </div></section></div>}
    {(action.isPending || remove.isPending) && <p className="panel-note" role="status">Applying pipeline change…</p>}
  </>
}
