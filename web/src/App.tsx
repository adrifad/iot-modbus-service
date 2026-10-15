import { FormEvent, ReactNode, useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Activity, AlertTriangle, ArrowDownToLine, AudioLines, BookOpenCheck, Cable, Check, ChevronDown,
  ChevronRight, CircleHelp, Clock3, Command, Cpu, Database, FileClock, Gauge, GitBranch, LayoutDashboard,
  ListFilter, LoaderCircle, Menu, Moon, Plus, Radio, RefreshCw, Search, Settings2, Shield, SlidersHorizontal,
  Sun, TerminalSquare, X,
} from 'lucide-react'
import { api, EventRow, Gateway, getToken, Measurement, Node, Overview, patch, PollItem, post, put, setToken } from './api'

type Page = 'Overview' | 'Input Nodes' | 'Output Nodes' | 'Modbus Polling' | 'Live Telemetry' | 'Calibration' | 'I/O Mapping' | 'Communication Logs' | 'Settings'

const navigation: { label: Page; icon: typeof Activity; section: string }[] = [
  { label: 'Overview', icon: LayoutDashboard, section: 'Monitor' },
  { label: 'Input Nodes', icon: AudioLines, section: 'Monitor' },
  { label: 'Output Nodes', icon: Cable, section: 'Monitor' },
  { label: 'Modbus Polling', icon: SlidersHorizontal, section: 'Configure' },
  { label: 'Live Telemetry', icon: Activity, section: 'Configure' },
  { label: 'Calibration', icon: Gauge, section: 'Configure' },
  { label: 'I/O Mapping', icon: GitBranch, section: 'Configure' },
  { label: 'Communication Logs', icon: FileClock, section: 'System' },
  { label: 'Settings', icon: Settings2, section: 'System' },
]

const categories = ['Analog Measurements', 'Device Information', 'Analog Configuration', 'Trigger Configuration', 'Network Configuration', 'Custom Registers']
const fmtTime = (value?: string | null) => value ? new Date(value).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' }) : 'No successful read'
const relativeTime = (value?: string | null) => {
  if (!value) return 'No data'
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000))
  return seconds < 60 ? `${seconds}s ago` : seconds < 3600 ? `${Math.floor(seconds / 60)}m ago` : `${Math.floor(seconds / 3600)}h ago`
}
const labelForNode = (node?: Node) => node ? `${node.name} · ${node.hardware_model || node.node_type.replace('_', ' ')}` : 'Select node'

function Status({ good, warning, children }: { good?: boolean; warning?: boolean; children: ReactNode }) {
  return <span className={`state ${good ? 'good' : warning ? 'warning' : ''}`}><i className="status-dot" />{children}</span>
}

function PageHeading({ title, description, right }: { title: string; description: string; right?: ReactNode }) {
  return <div className="page-head"><div><h1>{title}</h1><p>{description}</p></div>{right}</div>
}

function Panel({ title, note, action, children }: { title: string; note?: string; action?: ReactNode; children: ReactNode }) {
  return <section className="panel"><div className="panel-head"><div><div className="panel-title">{title}</div>{note && <div className="panel-note" style={{ marginTop: 3 }}>{note}</div>}</div>{action}</div>{children}</section>
}

function Empty({ title, children }: { title: string; children?: ReactNode }) {
  return <div className="empty"><strong>{title}</strong>{children}</div>
}

function Modal({ title, onClose, children }: { title: string; onClose: () => void; children: ReactNode }) {
  useEffect(() => { const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose(); window.addEventListener('keydown', onKey); return () => window.removeEventListener('keydown', onKey) }, [onClose])
  return <div className="modal-backdrop" onMouseDown={e => e.target === e.currentTarget && onClose()}><div className="modal" role="dialog" aria-modal="true" aria-label={title}><div className="modal-head"><strong>{title}</strong><button className="icon-button" aria-label="Close dialog" onClick={onClose}><X size={16} /></button></div><div className="modal-body">{children}</div></div></div>
}

function Login({ onLogin }: { onLogin: (token: string) => void }) {
  const [value, setValue] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  async function submit(e: FormEvent) {
    e.preventDefault(); setBusy(true); setError('')
    setToken(value)
    try { await api('/api/overview'); onLogin(value) }
    catch (err) { setToken(''); setError(err instanceof Error ? err.message : 'Could not connect to the service') }
    finally { setBusy(false) }
  }
  return <main className="login-wrap"><section className="login-panel"><div className="brand" style={{ padding: 0 }}><span className="brand-mark"><AudioLines size={18} /></span>Fieldline <span style={{ color: 'var(--muted)', fontWeight: 450 }}>I/O Monitor</span></div><h1>Connect to your service</h1><p>Enter the administrator API token configured on the backend. The token is kept in this browser for this session.</p><form onSubmit={submit}><div className="field"><label htmlFor="token">Administrator token</label><input id="token" className="input" type="password" value={value} onChange={e => setValue(e.target.value)} autoComplete="current-password" required /></div>{error && <p className="error" role="alert">{error}</p>}<button className="button primary" style={{ width: '100%', marginTop: 14 }} disabled={busy}>{busy ? <LoaderCircle size={15} className="spin" /> : <Shield size={15} />} Connect</button></form></section></main>
}

function App() {
  const queryClient = useQueryClient()
  const [token, updateToken] = useState(getToken())
  const [page, setPage] = useState<Page>('Overview')
  const [mobileOpen, setMobileOpen] = useState(false)
  const [theme, setTheme] = useState(localStorage.getItem('fieldline-theme') || 'light')
  useEffect(() => { document.documentElement.dataset.theme = theme; localStorage.setItem('fieldline-theme', theme) }, [theme])
  useEffect(() => { const clear = () => updateToken(''); window.addEventListener('fieldline-auth-expired', clear); return () => window.removeEventListener('fieldline-auth-expired', clear) }, [])
  const health = useQuery({ queryKey: ['health', token], queryFn: () => api<{ service: string }>('/api/health'), enabled: !!token, refetchInterval: 5000, retry: false })
  if (!token) return <Login onLogin={value => updateToken(value)} />
  return <div className="app-shell">
    <button className={`sidebar-backdrop ${mobileOpen ? 'open' : ''}`} aria-label="Close navigation" onClick={() => setMobileOpen(false)} />
    <aside className={`sidebar ${mobileOpen ? 'open' : ''}`}>
      <div className="brand"><span className="brand-mark"><AudioLines size={17} /></span><span>Fieldline</span><span style={{ color: 'var(--muted)', fontSize: 11, fontWeight: 450 }}>I/O</span></div>
      {['Monitor', 'Configure', 'System'].map(section => <div key={section}><div className="nav-label">{section}</div>{navigation.filter(item => item.section === section).map(item => { const Icon = item.icon; return <button key={item.label} className={`nav-item ${page === item.label ? 'active' : ''}`} onClick={() => { setPage(item.label); setMobileOpen(false) }}><Icon size={16} strokeWidth={1.8} />{item.label}</button> })}</div>)}
      <div className="sidebar-foot">Gateway TCP · Port 5001<br />Modbus writes disabled</div>
    </aside>
    <div className="workspace">
      <header className="topbar"><div style={{ display: 'flex', alignItems: 'center', gap: 12 }}><button className="icon-button mobile-menu" aria-label="Open navigation" onClick={() => setMobileOpen(true)}><Menu size={17} /></button><span className="crumb">Fieldline <ChevronRight size={12} style={{ verticalAlign: 'middle', margin: '0 5px' }} /> {page}</span></div><div className="top-actions"><span className="sync-status"><i className={`status-dot ${health.isSuccess ? 'good' : ''}`} /><span className="sync-label">Backend {health.isSuccess ? 'connected' : health.isPending ? 'checking' : 'disconnected'}</span></span><button className="icon-button" aria-label={`Switch to ${theme === 'light' ? 'dark' : 'light'} theme`} onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}>{theme === 'light' ? <Moon size={16} /> : <Sun size={16} />}</button><button className="icon-button" aria-label="Disconnect" title="Disconnect" onClick={() => { setToken(''); updateToken(''); queryClient.clear() }}><X size={16} /></button></div></header>
      <main className="content"><div className="signal-rail" aria-hidden="true"><span />{Array.from({ length: 12 }, (_, i) => <span key={i} />)}</div>
        {page === 'Overview' && <OverviewPage onNavigate={setPage} />}
        {page === 'Input Nodes' && <NodesPage type="ANALOG_INPUT" />}
        {page === 'Output Nodes' && <NodesPage type="ANALOG_OUTPUT" />}
        {page === 'Modbus Polling' && <PollingPage />}
        {page === 'Live Telemetry' && <TelemetryPage />}
        {page === 'Calibration' && <CalibrationPage />}
        {page === 'I/O Mapping' && <MappingPage />}
        {page === 'Communication Logs' && <LogsPage />}
        {page === 'Settings' && <SettingsPage theme={theme} setTheme={setTheme} />}
      </main>
    </div>
  </div>
}

function OverviewPage({ onNavigate }: { onNavigate: (page: Page) => void }) {
  const query = useQuery({ queryKey: ['overview'], queryFn: () => api<Overview>('/api/overview'), refetchInterval: 3000 })
  const data = query.data
  if (query.isError) return <><PageHeading title="Overview" description="Live gateway, Modbus and measurement status." /><Empty title="Backend data is unavailable">{(query.error as Error).message}. Check the API connection, then refresh.</Empty><button className="button" onClick={() => query.refetch()}><RefreshCw size={14} /> Retry</button></>
  return <>
    <PageHeading title="Overview" description="Gateway links and Modbus health are reported separately." right={<span className="head-meta">Updated {data ? relativeTime(data.last_measurement_at) : '—'}</span>} />
    <div className="metrics">
      <Metric label="Input nodes" value={data?.inputs.total ?? '—'} detail={`${data?.inputs.connected ?? 0} connected · ${data?.inputs.disconnected ?? 0} disconnected`} icon={<AudioLines size={15} />} />
      <Metric label="Output nodes" value={data?.outputs.total ?? '—'} detail={`${data?.outputs.connected ?? 0} connected · ${data?.outputs.disconnected ?? 0} disconnected`} icon={<Cable size={15} />} />
      <Metric label="Poll items active" value={data?.polling.active ?? '—'} detail={`${data?.polling.failed ?? 0} errors recorded`} icon={<Activity size={15} />} />
      <Metric label="Average response" value={data?.polling.average_response_ms == null ? '—' : `${data.polling.average_response_ms.toFixed(1)} ms`} detail={`${data?.polling.successful ?? 0} valid responses`} icon={<Clock3 size={15} />} />
    </div>
    <div className="split-grid">
      <Panel title="Registered nodes" note="Gateway presence does not confirm a Modbus response." action={<button className="button small" onClick={() => onNavigate('Input Nodes')}>View nodes <ChevronRight size={13} /></button>}>
        {data?.nodes.length ? <div className="table-wrap"><table><thead><tr><th>Node</th><th>Gateway</th><th>Slave</th><th>Gateway</th><th>Modbus</th><th>Last measurement</th></tr></thead><tbody>{data.nodes.slice(0, 8).map(node => <tr key={node.id}><td><div className="primary-name">{node.name}</div><div className="secondary">{node.hardware_model || node.node_type.replace('_', ' ')}</div></td><td>{node.gateway_id?.slice(0, 8) ?? 'Unassigned'}</td><td>{node.slave_id}</td><td><Status good={node.gateway_connected}>{node.gateway_connected ? 'Connected' : 'Offline'}</Status></td><td><Status good={node.modbus_healthy} warning={!node.modbus_healthy && !!node.gateway_connected}>{node.modbus_healthy ? 'Responding' : node.gateway_connected ? 'No response' : 'Unknown'}</Status></td><td>{relativeTime(node.last_measurement_at)}</td></tr>)}</tbody></table></div> : <Empty title="No nodes registered">Add a gateway and input node to begin commissioning.</Empty>}
      </Panel>
      <div style={{ display: 'grid', gap: 18 }}>
        <Panel title="System health" note="Current backend and transport state"><div className="panel-body health-list"><HealthLine label="Backend service" value="Connected" good /><HealthLine label="Gateway sessions" value={`${data?.gateway_connections.connected ?? 0} / ${data?.gateway_connections.total ?? 0}`} good={!!data?.gateway_connections.connected} /><HealthLine label="Polling scheduler" value={data?.polling.state ?? 'Checking'} good={data?.polling.state === 'RUNNING'} /><HealthLine label="Latest measurement" value={relativeTime(data?.last_measurement_at)} good={!!data?.last_measurement_at} /></div></Panel>
        <Panel title="Recent communication" action={<button className="button small" onClick={() => onNavigate('Communication Logs')}>Open logs <ChevronRight size={13} /></button>}>
          <div className="panel-body">{data?.recent_events.length ? data.recent_events.slice(0, 5).map(event => <EventLine key={event.id} event={event} />) : <Empty title="No communication events">Events appear after a gateway connects or a polling request runs.</Empty>}</div>
        </Panel>
      </div>
    </div>
    <div style={{ marginTop: 18 }}><Panel title="I/O readiness" note="No output commands are sent by this service."><div className="panel-body health-list"><HealthLine label="Input measurements" value={data?.inputs.total ? `${data.inputs.total} nodes configured` : 'Not configured'} good={!!data?.inputs.total} /><HealthLine label="Output integration" value="Planned · writes disabled" /></div></Panel></div>
  </>
}

function Metric({ label, value, detail, icon }: { label: string; value: string | number; detail: string; icon: ReactNode }) {
  return <div className="metric"><div className="metric-label"><span>{label}</span><span>{icon}</span></div><div className="metric-value">{value}<span className="metric-sub">{detail}</span></div></div>
}
function HealthLine({ label, value, good, warning }: { label: string; value: string; good?: boolean; warning?: boolean }) { return <div className="health-line"><span>{label}</span><Status good={good} warning={warning}>{value}</Status></div> }
function EventLine({ event }: { event: EventRow }) { return <div className="event-row"><span className="event-time">{new Date(event.at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}</span><span className="event-detail" title={event.detail}>{event.detail}</span><span className="event-result">{event.result}</span></div> }

function NodesPage({ type }: { type: Node['node_type'] }) {
  const queryClient = useQueryClient()
  const [search, setSearch] = useState('')
  const [showAdd, setShowAdd] = useState(false)
  const [selected, setSelected] = useState<Node | null>(null)
  const nodesQuery = useQuery({ queryKey: ['nodes', type], queryFn: () => api<Node[]>(`/api/nodes?type=${type}`) })
  const gatewaysQuery = useQuery({ queryKey: ['gateways'], queryFn: () => api<Gateway[]>('/api/gateways') })
  const nodes = (nodesQuery.data || []).filter(node => `${node.name} ${node.hardware_model} ${node.id}`.toLowerCase().includes(search.toLowerCase()))
  const mutation = useMutation({ mutationFn: (data: unknown) => post('/api/nodes', data), onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['nodes'] }); queryClient.invalidateQueries({ queryKey: ['overview'] }); setShowAdd(false) } })
  const title = type === 'ANALOG_INPUT' ? 'Input nodes' : 'Output nodes'
  return <>
    <PageHeading title={title} description={type === 'ANALOG_INPUT' ? 'Register and inspect Modbus analog input devices.' : 'Register output devices without assuming a hardware register map.'} right={<button className="button primary" onClick={() => setShowAdd(true)}><Plus size={15} /> Add node</button>} />
    <div className="toolbar"><div style={{ position: 'relative', width: 'min(320px,100%)' }}><Search size={14} style={{ position: 'absolute', left: 10, top: 11, color: 'var(--muted)' }} /><input className="input" style={{ paddingLeft: 32 }} placeholder="Search name, model or ID" value={search} onChange={e => setSearch(e.target.value)} /></div><span className="panel-note">{nodes.length} registered</span></div>
    <Panel title="Node registry" note="Identity is independent of gateway IP and TCP source port.">
      {nodesQuery.isLoading ? <Empty title="Loading node registry">Reading saved configuration.</Empty> : nodesQuery.isError ? <Empty title="Could not load nodes">{(nodesQuery.error as Error).message}</Empty> : nodes.length ? <div className="table-wrap"><table><thead><tr><th>Name</th><th>Hardware model</th><th>Gateway</th><th>Slave ID</th><th>Channels</th><th>Gateway session</th><th>Modbus</th><th>Polling</th><th>Last measurement</th></tr></thead><tbody>{nodes.map(node => <tr key={node.id} data-clickable="true" onClick={() => setSelected(node)}><td><div className="primary-name">{node.name}</div><div className="secondary">{node.id}</div></td><td>{node.hardware_model || 'Not specified'}</td><td>{node.gateway_id?.slice(0, 8) || 'Unassigned'}</td><td>{node.slave_id}</td><td>{node.enabled_channel_count ?? 0}</td><td><Status good={node.gateway_connected}>{node.gateway_connected ? 'Connected' : 'Disconnected'}</Status></td><td><Status good={node.modbus_healthy} warning={!!node.gateway_connected && !node.modbus_healthy}>{node.modbus_healthy ? 'Responding' : 'Unknown'}</Status></td><td>{node.polling_enabled ? 'Enabled' : 'Disabled'}</td><td>{relativeTime(node.last_measurement_at)}</td></tr>)}</tbody></table></div> : <Empty title={search ? 'No matching nodes' : 'No nodes registered'}>{search ? 'Try another search.' : 'Register this device to begin configuration.'}</Empty>}
    </Panel>
    {showAdd && <Modal title={`Register ${type === 'ANALOG_INPUT' ? 'input' : 'output'} node`} onClose={() => setShowAdd(false)}><form onSubmit={e => { e.preventDefault(); const form = new FormData(e.currentTarget); mutation.mutate({ name: form.get('name'), node_type: type, hardware_model: form.get('hardware_model'), gateway_id: form.get('gateway_id') || null, slave_id: Number(form.get('slave_id')), profile_id: type === 'ANALOG_INPUT' ? 'ebyte-me31-xaxx0600-v1.1' : null }) }}><div className="form-grid"><Field label="Display name"><input className="input" name="name" required maxLength={100} placeholder="Tank farm input 01" /></Field><Field label="Hardware model"><input className="input" name="hardware_model" defaultValue={type === 'ANALOG_INPUT' ? 'EBYTE ME31-XAXX0600' : ''} placeholder="Manufacturer and model" /></Field><Field label="Assigned gateway"><select className="select" name="gateway_id"><option value="">No gateway assigned</option>{gatewaysQuery.data?.map(gateway => <option value={gateway.id} key={gateway.id}>{gateway.name} · {gateway.transport_mode}</option>)}</select></Field><Field label="Modbus slave ID"><input className="input" name="slave_id" type="number" defaultValue="1" min="1" max="247" required /></Field></div>{mutation.isError && <p className="error">{(mutation.error as Error).message}</p>}<div className="form-actions"><button type="button" className="button" onClick={() => setShowAdd(false)}>Cancel</button><button className="button primary" disabled={mutation.isPending}>Save node</button></div></form></Modal>}
    {selected && <NodeDetails node={selected} onClose={() => setSelected(null)} />}
  </>
}

function NodeDetails({ node, onClose }: { node: Node; onClose: () => void }) {
  const queryClient = useQueryClient()
  const [name, setName] = useState(node.name)
  const [model, setModel] = useState(node.hardware_model)
  const [slaveId, setSlaveId] = useState(String(node.slave_id))
  const [gatewayId, setGatewayId] = useState(node.gateway_id || '')
  const [enabled, setEnabled] = useState(!!node.polling_enabled)
  const mutation = useMutation({ mutationFn: (data: unknown) => patch(`/api/nodes/${node.id}`, data), onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['nodes'] }); queryClient.invalidateQueries({ queryKey: ['overview'] }); onClose() } })
  const polling = useQuery({ queryKey: ['polling', node.id], queryFn: () => api<PollItem[]>(`/api/nodes/${node.id}/polling`) })
  const channels = useQuery({ queryKey: ['channels', node.id], queryFn: () => api<any[]>(`/api/nodes/${node.id}/channels`) })
  const gateways = useQuery({ queryKey: ['gateways'], queryFn: () => api<Gateway[]>('/api/gateways') })
  const addChannel = useMutation({ mutationFn: (data: unknown) => post(`/api/nodes/${node.id}/channels`, data), onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['channels', node.id] }); queryClient.invalidateQueries({ queryKey: ['overview'] }) } })
  return <Modal title={node.name} onClose={onClose}><div className="notice info">Persistent node identity <code>{node.id}</code></div><div className="form-grid"><Field label="Display name"><input className="input" value={name} onChange={e => setName(e.target.value)} /></Field><Field label="Hardware model"><input className="input" value={model} onChange={e => setModel(e.target.value)} /></Field><Field label="Assigned gateway"><select className="select" value={gatewayId} onChange={e => setGatewayId(e.target.value)}><option value="">Unassigned</option>{gateways.data?.map(gateway => <option value={gateway.id} key={gateway.id}>{gateway.name}</option>)}</select></Field><Field label="Modbus slave ID"><input className="input" type="number" min="1" max="247" value={slaveId} onChange={e => setSlaveId(e.target.value)} /></Field><Field label="Node polling"><select className="select" value={enabled ? 'enabled' : 'disabled'} onChange={e => setEnabled(e.target.value === 'enabled')}><option value="enabled">Enabled</option><option value="disabled">Disabled</option></select></Field></div><div style={{ marginTop: 18, borderTop: '1px solid var(--line)', paddingTop: 14 }}><div className="panel-title">Configured channels · {channels.data?.length ?? '…'}</div>{channels.data?.length ? channels.data.map(item => <div className="health-line" key={item.id}><span>{item.label} · channel {item.channel_index}</span><span>{item.enabled ? 'Enabled' : 'Disabled'}</span></div>) : <p className="panel-note">No output channels are configured.</p>}{node.node_type === 'ANALOG_OUTPUT' && <form onSubmit={e => { e.preventDefault(); const form = new FormData(e.currentTarget); addChannel.mutate({ channel_index: Number(form.get('channel_index')), label: form.get('label') }) }} style={{ display: 'flex', gap: 7, marginTop: 10 }}><input className="input" type="number" name="channel_index" min="1" max="65535" placeholder="Output index" required /><input className="input" name="label" placeholder="Channel label" required maxLength={80} /><button className="button" disabled={addChannel.isPending}>Add channel</button></form>}{addChannel.isError && <p className="error">{(addChannel.error as Error).message}</p>}</div><p className="panel-note" style={{ marginTop: 14 }}>{polling.data?.length ?? '…'} polling items · last measurement {relativeTime(node.last_measurement_at)}</p><div className="form-actions"><button className="button primary" onClick={() => mutation.mutate({ name, hardware_model: model, gateway_id: gatewayId || null, slave_id: Number(slaveId), polling_enabled: enabled })} disabled={mutation.isPending}>Save changes</button></div>{mutation.isError && <p className="error">{(mutation.error as Error).message}</p>}</Modal>
}

function Field({ label, children }: { label: string; children: ReactNode }) { return <div className="field"><label>{label}</label>{children}</div> }

function PollingPage() {
  const queryClient = useQueryClient()
  const nodesQuery = useQuery({ queryKey: ['nodes', 'ANALOG_INPUT'], queryFn: () => api<Node[]>('/api/nodes?type=ANALOG_INPUT') })
  const profileQuery = useQuery({ queryKey: ['profiles'], queryFn: () => api<{ id: string; registers: any[] }[]>('/api/device-profiles') })
  const nodes = nodesQuery.data || []
  const [nodeId, setNodeId] = useState('')
  const activeNode = nodes.find(node => node.id === nodeId) || nodes[0]
  const [openGroups, setOpenGroups] = useState<string[]>(['Analog Measurements'])
  const pollingQuery = useQuery({ queryKey: ['polling', activeNode?.id], queryFn: () => api<PollItem[]>(`/api/nodes/${activeNode!.id}/polling`), enabled: !!activeNode, refetchInterval: 3000 })
  const addMutation = useMutation({ mutationFn: (data: unknown) => post(`/api/nodes/${activeNode!.id}/polling`, data), onSuccess: () => queryClient.invalidateQueries({ queryKey: ['polling', activeNode?.id] }) })
  const toggleMutation = useMutation({ mutationFn: ({ id, enabled }: { id: string; enabled: boolean }) => patch(`/api/polling/${id}`, { enabled }), onSuccess: () => queryClient.invalidateQueries({ queryKey: ['polling', activeNode?.id] }) })
  const groupMutation = useMutation({ mutationFn: ({ group, enabled }: { group: string; enabled: boolean }) => put(`/api/nodes/${activeNode!.id}/polling/groups/${encodeURIComponent(group)}?enabled=${enabled}`, {}), onSuccess: () => queryClient.invalidateQueries({ queryKey: ['polling', activeNode?.id] }) })
  const onceMutation = useMutation({ mutationFn: (id: string) => post(`/api/polling/${id}/read`, {}), onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['polling', activeNode?.id] }); queryClient.invalidateQueries({ queryKey: ['overview'] }) } })
  const deleteMutation = useMutation({ mutationFn: (id: string) => api(`/api/polling/${id}`, { method: 'DELETE' }), onSuccess: () => queryClient.invalidateQueries({ queryKey: ['polling', activeNode?.id] }) })
  const editMutation = useMutation({ mutationFn: ({ id, values }: { id: string; values: unknown }) => patch(`/api/polling/${id}`, values), onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['polling', activeNode?.id] }); setEditItem(null) } })
  const definitions = profileQuery.data?.flatMap(profile => profile.registers.filter(item => item.readable && item.quantity).map(item => ({ ...item, profile_id: profile.id }))) || []
  const available = definitions.filter(def => !pollingQuery.data?.some(item => item.register_name === def.name))
  const [editItem, setEditItem] = useState<PollItem | null>(null)
  const [customRegisterOpen, setCustomRegisterOpen] = useState(false)
  return <>
    <PageHeading title="Modbus polling" description="Saved items are scheduled by the backend and serialized per gateway session." right={<span className="head-meta">Minimum interval 250 ms</span>} />
    {nodes.length ? <><div className="toolbar"><Field label="Input node"><select className="select" value={activeNode?.id || ''} onChange={e => setNodeId(e.target.value)}>{nodes.map(node => <option value={node.id} key={node.id}>{labelForNode(node)}</option>)}</select></Field><span className="panel-note">Slave {activeNode?.slave_id} · {activeNode?.polling_enabled ? 'Node polling enabled' : 'Node polling disabled'}</span></div>
      {categories.map(category => {
        const items = (pollingQuery.data || []).filter(item => definitions.find(def => def.name === item.register_name)?.category === category || (!definitions.some(def => def.name === item.register_name) && category === 'Custom Registers'))
        const groupAvailable = available.filter(def => def.category === category)
        const isOpen = openGroups.includes(category)
        return <section className="panel" style={{ marginBottom: 12 }} key={category}>
          <div className="panel-head"><button className="nav-item" style={{ width: 'auto', margin: 0, padding: '7px 0', color: 'var(--ink)' }} onClick={() => setOpenGroups(isOpen ? openGroups.filter(x => x !== category) : [...openGroups, category])}>{isOpen ? <ChevronDown size={15} /> : <ChevronRight size={15} />}<span className="panel-title">{category}</span><span className="panel-note">{items.length} configured</span></button><div style={{ display: 'flex', alignItems: 'center', gap: 10 }}><button className="button small" onClick={() => groupMutation.mutate({ group: category, enabled: true })} disabled={!items.length}>Enable group</button><button className="button small" onClick={() => groupMutation.mutate({ group: category, enabled: false })} disabled={!items.length}>Disable group</button>{category === 'Custom Registers' && <button className="button small" onClick={() => setCustomRegisterOpen(true)}><Plus size={13} /> Add custom</button>}{groupAvailable.length > 0 && <select className="select" style={{ width: 180, minHeight: 28, padding: '4px 7px' }} value="" aria-label={`Add ${category} polling item`} onChange={e => { const def = definitions.find(x => x.name === e.target.value); if (def) addMutation.mutate({ profile_id: def.profile_id, register_name: def.name, function_code: def.function_code, address: def.address, quantity: def.quantity, data_type: def.data_type, byte_order: def.byte_order || 'big', word_order: def.word_order || 'unverified', interval_ms: def.default_interval_ms || 1000, timeout_ms: 1500, retries: 1, scale: def.scale ?? 1, unit: def.unit || '', enabled: def.default_enabled !== false }) }}><option value="">Add profile item…</option>{groupAvailable.map(def => <option value={def.name} key={def.name}>{def.name} · 0x{def.address.toString(16).toUpperCase()}</option>)}</select>}</div></div>
          {isOpen && (items.length ? <div className="table-wrap"><table><thead><tr><th>Register</th><th>Function</th><th>Start</th><th>Quantity</th><th>Interval</th><th>Last value</th><th>Last response</th><th>State</th><th>Actions</th></tr></thead><tbody>{items.map(item => <tr key={item.id}><td><div className="primary-name">{item.register_name}</div><div className="secondary">{item.data_type} · {item.unit || 'raw'}</div></td><td>FC{String(item.function_code).padStart(2, '0')}</td><td>0x{item.address.toString(16).toUpperCase().padStart(4, '0')}</td><td>{item.quantity}</td><td>{item.interval_ms} ms</td><td>{item.last_value ?? '—'}</td><td>{fmtTime(item.last_success_at)}</td><td><Status good={item.last_status === 'GOOD'} warning={item.last_status === 'TIMEOUT'}>{item.enabled ? item.last_status : 'DISABLED'}</Status></td><td><div style={{ display: 'flex', gap: 5 }}><button className="button small" onClick={() => onceMutation.mutate(item.id)} disabled={onceMutation.isPending || !activeNode?.polling_enabled}>Read once</button><button className="button small" onClick={() => toggleMutation.mutate({ id: item.id, enabled: !item.enabled })}>{item.enabled ? 'Disable' : 'Enable'}</button><button className="button small" onClick={() => setEditItem(item)}>Edit</button><button className="button small danger" onClick={() => deleteMutation.mutate(item.id)}>Remove</button></div></td></tr>)}</tbody></table></div> : <Empty title={pollingQuery.isLoading ? 'Loading polling configuration' : 'No items in this group'}>{groupAvailable.length ? 'Add a register from the selected device profile.' : 'No pollable profile items are available in this group.'}</Empty>)}
        </section>
      })}
      {(addMutation.isError || groupMutation.isError || onceMutation.isError) && <p className="error">{((addMutation.error || groupMutation.error || onceMutation.error) as Error).message}</p>}
      {customRegisterOpen && <Modal title="Add custom read register" onClose={() => setCustomRegisterOpen(false)}><p className="panel-note">Read-only custom polling item. Confirm the address and register format against the device documentation.</p><form onSubmit={e => { e.preventDefault(); const form = new FormData(e.currentTarget); addMutation.mutate({ register_name: String(form.get('register_name')), function_code: Number(form.get('function_code')), address: Number(form.get('address')), quantity: Number(form.get('quantity')), data_type: String(form.get('data_type')), byte_order: 'big', word_order: 'unverified', interval_ms: Number(form.get('interval_ms')), timeout_ms: 1500, retries: 1, scale: Number(form.get('scale')), unit: String(form.get('unit')), enabled: true }, { onSuccess: () => setCustomRegisterOpen(false) }) }}><div className="form-grid"><Field label="Register name"><input className="input" name="register_name" maxLength={120} required /></Field><Field label="Function code"><select className="select" name="function_code"><option value="3">FC03 · Holding registers</option><option value="4">FC04 · Input registers</option></select></Field><Field label="Start address"><input className="input" name="address" type="number" min="0" max="65535" defaultValue="0" required /></Field><Field label="Quantity"><input className="input" name="quantity" type="number" min="1" max="125" defaultValue="1" required /></Field><Field label="Data type"><select className="select" name="data_type"><option value="uint16">Unsigned 16-bit</option><option value="int16">Signed 16-bit</option><option value="uint16_array">Unsigned 16-bit array</option><option value="raw">Raw registers</option></select></Field><Field label="Poll interval (ms)"><input className="input" name="interval_ms" type="number" min="250" max="86400000" defaultValue="1000" required /></Field><Field label="Value scale"><input className="input" name="scale" type="number" step="any" defaultValue="1" required /></Field><Field label="Engineering unit"><input className="input" name="unit" maxLength={32} /></Field></div>{addMutation.isError && <p className="error">{(addMutation.error as Error).message}</p>}<div className="form-actions"><button type="button" className="button" onClick={() => setCustomRegisterOpen(false)}>Cancel</button><button className="button primary" disabled={addMutation.isPending}>Save polling item</button></div></form></Modal>}
      {editItem && <Modal title={`Edit ${editItem.register_name}`} onClose={() => setEditItem(null)}><form onSubmit={e => { e.preventDefault(); const form = new FormData(e.currentTarget); editMutation.mutate({ id: editItem.id, values: { interval_ms: Number(form.get('interval_ms')), timeout_ms: Number(form.get('timeout_ms')), retries: Number(form.get('retries')) } }) }}><div className="form-grid"><Field label="Interval (ms)"><input className="input" name="interval_ms" type="number" min="250" max="86400000" defaultValue={editItem.interval_ms} /></Field><Field label="Timeout (ms)"><input className="input" name="timeout_ms" type="number" min="100" max="60000" defaultValue={editItem.timeout_ms} /></Field><Field label="Retry count"><input className="input" name="retries" type="number" min="0" max="5" defaultValue={editItem.retries} /></Field></div>{editMutation.isError && <p className="error">{(editMutation.error as Error).message}</p>}<div className="form-actions"><button type="button" className="button" onClick={() => setEditItem(null)}>Cancel</button><button className="button primary" disabled={editMutation.isPending}>Save polling item</button></div></form></Modal>}
    </> : <Empty title="No input nodes to poll">Register an input node and assign it to a gateway first.</Empty>}
  </>
}

function TelemetryPage() {
  const nodesQuery = useQuery({ queryKey: ['nodes', 'ANALOG_INPUT'], queryFn: () => api<Node[]>('/api/nodes?type=ANALOG_INPUT') })
  const nodes = nodesQuery.data || []
  const [nodeId, setNodeId] = useState('')
  const [range, setRange] = useState('15m')
  const [paused, setPaused] = useState(false)
  const activeNode = nodes.find(node => node.id === nodeId) || nodes[0]
  const since = useMemo(() => { const minutes = range === '1h' ? 60 : range === '6h' ? 360 : range === '24h' ? 1440 : 15; return new Date(Date.now() - minutes * 60000).toISOString() }, [range])
  const history = useQuery({ queryKey: ['history', activeNode?.id, range], queryFn: () => api<Measurement[]>(`/api/nodes/${activeNode!.id}/measurements/history?since=${encodeURIComponent(since)}&limit=5000`), enabled: !!activeNode, refetchInterval: paused ? false : 3000 })
  const latest = useQuery({ queryKey: ['measurements', activeNode?.id], queryFn: () => api<Measurement[]>(`/api/nodes/${activeNode!.id}/measurements/latest`), enabled: !!activeNode, refetchInterval: paused ? false : 2000 })
  const measurements = latest.data || []
  function exportCsv() {
    const rows = history.data || []
    const csv = ['timestamp,channel,register_value,raw_ma,calibrated_ma,engineering_value,engineering_unit,quality', ...rows.map(row => [row.measured_at, row.channel, row.register_value, row.raw_ma, row.calibrated_ma, row.engineering_value ?? '', row.engineering_unit ?? '', row.quality].join(','))].join('\n')
    const link = document.createElement('a'); link.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' })); link.download = `${activeNode?.name || 'telemetry'}-${range}.csv`; link.click(); URL.revokeObjectURL(link.href)
  }
  return <>
    <PageHeading title="Live telemetry" description="Raw register, calibrated current and engineering value remain distinct." />
    {!nodes.length ? <Empty title="No input nodes registered">Live channels appear after a node has valid measurements.</Empty> : <>
      <div className="toolbar"><Field label="Input node"><select className="select" value={activeNode?.id || ''} onChange={e => setNodeId(e.target.value)}>{nodes.map(node => <option value={node.id} key={node.id}>{labelForNode(node)}</option>)}</select></Field><div className="field"><label>History window</label><div className="tabs">{[['15m','15 min'],['1h','1 hour'],['6h','6 hours'],['24h','24 hours']].map(([value,label]) => <button className={`tab ${range === value ? 'active' : ''}`} key={value} onClick={() => setRange(value)}>{label}</button>)}</div></div><button className="button" style={{ marginTop: 18 }} onClick={() => setPaused(!paused)}>{paused ? 'Resume updates' : 'Pause updates'}</button><button className="button" style={{ marginTop: 18 }} onClick={exportCsv} disabled={!history.data?.length}><ArrowDownToLine size={14} /> Export CSV</button></div>
      {history.isError && <p className="error">{(history.error as Error).message}</p>}
      {measurements.length ? <div className="channel-grid">{Array.from({ length: 6 }, (_, index) => { const channel = index + 1; const measurement = measurements.find(row => row.channel === channel); const points = (history.data || []).filter(row => row.channel === channel).sort((a,b) => new Date(a.measured_at).getTime() - new Date(b.measured_at).getTime()); return <ChannelCard key={channel} channel={channel} measurement={measurement} points={points} pollingEnabled={!!activeNode?.polling_enabled} /> })}</div> : <Panel title="Channel values"><Empty title={latest.isLoading ? 'Waiting for measurement data' : 'No valid measurements yet'}>A TCP connection alone does not establish Modbus health. Check the communication log after the first poll.</Empty></Panel>}
    </>}
  </>
}

function ChannelCard({ channel, measurement, points, pollingEnabled }: { channel: number; measurement?: Measurement; points: Measurement[]; pollingEnabled: boolean }) {
  const good = measurement?.quality === 'GOOD'
  return <section className="channel"><div className="channel-head"><span>AI{channel}</span><Status good={good} warning={measurement?.quality === 'STALE'}>{measurement?.quality || 'UNKNOWN'}</Status></div>{measurement?.engineering_value != null ? <><div className="channel-value">{measurement.engineering_value.toFixed(2)}</div><div className="channel-unit">{measurement.engineering_unit || 'Engineering value'}</div></> : <><div className="channel-value">{measurement?.calibrated_ma.toFixed(3) ?? '—'}</div><div className="channel-unit">mA calibrated</div></>}<Trend points={points} /><div className="channel-meta"><span>Raw {measurement?.raw_ma.toFixed(3) ?? '—'} mA</span><span>Reg {measurement?.register_value ?? '—'}</span><span>Updated {relativeTime(measurement?.measured_at)}</span><span>{pollingEnabled ? 'Polling on' : 'Polling off'}</span></div></section>
}

function Trend({ points }: { points: Measurement[] }) {
  if (points.length < 2) return <svg className="trend" viewBox="0 0 120 30" role="img" aria-label="Not enough history to plot"><path d="M0 26 H120" stroke="var(--line)" strokeWidth="1" /></svg>
  const values = points.map(point => point.calibrated_ma); const min = Math.min(...values); const max = Math.max(...values); const span = max - min || 1
  const first = new Date(points[0].measured_at).getTime(); const last = new Date(points[points.length - 1].measured_at).getTime(); const duration = last - first || 1
  let path = ''
  points.forEach((point, index) => { const x = ((new Date(point.measured_at).getTime() - first) / duration) * 120; const y = 27 - ((point.calibrated_ma - min) / span) * 23; const gap = index > 0 && new Date(point.measured_at).getTime() - new Date(points[index - 1].measured_at).getTime() > 90000; path += `${index === 0 || gap ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)} ` })
  return <svg className="trend" viewBox="0 0 120 30" role="img" aria-label={`${points.length} real measurements over selected period`}><path d={path} fill="none" stroke="var(--accent)" strokeWidth="1.8" vectorEffect="non-scaling-stroke" /></svg>
}

function CalibrationPage() {
  const queryClient = useQueryClient()
  const nodesQuery = useQuery({ queryKey: ['nodes', 'ANALOG_INPUT'], queryFn: () => api<Node[]>('/api/nodes?type=ANALOG_INPUT') })
  const nodes = nodesQuery.data || []
  const [nodeId, setNodeId] = useState('')
  const [channel, setChannel] = useState(1)
  const [method, setMethod] = useState<'GAIN_OFFSET' | 'TWO_POINT'>('GAIN_OFFSET')
  const [gain, setGain] = useState('1')
  const [offset, setOffset] = useState('0')
  const [raw1, setRaw1] = useState('4')
  const [ref1, setRef1] = useState('4')
  const [raw2, setRaw2] = useState('20')
  const [ref2, setRef2] = useState('20')
  const [scaleEnabled, setScaleEnabled] = useState(false)
  const [inputMin, setInputMin] = useState('4')
  const [inputMax, setInputMax] = useState('20')
  const [engineeringMin, setEngineeringMin] = useState('0')
  const [engineeringMax, setEngineeringMax] = useState('100')
  const [unit, setUnit] = useState('bar')
  const activeNode = nodes.find(node => node.id === nodeId) || nodes[0]
  const latestQuery = useQuery({ queryKey: ['measurements', activeNode?.id], queryFn: () => api<Measurement[]>(`/api/nodes/${activeNode!.id}/measurements/latest`), enabled: !!activeNode, refetchInterval: 2000 })
  const calibrationQuery = useQuery({ queryKey: ['calibrations', activeNode?.id], queryFn: () => api<any[]>(`/api/nodes/${activeNode!.id}/calibration?channel=${channel}`), enabled: !!activeNode })
  const measurement = latestQuery.data?.find(item => item.channel === channel)
  const calibration = calibrationQuery.data?.[0]
  useEffect(() => { if (calibration) { setGain(String(calibration.gain)); setOffset(String(calibration.offset_ma)); setScaleEnabled(!!calibration.scaling_json); if (calibration.scaling_json) { const config = JSON.parse(calibration.scaling_json); setInputMin(String(config.input_min_ma)); setInputMax(String(config.input_max_ma)); setEngineeringMin(String(config.engineering_min)); setEngineeringMax(String(config.engineering_max)); setUnit(config.unit) } } else { setGain('1'); setOffset('0'); setScaleEnabled(false) } }, [calibration?.revision, channel])
  const scaling = scaleEnabled ? { input_min_ma: Number(inputMin), input_max_ma: Number(inputMax), engineering_min: Number(engineeringMin), engineering_max: Number(engineeringMax), unit, enabled: true } : null
  const preview = useMutation({ mutationFn: (values: { gain: number; offset_ma: number }) => post<{ calibrated_ma: number; engineering_value: number | null }>('/api/calibration/preview', { raw_ma: measurement?.raw_ma ?? 0, ...values, enabled: true, scaling }) })
  const save = useMutation({ mutationFn: async () => {
    if (method === 'TWO_POINT') return post(`/api/nodes/${activeNode!.id}/calibration/two-point`, { channel, raw1: Number(raw1), reference1: Number(ref1), raw2: Number(raw2), reference2: Number(ref2), scaling })
    return put(`/api/nodes/${activeNode!.id}/calibration`, { channel, gain: Number(gain), offset_ma: Number(offset), enabled: true, method, scaling })
  }, onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['calibrations', activeNode?.id] }); queryClient.invalidateQueries({ queryKey: ['measurements', activeNode?.id] }) } })
  const reset = useMutation({ mutationFn: () => put(`/api/nodes/${activeNode!.id}/calibration`, { channel, gain: 1, offset_ma: 0, enabled: false, method: 'RESET', scaling: null }), onSuccess: () => queryClient.invalidateQueries({ queryKey: ['calibrations', activeNode?.id] }) })
  const revert = useMutation({ mutationFn: (revision: number) => post(`/api/nodes/${activeNode!.id}/calibration/${channel}/revert/${revision}`, {}), onSuccess: () => queryClient.invalidateQueries({ queryKey: ['calibrations', activeNode?.id] }) })
  const calibrationHistory = calibrationQuery.data || []
  return <>
    <PageHeading title="Channel calibration" description="Software correction is stored separately from raw register values." />
    {!nodes.length ? <Empty title="No input nodes registered">Add an input device before setting channel calibration.</Empty> : <>
      <div className="toolbar"><Field label="Input node"><select className="select" value={activeNode?.id || ''} onChange={e => setNodeId(e.target.value)}>{nodes.map(node => <option value={node.id} key={node.id}>{labelForNode(node)}</option>)}</select></Field><Field label="Channel"><select className="select" value={channel} onChange={e => setChannel(Number(e.target.value))}>{Array.from({ length: 6 }, (_, i) => <option value={i + 1} key={i}>AI{i + 1}</option>)}</select></Field><Field label="Method"><select className="select" value={method} onChange={e => setMethod(e.target.value as any)}><option value="GAIN_OFFSET">Gain and offset</option><option value="TWO_POINT">Two-point calibration</option></select></Field></div>
      <div className="split-grid">
        <Panel title={`AI${channel} correction`} note="Preview values are not applied until saved."><div className="panel-body">
          <div className="notice info">Latest raw value: <strong>{measurement ? `${measurement.raw_ma.toFixed(3)} mA` : 'No valid measurement'}</strong> · quality {measurement?.quality || 'UNKNOWN'} · {relativeTime(measurement?.measured_at)}</div>
          {method === 'GAIN_OFFSET' ? <div className="form-grid"><Field label="Gain"><input className="input" type="number" min="0.0001" max="10" step="0.0001" value={gain} onChange={e => setGain(e.target.value)} /></Field><Field label="Offset (mA)"><input className="input" type="number" min="-25" max="25" step="0.001" value={offset} onChange={e => setOffset(e.target.value)} /></Field></div> : <div><div className="form-grid"><Field label="Raw point 1 (mA)"><input className="input" type="number" step="0.001" value={raw1} onChange={e => setRaw1(e.target.value)} /></Field><Field label="Reference 1 (mA)"><input className="input" type="number" step="0.001" value={ref1} onChange={e => setRef1(e.target.value)} /></Field><Field label="Raw point 2 (mA)"><input className="input" type="number" step="0.001" value={raw2} onChange={e => setRaw2(e.target.value)} /></Field><Field label="Reference 2 (mA)"><input className="input" type="number" step="0.001" value={ref2} onChange={e => setRef2(e.target.value)} /></Field></div><p className="panel-note" style={{ marginTop: 12 }}>Raw points must be separated by at least 0.1 mA; calculated gain is limited to 0.1–10.</p></div>}
          <div style={{ marginTop: 18, borderTop: '1px solid var(--line)', paddingTop: 15 }}><label style={{ display: 'flex', gap: 8, alignItems: 'center', fontSize: 12, marginBottom: 12 }}><input type="checkbox" checked={scaleEnabled} onChange={e => setScaleEnabled(e.target.checked)} /> Enable engineering scaling</label>{scaleEnabled && <div className="form-grid"><Field label="Input minimum mA"><input className="input" type="number" step="0.01" value={inputMin} onChange={e => setInputMin(e.target.value)} /></Field><Field label="Input maximum mA"><input className="input" type="number" step="0.01" value={inputMax} onChange={e => setInputMax(e.target.value)} /></Field><Field label="Engineering minimum"><input className="input" type="number" step="0.01" value={engineeringMin} onChange={e => setEngineeringMin(e.target.value)} /></Field><Field label="Engineering maximum"><input className="input" type="number" step="0.01" value={engineeringMax} onChange={e => setEngineeringMax(e.target.value)} /></Field><Field label="Unit"><input className="input" value={unit} onChange={e => setUnit(e.target.value)} /></Field></div>}</div>
          <div className="form-actions" style={{ justifyContent: 'space-between' }}><button className="button danger" onClick={() => reset.mutate()} disabled={reset.isPending}>Disable / reset</button><div style={{ display: 'flex', gap: 8 }}><button className="button" onClick={() => { if (method === 'TWO_POINT') { const delta = Number(raw2) - Number(raw1); const nextGain = (Number(ref2) - Number(ref1)) / delta; preview.mutate({ gain: nextGain, offset_ma: Number(ref1) - nextGain * Number(raw1) }) } else preview.mutate({ gain: Number(gain), offset_ma: Number(offset) }) }} disabled={preview.isPending || (method === 'TWO_POINT' && Math.abs(Number(raw2) - Number(raw1)) < 0.1)}>Preview</button><button className="button primary" onClick={() => save.mutate()} disabled={save.isPending}>Apply calibration</button></div></div>
          {preview.data && <div className="notice info" style={{ marginTop: 12, marginBottom: 0 }}>Preview: {preview.data.calibrated_ma.toFixed(4)} mA{preview.data.engineering_value != null ? ` · ${preview.data.engineering_value.toFixed(3)} ${scaling?.unit || ''}` : ''}. Not saved.</div>}
          {(save.isError || preview.isError || reset.isError) && <p className="error">{((save.error || preview.error || reset.error) as Error).message}</p>}
        </div></Panel>
        <Panel title="Applied configuration" note="Revisions are append-only; reverting creates a new revision."><div className="panel-body health-list"><HealthLine label="Calibration state" value={calibration?.enabled ? 'Enabled' : 'Disabled'} good={!!calibration?.enabled} /><HealthLine label="Gain" value={calibration ? Number(calibration.gain).toFixed(5) : '1.00000'} /><HealthLine label="Offset" value={calibration ? `${Number(calibration.offset_ma).toFixed(4)} mA` : '0.0000 mA'} /><HealthLine label="Revision" value={calibration?.revision ? `r${calibration.revision}` : 'Default'} /><HealthLine label="Modified" value={fmtTime(calibration?.created_at)} /><div style={{ paddingTop: 14, fontSize: 11, color: 'var(--muted)' }}>Calibration modifies the reported mA value in software. It does not write a hardware calibration register.</div></div></Panel>
      </div>
      <div style={{ marginTop: 18 }}><Panel title="Calibration history" note="Select a previous revision to restore its values as a new revision.">{calibrationHistory.length ? <div className="table-wrap"><table><thead><tr><th>Revision</th><th>Method</th><th>Gain</th><th>Offset</th><th>State</th><th>Modified</th><th>Action</th></tr></thead><tbody>{calibrationHistory.map(entry => <tr key={entry.revision}><td>r{entry.revision}</td><td>{entry.method}</td><td>{Number(entry.gain).toFixed(5)}</td><td>{Number(entry.offset_ma).toFixed(4)} mA</td><td>{entry.enabled ? 'Enabled' : 'Disabled'}</td><td>{fmtTime(entry.created_at)}</td><td><button className="button small" onClick={() => revert.mutate(entry.revision)} disabled={revert.isPending}>Revert to this</button></td></tr>)}</tbody></table></div> : <Empty title="No calibration revisions">Default gain 1 and offset 0 are active until a configuration is saved.</Empty>}</Panel></div>
    </>}
  </>
}

function MappingPage() {
  return <><PageHeading title="I/O mapping" description="Input-to-output forwarding is reserved for a verified output profile and safety workflow." /><div className="notice">Planned feature · Mapping execution and output writes are disabled. The analog output module, register map, acknowledgement behavior and fail-safe policy still need confirmation.</div><div className="split-grid"><Panel title="Processing path" note="Preview architecture only"><div className="panel-body health-list"><HealthLine label="Source measurement" value="Raw / calibrated / engineering" /><HealthLine label="Mapping rule" value="Not active" /><HealthLine label="Destination output" value="No verified profile" /><HealthLine label="Write authorization" value="Disabled" /></div></Panel><Panel title="Enablement requirements"><div className="panel-body"><ul style={{ margin: 0, paddingLeft: 17, color: 'var(--muted)', fontSize: 12, lineHeight: 1.9 }}><li>Device-specific analog output register map</li><li>Output range and command acknowledgement</li><li>Stale-source and communication-loss behavior</li><li>Fail-safe output behavior and retry limits</li></ul></div></Panel></div></>
}

function LogsPage() {
  const [page, setPage] = useState(0)
  const [result, setResult] = useState('')
  const [selected, setSelected] = useState<EventRow | null>(null)
  const query = useQuery({ queryKey: ['logs', page, result], queryFn: () => api<EventRow[]>(`/api/logs?limit=50&offset=${page * 50}${result ? `&result=${encodeURIComponent(result)}` : ''}`), refetchInterval: 5000 })
  return <><PageHeading title="Communication logs" description="Validated request and response frames are retained for protocol diagnosis." /><div className="toolbar"><Field label="Result filter"><select className="select" value={result} onChange={e => { setResult(e.target.value); setPage(0) }}><option value="">All results</option>{['SUCCESS','TIMEOUT','MODBUS_ERROR','INVALID_RESPONSE','DISCONNECTED','CONNECTED','PENDING'].map(value => <option key={value}>{value}</option>)}</select></Field><span className="panel-note">50 events per page</span></div><Panel title="Event stream" action={<div style={{ display: 'flex', gap: 7 }}><button className="button small" onClick={() => setPage(Math.max(0,page-1))} disabled={page===0}>Previous</button><button className="button small" onClick={() => setPage(page+1)} disabled={(query.data?.length || 0) < 50}>Next</button></div>}>{query.data?.length ? <div className="table-wrap"><table><thead><tr><th>Timestamp</th><th>Gateway</th><th>Node</th><th>Direction</th><th>Protocol</th><th>Function</th><th>Address / count</th><th>Latency</th><th>Result</th></tr></thead><tbody>{query.data.map(row => <tr key={row.id} data-clickable="true" onClick={() => setSelected(row)}><td>{fmtTime(row.at)}</td><td>{row.gateway_id?.slice(0,8) || '—'}</td><td>{row.node_id?.slice(0,8) || '—'}</td><td>{row.direction || '—'}</td><td>{row.protocol || '—'}</td><td>{row.function_code ? `FC${row.function_code}` : '—'}</td><td>{row.address == null ? '—' : `0x${row.address.toString(16).toUpperCase()} / ${row.quantity}`}</td><td>{row.latency_ms == null ? '—' : `${row.latency_ms.toFixed(1)} ms`}</td><td><Status good={row.result === 'SUCCESS' || row.result === 'CONNECTED'} warning={row.result === 'PENDING'}>{row.result}</Status></td></tr>)}</tbody></table></div> : <Empty title={query.isLoading ? 'Loading event log' : 'No events match this filter'}>Gateway activity and Modbus polling results appear here.</Empty>}</Panel>{selected && <Modal title="Communication frame inspector" onClose={() => setSelected(null)}><div className="health-list"><HealthLine label="Timestamp" value={fmtTime(selected.at)} /><HealthLine label="Result" value={selected.result} /><HealthLine label="Protocol" value={selected.protocol || '—'} /><HealthLine label="Function" value={selected.function_code ? `FC${selected.function_code}` : '—'} /><HealthLine label="Register range" value={selected.address == null ? '—' : `0x${selected.address.toString(16).toUpperCase()} · ${selected.quantity} registers`} /><HealthLine label="Round trip" value={selected.latency_ms == null ? '—' : `${selected.latency_ms.toFixed(1)} ms`} /></div><p>{selected.detail}</p><div className="field"><label>Request HEX</label><pre className="hex">{selected.request_hex || 'No frame captured for this event.'}</pre></div><div className="field" style={{ marginTop: 12 }}><label>Response HEX</label><pre className="hex">{selected.response_hex || 'No frame captured for this event.'}</pre></div></Modal>}</>
}

function SettingsPage({ theme, setTheme }: { theme: string; setTheme: (value: string) => void }) {
  const settings = useQuery({ queryKey: ['settings'], queryFn: () => api<any>('/api/settings') })
  const gateways = useQuery({ queryKey: ['gateways'], queryFn: () => api<Gateway[]>('/api/gateways') })
  const sessions = useQuery({ queryKey: ['sessions'], queryFn: () => api<any[]>('/api/gateways/sessions'), refetchInterval: 3000 })
  const [showGateway, setShowGateway] = useState(false)
  const queryClient = useQueryClient()
  const mutation = useMutation({ mutationFn: (data: unknown) => post('/api/gateways', data), onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['gateways'] }); setShowGateway(false) } })
  const commission = useMutation({ mutationFn: ({ session_id, gateway_id }: any) => post(`/api/gateways/sessions/${session_id}/assign/${gateway_id}`, {}), onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['sessions'] }); queryClient.invalidateQueries({ queryKey: ['overview'] }) } })
  return <><PageHeading title="Settings" description="Service defaults and gateway commissioning state." right={<button className="button primary" onClick={() => setShowGateway(true)}><Plus size={14} /> Register gateway</button>} /><div className="split-grid"><Panel title="Service configuration" note="Read from the running backend"><div className="panel-body health-list"><HealthLine label="TCP listener" value={`${settings.data?.tcp_listener.host || '—'}:${settings.data?.tcp_listener.port || '—'}`} /><HealthLine label="Minimum poll interval" value={`${settings.data?.polling.minimum_interval_ms ?? '—'} ms`} /><HealthLine label="Default timeout" value={`${settings.data?.polling.default_timeout_ms ?? '—'} ms`} /><HealthLine label="Freshness threshold" value={`${settings.data?.freshness_seconds ?? '—'} seconds`} /><HealthLine label="Hardware writes" value={settings.data?.hardware_writes || 'Loading'} /></div></Panel><Panel title="Appearance" note="Stored in this browser"><div className="panel-body"><div className="field"><label htmlFor="theme">Color theme</label><select id="theme" className="select" value={theme} onChange={e => setTheme(e.target.value)}><option value="light">Light</option><option value="dark">Dark</option></select></div></div></Panel></div>
    <div style={{ marginTop: 18 }}><Panel title="Gateway registry" note="A TCP socket is not assigned to a gateway until commissioned.">{gateways.data?.length ? <div className="table-wrap"><table><thead><tr><th>Name</th><th>Model</th><th>Transport framing</th><th>Session</th><th>Actions</th></tr></thead><tbody>{gateways.data.map(gateway => { const session = sessions.data?.find(item => item.gateway_id === gateway.id); return <tr key={gateway.id}><td><div className="primary-name">{gateway.name}</div><div className="secondary">{gateway.id}</div></td><td>{gateway.model || '—'}</td><td>{gateway.transport_mode}</td><td><Status good={!!session}>{session ? 'Connected' : 'Disconnected'}</Status></td><td>{!session && sessions.data?.some(item => !item.gateway_id) ? <select className="select" style={{ width: 200 }} defaultValue="" onChange={e => e.target.value && commission.mutate({ session_id: e.target.value, gateway_id: gateway.id })}><option value="">Assign pending session…</option>{sessions.data.filter(item => !item.gateway_id).map(item => <option key={item.session_id} value={item.session_id}>{item.peer} · {item.session_id.slice(0,8)}</option>)}</select> : '—'}</td></tr> })}</tbody></table></div> : <Empty title="No gateways registered">Register a gateway with the transport mode confirmed from its configuration.</Empty>}</Panel></div>
    <div style={{ marginTop: 18 }}><Panel title="Unassigned TCP sessions" note="Peer addresses are shown only for operator commissioning.">{sessions.data?.filter(item => !item.gateway_id).length ? <div className="table-wrap"><table><thead><tr><th>Session</th><th>Peer address</th><th>State</th><th>First seen</th></tr></thead><tbody>{sessions.data.filter(item => !item.gateway_id).map(item => <tr key={item.session_id}><td>{item.session_id}</td><td>{item.peer}</td><td>Awaiting assignment</td><td>{relativeTime(item.last_activity ? new Date(item.last_activity * 1000).toISOString() : null)}</td></tr>)}</tbody></table></div> : <Empty title="No pending gateway connections">A connection appears after an LTE gateway opens the TCP listener.</Empty>}</Panel></div>
    {showGateway && <Modal title="Register gateway" onClose={() => setShowGateway(false)}><form onSubmit={e => { e.preventDefault(); const form = new FormData(e.currentTarget); mutation.mutate({ name: form.get('name'), model: form.get('model'), transport_mode: form.get('transport_mode') }) }}><div className="notice">Confirm the USR-G781 forwarding mode first. This setting determines whether the service sends MBAP frames or RTU frames.</div><div className="form-grid"><Field label="Gateway name"><input className="input" name="name" required /></Field><Field label="Gateway model"><input className="input" name="model" defaultValue="USR-G781" /></Field><Field label="Transport mode"><select className="select" name="transport_mode"><option value="TRANSPARENT_SERIAL">Transparent serial / RTU</option><option value="MODBUS_RTU">Modbus RTU</option><option value="MODBUS_TCP">Modbus TCP</option></select></Field></div>{mutation.isError && <p className="error">{(mutation.error as Error).message}</p>}<div className="form-actions"><button type="button" className="button" onClick={() => setShowGateway(false)}>Cancel</button><button className="button primary" disabled={mutation.isPending}>Save gateway</button></div></form></Modal>}
  </>
}

export default App
