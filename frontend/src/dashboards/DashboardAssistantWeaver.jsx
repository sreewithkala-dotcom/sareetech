import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'
import EnterprisePanel from '../components/EnterprisePanel'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const ASSISTANT_WEAVER_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Master Weaver' },
  { id: 'postprocess', label: 'Post-Process: Shift Operations Log' },
  { id: 'certificates', label: 'Certificates & Product Details' },
  { id: 'forecast', label: 'Sales Forecast & Material Plan' },
]

const WEFT_FEEDER_POSITION_OPTIONS = [
  { value: 'Feeder 1 (Ground Silk)', label: 'Feeder 1 (Ground Silk) [Default]' },
  { value: 'Feeder 2 (Zari Extra Weft)', label: 'Feeder 2 (Zari Extra Weft)' },
  { value: 'Feeder 3 (Contrast Border Silk)', label: 'Feeder 3 (Contrast Border Silk)' },
  { value: 'Feeder 4 (Secondary Zari)', label: 'Feeder 4 (Secondary Zari)' },
]

const YARN_TAIL_TRANSFER_OPTIONS = [
  { value: 'Spliced & Tail-Locked', label: 'Spliced & Tail-Locked [Default]' },
  { value: 'Single Spool (No Reserve)', label: 'Single Spool (No Reserve)' },
  { value: 'Unverified / Loose Tail', label: 'Unverified / Loose Tail' },
]

const ZARI_TENSION_DISC_OPTIONS = [
  { value: 'Micro-Tension Active (Fine Zari)', label: 'Micro-Tension Active (Fine Zari) [Default]' },
  { value: 'Standard Friction', label: 'Standard Friction' },
  { value: 'Low-Tension Light', label: 'Low-Tension Light' },
  { value: 'Bypassed', label: 'Bypassed' },
]

const MENDING_KNOT_TYPE_OPTIONS = [
  { value: "Weaver's Micro-Knot (Short Tail)", label: "Weaver's Micro-Knot (Short Tail) [Default]" },
  { value: 'Standard Overhand Knot', label: 'Standard Overhand Knot' },
  { value: 'Spliced Loop', label: 'Spliced Loop' },
]

const DROPPER_RETHREAD_OPTIONS = [
  { value: 'Threaded & Dropper Active', label: 'Threaded & Dropper Active [Default]' },
  { value: 'Bypassed Dropper (Unsafe)', label: 'Bypassed Dropper (Unsafe)' },
  { value: 'Missing Dropper', label: 'Missing Dropper' },
]

const COMBER_BOARD_CLEANING_OPTIONS = [
  { value: 'Cleaned / Compressed Air Blowout Done', label: 'Cleaned / Compressed Air Blowout Done [Default]' },
  { value: 'Pending Cleaning', label: 'Pending Cleaning' },
  { value: 'Heavy Fly Buildup', label: 'Heavy Fly Buildup' },
]

const SHIFT_HANDOVER_OPTIONS = [
  { value: 'READY_FOR_NEXT_SHIFT', label: 'READY_FOR_NEXT_SHIFT [Default]' },
  { value: 'PENDING_WARP_BREAK_REPAIR', label: 'PENDING_WARP_BREAK_REPAIR' },
  { value: 'LOW_WEFT_RESERVE_WARNING', label: 'LOW_WEFT_RESERVE_WARNING' },
]

const APPROVAL_STATE_OPTIONS = [
  { value: 'ACTIVE_LOGGING', label: 'ACTIVE_LOGGING [Default]' },
  { value: 'PASSED_SHIFT_AUDIT', label: 'PASSED_SHIFT_AUDIT' },
  { value: 'REJECTED_UNRESOLVED_WARP_BREAKS', label: 'REJECTED_UNRESOLVED_WARP_BREAKS' },
]

export default function DashboardAssistantWeaver() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [masterWeaverJobs, setMasterWeaverJobs] = useState([])
  const [assistantJobs, setAssistantJobs] = useState([])
  const [certificates, setCertificates] = useState([])
  const [alarms, setAlarms] = useState([])
  const [forecast, setForecast] = useState(null)

  const [form, setForm] = useState({
    active_loom_id: '',
    lead_weaver_id: '',
    shift_start_time: new Date().toISOString().slice(0, 16),
    shift_end_time: new Date(Date.now() + 28800000).toISOString().slice(0, 16),
    pirns_replaced_count: 0,
    logged_warp_breaks: 0,
    logged_weft_breaks: 0,
    weft_spool_lot_id: '',
    weft_feeder_position: 'Feeder 1 (Ground Silk)',
    yarn_tail_transfer_status: 'Spliced & Tail-Locked',
    zari_tension_disc_setting: 'Micro-Tension Active (Fine Zari)',
    warp_break_repair_count: 0,
    mending_knot_type: "Weaver's Micro-Knot (Short Tail)",
    dropper_rethread_verification: 'Threaded & Dropper Active',
    comber_board_cleaning_status: 'Cleaned / Compressed Air Blowout Done',
    shift_handover_readiness: 'READY_FOR_NEXT_SHIFT',
    assistant_weaver_approval_state: 'ACTIVE_LOGGING'
  })

  useEffect(() => {
    fetchMasterWeaverJobs()
    fetchAssistantJobs()
    fetchCertificates()
    fetchAlarms()
    fetchSalesForecast()
  }, [])

  const fetchMasterWeaverJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/master-weaver/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setMasterWeaverJobs(data.jobs || [])
    } catch (error) {
      console.error('Failed to fetch master weaver jobs:', error)
    }
  }

  const fetchAssistantJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/assistant-weaver/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setAssistantJobs(data.jobs || [])
    } catch (error) {
      console.error('Failed to fetch assistant jobs:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/assistant-weaver/certificates`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setCertificates(data.certificates || [])
    } catch (error) {
      console.error('Failed to fetch assistant certificates:', error)
    }
  }

  const fetchAlarms = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/assistant-weaver/loom-alarms`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setAlarms(data.alarms || [])
    } catch (error) {
      console.error('Failed to fetch alarms:', error)
    }
  }

  const fetchSalesForecast = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/sales/forecast/assistant-weaver`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setForecast(data)
    } catch (error) {
      console.error('Failed to fetch sales forecast:', error)
    }
  }

  const handleSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/assistant-weaver/jobs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(form)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Shift log ${data.assistant_job_log_id} submitted`, 'success')
        setValidationResult({ type: 'success', data })
        fetchAssistantJobs()
        fetchAlarms()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to submit shift log', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleApprove = async (jobId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/assistant-weaver/jobs/${jobId}/approve`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ notes: 'Approved by lead weaver' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Shift log approved', 'success')
        fetchAssistantJobs()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Approval failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to approve shift log', 'error')
    }
  }

  const handleReject = async (jobId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/assistant-weaver/jobs/${jobId}/reject`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ reason: 'Rejected by lead weaver - unresolved issues' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Shift log rejected', 'warning')
        fetchAssistantJobs()
      } else {
        addNotification(data.error || 'Rejection failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to reject shift log', 'error')
    }
  }

  const handleCertify = async (jobId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/assistant-weaver/jobs/${jobId}/certify`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Certificate generated: ${data.qr_tag_id}`, 'success')
        setCertificateDetail(data)
        fetchAssistantJobs()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify shift log', 'error')
    }
  }

  const getApprovalColor = (state) => {
    switch (state) {
      case 'PASSED_SHIFT_AUDIT':
      case 'PASSED_QUALITY_AUDIT':
      case 'SHIFT_HANDOVER_COMPLETE':
      case 'PASSED_READY_FOR_WEAVER_START':
        return 'success'
      case 'REJECTED_UNRESOLVED_WARP_BREAKS':
      case 'REJECTED_HIGH_KNOT_FAILURE':
      case 'LOOM_STOPPED_MAINTENANCE_REQUIRED':
        return 'error'
      case 'ACTIVE_LOGGING':
      case 'WEAVING_IN_PROGRESS':
      case 'IN_PROGRESS_WEAVING':
      case 'TYING_IN_PROGRESS':
        return 'warning'
      default: return 'default'
    }
  }

  const getAlarmSeverity = (severity) => {
    switch (severity) {
      case 'CRITICAL': return 'error'
      case 'WARNING': return 'warning'
      default: return 'info'
    }
  }

  const getRoutingColor = (routing) => {
    if (!routing) return 'default'
    if (routing.includes('HOLD') || routing.includes('REJECTED')) return 'error'
    if (routing.includes('PASSED') || routing.includes('READY')) return 'success'
    return 'info'
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Assistant Weaver — Floor Operations
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
      </Box>

      {validationResult && validationResult.type === 'error' && validationResult.data.validation_errors && validationResult.data.validation_errors.length > 0 && (
        <Alert severity="error" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Guardrail Violations</Typography>
          {validationResult.data.validation_errors.map((err, idx) => (
            <Typography key={idx} variant="body2">
              • [{err.code}] {err.message}
            </Typography>
          ))}
        </Alert>
      )}

      {validationResult && validationResult.type === 'success' && (
        <Alert severity="success" sx={{ mb: 3 }}>
          Shift log {validationResult.data.assistant_job_log_id} submitted successfully
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Job:</strong> {certificateDetail.assistant_job_log_id}</Typography>
          <Typography variant="body2"><strong>Hash:</strong> {certificateDetail.certificate_hash}</Typography>
          <Typography variant="body2"><strong>QR Tag:</strong> {certificateDetail.qr_tag_id}</Typography>
          <Typography variant="body2"><strong>Routing:</strong> {certificateDetail.auto_assigned_routing}</Typography>
        </Alert>
      )}

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="fullWidth">
          {ASSISTANT_WEAVER_TABS.map((t) => (
            <Tab key={t.id} label={t.label} value={t.id} />
          ))}
        </Tabs>
      </Paper>

      {/* ===================== PRE-PROCESS TAB ===================== */}
      {tab === 'preprocess' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Master Weaver Approved Jobs (Pre-Process)</Typography>
              {masterWeaverJobs.length === 0 ? (
                <Typography color="text.secondary">No approved master weaver jobs found. Jobs must be approved by Master Weaver before Assistant Weaver can start floor operations.</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job ID</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Production Run ID</TableCell>
                        <TableCell>Design Master</TableCell>
                        <TableCell>Efficiency %</TableCell>
                        <TableCell>Loom RPM</TableCell>
                        <TableCell>Weaver State</TableCell>
                        <TableCell>Routing</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {masterWeaverJobs.map((job) => (
                        <TableRow key={job.id}>
                          <TableCell>{job.id.slice(0, 8)}...</TableCell>
                          <TableCell>{job.loom_id}</TableCell>
                          <TableCell>{job.production_run_id || 'N/A'}</TableCell>
                          <TableCell>{job.design_master_id ? job.design_master_id.slice(0, 8) + '...' : 'N/A'}</TableCell>
                          <TableCell>{job.efficiency_percent}%</TableCell>
                          <TableCell>{job.loom_rpm}</TableCell>
                          <TableCell><Chip label={job.weaver_approval_state} color={getApprovalColor(job.weaver_approval_state)} size="small" /></TableCell>
                          <TableCell><Chip label={job.auto_assigned_routing} color={getRoutingColor(job.auto_assigned_routing)} size="small" /></TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== POST-PROCESS TAB ===================== */}
      {tab === 'postprocess' && (
        <Grid container spacing={3}>
          {/* Shift & Operator Linkage */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Shift & Operator Linkage
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Active Loom ID"
                    value={form.active_loom_id}
                    onChange={(e) => setForm({ ...form, active_loom_id: e.target.value })}
                    placeholder="LOOM-2401"
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Lead Weaver ID"
                    value={form.lead_weaver_id}
                    onChange={(e) => setForm({ ...form, lead_weaver_id: e.target.value })}
                    placeholder="Scan or enter lead weaver ID"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Shift Start Time"
                    type="datetime-local"
                    value={form.shift_start_time}
                    onChange={(e) => setForm({ ...form, shift_start_time: e.target.value })}
                    InputLabelProps={{ shrink: true }}
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Shift End Time"
                    type="datetime-local"
                    value={form.shift_end_time}
                    onChange={(e) => setForm({ ...form, shift_end_time: e.target.value })}
                    InputLabelProps={{ shrink: true }}
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Pirns Replaced Count"
                    type="number"
                    value={form.pirns_replaced_count}
                    onChange={(e) => setForm({ ...form, pirns_replaced_count: parseInt(e.target.value) || 0 })}
                    inputProps={{ min: 0 }}
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Logged Warp Breaks"
                    type="number"
                    value={form.logged_warp_breaks}
                    onChange={(e) => setForm({ ...form, logged_warp_breaks: parseInt(e.target.value) || 0 })}
                    inputProps={{ min: 0 }}
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Logged Weft Breaks"
                    type="number"
                    value={form.logged_weft_breaks}
                    onChange={(e) => setForm({ ...form, logged_weft_breaks: parseInt(e.target.value) || 0 })}
                    inputProps={{ min: 0 }}
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Category A: Weft Material Creeling */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category A: Weft Material Creeling & Replenishment
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Weft Spool Lot ID"
                    value={form.weft_spool_lot_id}
                    onChange={(e) => setForm({ ...form, weft_spool_lot_id: e.target.value })}
                    placeholder="Dye lot / batch code"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Weft Feeder Position</InputLabel>
                    <Select
                      value={form.weft_feeder_position}
                      label="Weft Feeder Position"
                      onChange={(e) => setForm({ ...form, weft_feeder_position: e.target.value })}
                    >
                      {WEFT_FEEDER_POSITION_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Yarn Tail Transfer Status</InputLabel>
                    <Select
                      value={form.yarn_tail_transfer_status}
                      label="Yarn Tail Transfer Status"
                      onChange={(e) => setForm({ ...form, yarn_tail_transfer_status: e.target.value })}
                    >
                      {YARN_TAIL_TRANSFER_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Zari Tension Disc Setting</InputLabel>
                    <Select
                      value={form.zari_tension_disc_setting}
                      label="Zari Tension Disc Setting"
                      onChange={(e) => setForm({ ...form, zari_tension_disc_setting: e.target.value })}
                    >
                      {ZARI_TENSION_DISC_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Category B: Warp Repair & Maintenance */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category B: Warp Repair & Maintenance Logging
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Warp Break Repair Count"
                    type="number"
                    value={form.warp_break_repair_count}
                    onChange={(e) => setForm({ ...form, warp_break_repair_count: parseInt(e.target.value) || 0 })}
                    inputProps={{ min: 0 }}
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Mending Knot Type</InputLabel>
                    <Select
                      value={form.mending_knot_type}
                      label="Mending Knot Type"
                      onChange={(e) => setForm({ ...form, mending_knot_type: e.target.value })}
                    >
                      {MENDING_KNOT_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Dropper Rethread Verification</InputLabel>
                    <Select
                      value={form.dropper_rethread_verification}
                      label="Dropper Rethread Verification"
                      onChange={(e) => setForm({ ...form, dropper_rethread_verification: e.target.value })}
                    >
                      {DROPPER_RETHREAD_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Comber Board Cleaning Status</InputLabel>
                    <Select
                      value={form.comber_board_cleaning_status}
                      label="Comber Board Cleaning Status"
                      onChange={(e) => setForm({ ...form, comber_board_cleaning_status: e.target.value })}
                    >
                      {COMBER_BOARD_CLEANING_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Category C: Final Shift Status */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category C: Final Shift Status & System Verification
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel>Shift Handover Readiness</InputLabel>
                    <Select
                      value={form.shift_handover_readiness}
                      label="Shift Handover Readiness"
                      onChange={(e) => setForm({ ...form, shift_handover_readiness: e.target.value })}
                    >
                      {SHIFT_HANDOVER_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel>Assistant Weaver Approval State</InputLabel>
                    <Select
                      value={form.assistant_weaver_approval_state}
                      label="Assistant Weaver Approval State"
                      onChange={(e) => setForm({ ...form, assistant_weaver_approval_state: e.target.value })}
                    >
                      {APPROVAL_STATE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12}>
                  <Button
                    variant="contained"
                    size="large"
                    onClick={handleSubmit}
                    disabled={submitting}
                    fullWidth
                  >
                    {submitting ? 'Submitting...' : 'Submit Shift Log'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Recent Shift Logs */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Recent Shift Logs
              </Typography>
              {assistantJobs.length === 0 ? (
                <Typography color="text.secondary">No shift logs yet</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Log ID</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Shift Start</TableCell>
                        <TableCell>Pirns</TableCell>
                        <TableCell>Warp Breaks</TableCell>
                        <TableCell>Weft Breaks</TableCell>
                        <TableCell>Repairs</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {assistantJobs.map((job) => (
                        <TableRow key={job.id}>
                          <TableCell>{job.assistant_job_log_id}</TableCell>
                          <TableCell>{job.active_loom_id}</TableCell>
                          <TableCell>{new Date(job.shift_start_time).toLocaleString()}</TableCell>
                          <TableCell>{job.pirns_replaced_count}</TableCell>
                          <TableCell>{job.logged_warp_breaks}</TableCell>
                          <TableCell>{job.logged_weft_breaks}</TableCell>
                          <TableCell>{job.warp_break_repair_count}</TableCell>
                          <TableCell>
                            <Chip label={job.approval_state} color={getApprovalColor(job.approval_state)} size="small" />
                          </TableCell>
                          <TableCell>
                            {job.approval_state === 'ACTIVE_LOGGING' && (
                              <>
                                <Button size="small" variant="outlined" onClick={() => handleApprove(job.id)}>
                                  Approve
                                </Button>
                                <Button size="small" variant="outlined" color="error" onClick={() => handleReject(job.id)} sx={{ ml: 1 }}>
                                  Reject
                                </Button>
                              </>
                            )}
                            {job.approval_state === 'ACTIVE_LOGGING' && (
                              <Button size="small" variant="contained" onClick={() => handleCertify(job.id)} sx={{ ml: 1 }}>
                                Certify
                              </Button>
                            )}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Paper>
          </Grid>

          {/* Loom Breakage Alarms */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Loom Friction & Breakage Alarms
              </Typography>
              {alarms.length === 0 ? (
                <Typography color="text.secondary">No active alarms</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Alarm Type</TableCell>
                        <TableCell>Rate/Hr</TableCell>
                        <TableCell>Severity</TableCell>
                        <TableCell>Message</TableCell>
                        <TableCell>Created</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {alarms.map((alarm) => (
                        <TableRow key={alarm.id}>
                          <TableCell>{alarm.loom_id}</TableCell>
                          <TableCell>{alarm.alarm_type}</TableCell>
                          <TableCell>{alarm.breakage_rate_per_hour}</TableCell>
                          <TableCell>
                            <Chip label={alarm.severity} color={getAlarmSeverity(alarm.severity)} size="small" />
                          </TableCell>
                          <TableCell>{alarm.message}</TableCell>
                          <TableCell>{new Date(alarm.created_at).toLocaleString()}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== CERTIFICATES TAB ===================== */}
      {tab === 'certificates' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Assistant Weaver Certificates & Product Details
              </Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates generated yet. Certificates are created when a shift log is certified by the lead weaver.</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag ID</TableCell>
                        <TableCell>Job ID</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Pirns</TableCell>
                        <TableCell>Warp Breaks</TableCell>
                        <TableCell>Weft Breaks</TableCell>
                        <TableCell>Repairs</TableCell>
                        <TableCell>Weft Feeder</TableCell>
                        <TableCell>Knot Type</TableCell>
                        <TableCell>Cleaning</TableCell>
                        <TableCell>Lead Signoff</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell>Certified At</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {certificates.map((cert) => (
                        <TableRow key={cert.id}>
                          <TableCell><code>{cert.certificate_hash}</code></TableCell>
                          <TableCell><code>{cert.qr_tag_id}</code></TableCell>
                          <TableCell>{cert.assistant_job_log_id}</TableCell>
                          <TableCell>{cert.active_loom_id}</TableCell>
                          <TableCell>{cert.pirns_replaced_count}</TableCell>
                          <TableCell>{cert.logged_warp_breaks}</TableCell>
                          <TableCell>{cert.logged_weft_breaks}</TableCell>
                          <TableCell>{cert.warp_break_repair_count}</TableCell>
                          <TableCell>{cert.weft_feeder_position}</TableCell>
                          <TableCell>{cert.mending_knot_type}</TableCell>
                          <TableCell>{cert.comber_board_cleaning_status}</TableCell>
                          <TableCell>
                            <Chip label={cert.lead_weaver_signoff ? 'YES' : 'NO'} color={cert.lead_weaver_signoff ? 'success' : 'error'} size="small" />
                          </TableCell>
                          <TableCell>
                            <Chip label={cert.status} color={cert.status === 'ACTIVE' ? 'success' : 'default'} size="small" />
                          </TableCell>
                          <TableCell>{new Date(cert.certified_at).toLocaleString()}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== FORECAST TAB ===================== */}
      {tab === 'forecast' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Sales Forecast: Assistant Weaver Material Processing Plan
              </Typography>
              {!forecast ? (
                <Typography color="text.secondary">Loading forecast...</Typography>
              ) : (
                <Grid container spacing={3}>
                  <Grid item xs={12} md={6}>
                    <Card variant="outlined">
                      <CardContent>
                        <Typography variant="subtitle1" gutterBottom>Factory</Typography>
                        <Typography variant="h6">{forecast.factory_node_id || 'N/A'}</Typography>
                        <Typography variant="body2" color="text.secondary">Period: {forecast.forecast_period}</Typography>
                        <Typography variant="body2" color="text.secondary">Generated: {forecast.generated_at}</Typography>
                      </CardContent>
                    </Card>
                  </Grid>
                  <Grid item xs={12}>
                    <Typography variant="subtitle1" gutterBottom sx={{ mt: 2 }}>Material Requirements</Typography>
                    {forecast.material_requirements && forecast.material_requirements.length > 0 ? (
                      <TableContainer>
                        <Table>
                          <TableHead>
                            <TableRow>
                              <TableCell>Saree Category</TableCell>
                              <TableCell>Design Code</TableCell>
                              <TableCell>Weft Feeder Position</TableCell>
                              <TableCell>Zari Tension Disc Setting</TableCell>
                              <TableCell>Est. Shifts</TableCell>
                              <TableCell>Est. Pirns</TableCell>
                              <TableCell>Priority</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.material_requirements.map((item, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{item.saree_category}</TableCell>
                                <TableCell><code>{item.design_code}</code></TableCell>
                                <TableCell>{item.weft_feeder_position}</TableCell>
                                <TableCell>{item.zari_tension_disc_setting}</TableCell>
                                <TableCell>{item.estimated_shifts}</TableCell>
                                <TableCell>{item.pirns_replaced_count}</TableCell>
                                <TableCell>
                                  <Chip label={item.priority} color={item.priority === 'HIGH' ? 'error' : item.priority === 'MEDIUM' ? 'warning' : 'info'} size="small" />
                                </TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </TableContainer>
                    ) : (
                      <Typography color="text.secondary">No material requirements available</Typography>
                    )}
                  </Grid>
                  <Grid item xs={12}>
                    <Typography variant="subtitle1" gutterBottom sx={{ mt: 2 }}>Upcoming Lots</Typography>
                    {forecast.upcoming_lots && forecast.upcoming_lots.length > 0 ? (
                      <TableContainer>
                        <Table>
                          <TableHead>
                            <TableRow>
                              <TableCell>Lot Number</TableCell>
                              <TableCell>Saree Category</TableCell>
                              <TableCell>Design Code</TableCell>
                              <TableCell>Est. Shifts</TableCell>
                              <TableCell>Loom ID</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.upcoming_lots.map((lot, idx) => (
                              <TableRow key={idx}>
                                <TableCell><code>{lot.lot_number}</code></TableCell>
                                <TableCell>{lot.saree_category}</TableCell>
                                <TableCell><code>{lot.design_code}</code></TableCell>
                                <TableCell>{lot.estimated_shifts}</TableCell>
                                <TableCell>{lot.loom_id}</TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </TableContainer>
                    ) : (
                      <Typography color="text.secondary">No upcoming lots available</Typography>
                    )}
                  </Grid>
                </Grid>
              )}
            </Paper>
          </Grid>
        </Grid>
      )}
      <EnterprisePanel userId={user?.id} factoryNodeId={user?.factory_node_id} />
    </Container>
  )
}
