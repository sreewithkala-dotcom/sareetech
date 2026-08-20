import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const SUP_LOOM_FLOOR_SUPERVISOR_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Assistant Weaver' },
  { id: 'postprocess', label: 'Post-Process: Floor Operations Log' },
  { id: 'certificates', label: 'Certificates & Product Details' },
  { id: 'forecast', label: 'Sales Forecast & Material Plan' },
]

const GAITING_HANDOVER_STATUS_OPTIONS = [
  { value: 'ALL_PRE_WEAVE_CHECKS_PASSED', label: 'ALL_PRE_WEAVE_CHECKS_PASSED [Default]' },
  { value: 'HARNESS_SETTER_HOLD', label: 'HARNESS_SETTER_HOLD' },
  { value: 'WARP_JOINER_HOLD', label: 'WARP_JOINER_HOLD' },
  { value: 'PETNI_MASTER_HOLD', label: 'PETNI_MASTER_HOLD' },
]

const PRIMARY_STOP_ROOT_CAUSE_OPTIONS = [
  { value: 'NONE_NORMAL_RUNNING', label: 'NONE_NORMAL_RUNNING [Default]' },
  { value: 'WARP_END_ABRASION_STATIC', label: 'WARP_END_ABRASION_STATIC' },
  { value: 'WEFT_PIRN_DELAYS', label: 'WEFT_PIRN_DELAYS' },
  { value: 'ELECTRONIC_JACQUARD_SOLENOID_FAULT', label: 'ELECTRONIC_JACQUARD_SOLENOID_FAULT' },
  { value: 'HUMIDITY_OUT_OF_SPEC', label: 'HUMIDITY_OUT_OF_SPEC' },
]

const FIRST_SAREE_DIMENSIONAL_AUDIT_OPTIONS = [
  { value: 'APPROVED_FULL_SPEC_MATCH', label: 'APPROVED_FULL_SPEC_MATCH [Default]' },
  { value: 'REJECTED_PALLU_LENGTH_ERROR', label: 'REJECTED_PALLU_LENGTH_ERROR' },
  { value: 'REJECTED_BORDER_MISALIGNMENT', label: 'REJECTED_BORDER_MISALIGNMENT' },
  { value: 'REJECTED_DENSITY_VARIANCE', label: 'REJECTED_DENSITY_VARIANCE' },
]

const SHIFT_HANDOVER_APPROVAL_STATE_OPTIONS = [
  { value: 'SHIFT_ACTIVE_NORMAL', label: 'SHIFT_ACTIVE_NORMAL [Default]' },
  { value: 'PASSED_SHIFT_TARGETS_MET', label: 'PASSED_SHIFT_TARGETS_MET' },
  { value: 'LINE_HALTED_QUALITY_INVESTIGATION', label: 'LINE_HALTED_QUALITY_INVESTIGATION' },
  { value: 'REJECTED_ENVIRONMENTAL_OUT_OF_SPEC', label: 'REJECTED_ENVIRONMENTAL_OUT_OF_SPEC' },
]

const SAREE_BATCH_RELEASE_AUTHORIZATION_OPTIONS = [
  { value: 'APPROVED_FOR_FINISHING', label: 'APPROVED_FOR_FINISHING [Default]' },
  { value: 'HOLD_PENDING_QA_REVIEW', label: 'HOLD_PENDING_QA_REVIEW' },
  { value: 'REJECTED_DEFECTIVE_BATCH', label: 'REJECTED_DEFECTIVE_BATCH' },
]

export default function DashboardSUPLoomFloorSupervisor() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [assistantWeaverJobs, setAssistantWeaverJobs] = useState([])
  const [supervisorLogs, setSupervisorLogs] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [form, setForm] = useState({
    loom_shed_line_id: '',
    assistant_weaver_job_log_id: '',
    master_weaver_job_id: '',
    ambient_relative_humidity_pct: '',
    ambient_temperature_celsius: '',
    shift_target_oee_pct: 85.0,
    gaiting_handover_status: 'ALL_PRE_WEAVE_CHECKS_PASSED',
    loom_stop_rate_per_hour: '',
    primary_stop_root_cause: 'NONE_NORMAL_RUNNING',
    first_saree_dimensional_audit: 'APPROVED_FULL_SPEC_MATCH',
    waste_percentage_current_run: '',
    shift_handover_approval_state: 'SHIFT_ACTIVE_NORMAL',
    saree_batch_release_authorization: 'APPROVED_FOR_FINISHING'
  })

  useEffect(() => {
    fetchAssistantWeaverJobs()
    fetchSupervisorLogs()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchAssistantWeaverJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/assistant-weaver/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setAssistantWeaverJobs(data.jobs || [])
    } catch (error) {
      console.error('Failed to fetch assistant weaver jobs:', error)
    }
  }

  const fetchSupervisorLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/sup-loom-floor-supervisor/logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setSupervisorLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch supervisor logs:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/sup-loom-floor-supervisor/certificates`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setCertificates(data.certificates || [])
    } catch (error) {
      console.error('Failed to fetch certificates:', error)
    }
  }

  const fetchSalesForecast = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/sales/forecast/sup-loom-floor-supervisor`, {
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
      const payload = {
        ...form,
        ambient_relative_humidity_pct: form.ambient_relative_humidity_pct ? parseFloat(form.ambient_relative_humidity_pct) : null,
        ambient_temperature_celsius: form.ambient_temperature_celsius ? parseFloat(form.ambient_temperature_celsius) : null,
        shift_target_oee_pct: form.shift_target_oee_pct ? parseFloat(form.shift_target_oee_pct) : 85.0,
        loom_stop_rate_per_hour: form.loom_stop_rate_per_hour ? parseFloat(form.loom_stop_rate_per_hour) : null,
        waste_percentage_current_run: form.waste_percentage_current_run ? parseFloat(form.waste_percentage_current_run) : null,
      }

      const response = await fetch(`${API_URL}/sup-loom-floor-supervisor/logs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Supervisor log ${data.supervisor_log_id} submitted`, 'success')
        setValidationResult({ type: 'success', data })
        fetchSupervisorLogs()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to submit supervisor log', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCertify = async (logId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/sup-loom-floor-supervisor/logs/${logId}/certify`, {
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
        fetchSupervisorLogs()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify log', 'error')
    }
  }

  const getApprovalColor = (state) => {
    switch (state) {
      case 'PASSED_SHIFT_TARGETS_MET':
      case 'APPROVED_FULL_SPEC_MATCH':
      case 'APPROVED_FOR_FINISHING':
      case 'PASSED_SHIFT_AUDIT':
        return 'success'
      case 'REJECTED_ENVIRONMENTAL_OUT_OF_SPEC':
      case 'REJECTED_PALLU_LENGTH_ERROR':
      case 'REJECTED_BORDER_MISALIGNMENT':
      case 'REJECTED_DENSITY_VARIANCE':
      case 'REJECTED_DEFECTIVE_BATCH':
      case 'REJECTED_UNRESOLVED_WARP_BREAKS':
        return 'error'
      case 'SHIFT_ACTIVE_NORMAL':
      case 'LINE_HALTED_QUALITY_INVESTIGATION':
      case 'ACTIVE_LOGGING':
      case 'WEAVING_IN_PROGRESS':
        return 'warning'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    if (!routing) return 'default'
    if (routing.includes('HOLD') || routing.includes('REJECTED')) return 'error'
    if (routing.includes('PASSED') || routing.includes('READY')) return 'success'
    return 'info'
  }

  const getSeverityColor = (severity) => {
    switch (severity) {
      case 'CRITICAL': return 'error'
      case 'WARNING': return 'warning'
      case 'BLOCK': return 'error'
      default: return 'info'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            SUP Loom Floor Supervisor
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
          Supervisor log {validationResult.data.supervisor_log_id} submitted successfully
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Log ID:</strong> {certificateDetail.supervisor_log_id}</Typography>
          <Typography variant="body2"><strong>Hash:</strong> {certificateDetail.certificate_hash}</Typography>
          <Typography variant="body2"><strong>QR Tag:</strong> {certificateDetail.qr_tag_id}</Typography>
        </Alert>
      )}

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="fullWidth">
          {SUP_LOOM_FLOOR_SUPERVISOR_TABS.map((t) => (
            <Tab key={t.id} label={t.label} value={t.id} />
          ))}
        </Tabs>
      </Paper>

      {/* ===================== PRE-PROCESS TAB ===================== */}
      {tab === 'preprocess' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Assistant Weaver Approved Jobs (Pre-Process)</Typography>
              {assistantWeaverJobs.length === 0 ? (
                <Typography color="text.secondary">No approved assistant weaver jobs found. Assistant Weaver must submit and pass shift audit before Supervisor can start floor operations.</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job ID</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Assistant</TableCell>
                        <TableCell>Lead Weaver</TableCell>
                        <TableCell>Pirns Replaced</TableCell>
                        <TableCell>Warp Breaks</TableCell>
                        <TableCell>Weft Breaks</TableCell>
                        <TableCell>Repairs</TableCell>
                        <TableCell>Approval State</TableCell>
                        <TableCell>Routing</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {assistantWeaverJobs.map((job) => (
                        <TableRow key={job.id}>
                          <TableCell>{job.assistant_job_log_id}</TableCell>
                          <TableCell>{job.active_loom_id}</TableCell>
                          <TableCell>{job.assistant_name}</TableCell>
                          <TableCell>{job.lead_name}</TableCell>
                          <TableCell>{job.pirns_replaced_count}</TableCell>
                          <TableCell>{job.logged_warp_breaks}</TableCell>
                          <TableCell>{job.logged_weft_breaks}</TableCell>
                          <TableCell>{job.warp_break_repair_count}</TableCell>
                          <TableCell><Chip label={job.approval_state} color={getApprovalColor(job.approval_state)} size="small" /></TableCell>
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
          {/* Category A: Floor Operations & Environmental Monitoring */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category A: Floor Operations & Environmental Monitoring
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Loom Shed Line ID"
                    value={form.loom_shed_line_id}
                    onChange={(e) => setForm({ ...form, loom_shed_line_id: e.target.value })}
                    placeholder="LINE-2400-A"
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Ambient Relative Humidity (%)"
                    type="number"
                    value={form.ambient_relative_humidity_pct}
                    onChange={(e) => setForm({ ...form, ambient_relative_humidity_pct: e.target.value })}
                    inputProps={{ min: 0, max: 100, step: 0.1 }}
                    helperText="65.0 - 70.0% RH for 2400 Hook, 55.0 - 60.0% RH for 1536 Hook"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Ambient Temperature (Celsius)"
                    type="number"
                    value={form.ambient_temperature_celsius}
                    onChange={(e) => setForm({ ...form, ambient_temperature_celsius: e.target.value })}
                    inputProps={{ min: 0, max: 50, step: 0.1 }}
                    helperText="24.0 - 26.0°C"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Shift Target OEE (%)"
                    type="number"
                    value={form.shift_target_oee_pct}
                    onChange={(e) => setForm({ ...form, shift_target_oee_pct: e.target.value })}
                    inputProps={{ min: 0, max: 100, step: 0.1 }}
                    helperText="Default: 85.0%"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Gaiting Handover Status</InputLabel>
                    <Select
                      value={form.gaiting_handover_status}
                      label="Gaiting Handover Status"
                      onChange={(e) => setForm({ ...form, gaiting_handover_status: e.target.value })}
                    >
                      {GAITING_HANDOVER_STATUS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Category B: Quality Escapes & Root Cause Audits */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category B: Quality Escapes & Root Cause Audits
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Loom Stop Rate per Hour"
                    type="number"
                    value={form.loom_stop_rate_per_hour}
                    onChange={(e) => setForm({ ...form, loom_stop_rate_per_hour: e.target.value })}
                    inputProps={{ min: 0, max: 20, step: 0.1 }}
                    helperText="≤1.5 stops/hour for 2400 Hook, ≤4.0 stops/hour for 1536 Hook"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Primary Stop Root Cause</InputLabel>
                    <Select
                      value={form.primary_stop_root_cause}
                      label="Primary Stop Root Cause"
                      onChange={(e) => setForm({ ...form, primary_stop_root_cause: e.target.value })}
                    >
                      {PRIMARY_STOP_ROOT_CAUSE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>First Saree Dimensional Audit</InputLabel>
                    <Select
                      value={form.first_saree_dimensional_audit}
                      label="First Saree Dimensional Audit"
                      onChange={(e) => setForm({ ...form, first_saree_dimensional_audit: e.target.value })}
                    >
                      {FIRST_SAREE_DIMENSIONAL_AUDIT_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Waste Percentage Current Run (%)"
                    type="number"
                    value={form.waste_percentage_current_run}
                    onChange={(e) => setForm({ ...form, waste_percentage_current_run: e.target.value })}
                    inputProps={{ min: 0, max: 100, step: 0.01 }}
                    helperText="≤0.4% for 2400 Hook, ≤1.5% for 1536 Hook"
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Category C: Executive Clearance & Shift Sign-Off */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category C: Executive Clearance & Shift Sign-Off
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel>Shift Handover Approval State</InputLabel>
                    <Select
                      value={form.shift_handover_approval_state}
                      label="Shift Handover Approval State"
                      onChange={(e) => setForm({ ...form, shift_handover_approval_state: e.target.value })}
                    >
                      {SHIFT_HANDOVER_APPROVAL_STATE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel>Saree Batch Release Authorization</InputLabel>
                    <Select
                      value={form.saree_batch_release_authorization}
                      label="Saree Batch Release Authorization"
                      onChange={(e) => setForm({ ...form, saree_batch_release_authorization: e.target.value })}
                    >
                      {SAREE_BATCH_RELEASE_AUTHORIZATION_OPTIONS.map((opt) => (
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
                    {submitting ? 'Submitting...' : 'Submit Supervisor Log'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Recent Supervisor Logs */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Recent Supervisor Logs
              </Typography>
              {supervisorLogs.length === 0 ? (
                <Typography color="text.secondary">No supervisor logs yet</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Log ID</TableCell>
                        <TableCell>Line ID</TableCell>
                        <TableCell>Humidity (%)</TableCell>
                        <TableCell>Temp (°C)</TableCell>
                        <TableCell>OEE (%)</TableCell>
                        <TableCell>Stops/Hr</TableCell>
                        <TableCell>First Saree Audit</TableCell>
                        <TableCell>Handover State</TableCell>
                        <TableCell>Batch Release</TableCell>
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {supervisorLogs.map((log) => (
                        <TableRow key={log.id}>
                          <TableCell>{log.supervisor_log_id}</TableCell>
                          <TableCell>{log.loom_shed_line_id}</TableCell>
                          <TableCell>{log.ambient_relative_humidity_pct}</TableCell>
                          <TableCell>{log.ambient_temperature_celsius}</TableCell>
                          <TableCell>{log.shift_target_oee_pct}</TableCell>
                          <TableCell>{log.loom_stop_rate_per_hour}</TableCell>
                          <TableCell><Chip label={log.first_saree_dimensional_audit} color={getApprovalColor(log.first_saree_dimensional_audit)} size="small" /></TableCell>
                          <TableCell><Chip label={log.shift_handover_approval_state} color={getApprovalColor(log.shift_handover_approval_state)} size="small" /></TableCell>
                          <TableCell><Chip label={log.saree_batch_release_authorization} color={getApprovalColor(log.saree_batch_release_authorization)} size="small" /></TableCell>
                          <TableCell>
                            <Button size="small" variant="contained" onClick={() => handleCertify(log.id)}>
                              Certify
                            </Button>
                          </TableCell>
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
                Supervisor Certificates & Product Details
              </Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates generated yet. Certificates are created when a supervisor log is certified.</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag ID</TableCell>
                        <TableCell>Log ID</TableCell>
                        <TableCell>Line ID</TableCell>
                        <TableCell>Humidity (%)</TableCell>
                        <TableCell>Temp (°C)</TableCell>
                        <TableCell>OEE (%)</TableCell>
                        <TableCell>Stops/Hr</TableCell>
                        <TableCell>First Saree Audit</TableCell>
                        <TableCell>Batch Release</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell>Certified At</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {certificates.map((cert) => (
                        <TableRow key={cert.id}>
                          <TableCell><code>{cert.certificate_hash}</code></TableCell>
                          <TableCell><code>{cert.qr_tag_id}</code></TableCell>
                          <TableCell>{cert.supervisor_log_id_ref}</TableCell>
                          <TableCell>{cert.loom_shed_line_id}</TableCell>
                          <TableCell>{cert.ambient_relative_humidity_pct}</TableCell>
                          <TableCell>{cert.ambient_temperature_celsius}</TableCell>
                          <TableCell>{cert.shift_target_oee_pct}</TableCell>
                          <TableCell>{cert.loom_stop_rate_per_hour}</TableCell>
                          <TableCell><Chip label={cert.first_saree_dimensional_audit} color={getApprovalColor(cert.first_saree_dimensional_audit)} size="small" /></TableCell>
                          <TableCell><Chip label={cert.saree_batch_release_authorization} color={getApprovalColor(cert.saree_batch_release_authorization)} size="small" /></TableCell>
                          <TableCell><Chip label={cert.status} color={cert.status === 'ACTIVE' ? 'success' : 'default'} size="small" /></TableCell>
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
                Sales Forecast: Floor Supervisor Material Processing Plan
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
                              <TableCell>Line ID</TableCell>
                              <TableCell>OEE Target (%)</TableCell>
                              <TableCell>Max Stops/Hr</TableCell>
                              <TableCell>Humidity Min (%)</TableCell>
                              <TableCell>Humidity Max (%)</TableCell>
                              <TableCell>Est. Shifts</TableCell>
                              <TableCell>Priority</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.material_requirements.map((item, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{item.saree_category}</TableCell>
                                <TableCell><code>{item.design_code}</code></TableCell>
                                <TableCell>{item.loom_shed_line_id}</TableCell>
                                <TableCell>{item.shift_target_ooe_pct}</TableCell>
                                <TableCell>{item.max_loom_stop_rate_per_hour}</TableCell>
                                <TableCell>{item.ambient_relative_humidity_pct_min}</TableCell>
                                <TableCell>{item.ambient_relative_humidity_pct_max}</TableCell>
                                <TableCell>{item.estimated_shifts}</TableCell>
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
                              <TableCell>Line ID</TableCell>
                              <TableCell>Est. Shifts</TableCell>
                              <TableCell>OEE Target (%)</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.upcoming_lots.map((lot, idx) => (
                              <TableRow key={idx}>
                                <TableCell><code>{lot.lot_number}</code></TableCell>
                                <TableCell>{lot.saree_category}</TableCell>
                                <TableCell><code>{lot.design_code}</code></TableCell>
                                <TableCell>{lot.loom_shed_line_id}</TableCell>
                                <TableCell>{lot.estimated_shifts}</TableCell>
                                <TableCell>{lot.shift_target_ooe_pct}</TableCell>
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
    </Container>
  )
}
