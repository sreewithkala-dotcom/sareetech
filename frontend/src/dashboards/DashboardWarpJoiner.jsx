import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const WARP_JOINER_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Loom Harness Setter' },
  { id: 'job-creation', label: 'Warp Joining Job & Tying Machine Setup' },
  { id: 'verification', label: 'Quality Inspection & Verification' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const TYING_MACHINE_MODEL_OPTIONS = [
  { value: 'STAUBLI_TOPMATIC', label: 'Stäubli TOPMATIC (Fine Silk) [Default]' },
  { value: 'KNOTEX_AS_3', label: 'Knotex AS/3 (Standard)' },
  { value: 'GROZ_BECKERT_KNOTMASTER', label: 'Groz-Beckert KnotMaster' },
  { value: 'MANUAL_HAND_TYING', label: 'Manual Hand Tying' },
]

const SEPARATION_NEEDLE_TYPE_OPTIONS = [
  { value: 'ULTRA_FINE_SILK_NEEDLE_LE_0_3MM', label: 'Ultra-Fine Silk Needle (≤0.3mm) [Default]' },
  { value: 'FINE_NEEDLE_0_5MM', label: 'Fine Needle (0.5mm)' },
  { value: 'STANDARD_NEEDLE_0_8MM', label: 'Standard Needle (0.8mm)' },
]

const KNOT_TYPE_SELECTION_OPTIONS = [
  { value: 'DOUBLE_LOOP_MICRO_KNOT', label: 'Double-Loop Micro Knot [Default]' },
  { value: 'STANDARD_SINGLE_OVERHAND', label: 'Standard Single Overhand' },
  { value: 'FLAT_SECURITY_KNOT', label: 'Flat Security Knot' },
]

const DOUBLE_END_DETECTION_STATUS_OPTIONS = [
  { value: 'ZERO_DOUBLE_ENDS_DETECTED', label: 'Zero Double-Ends Detected [Default]' },
  { value: 'MINOR_DOUBLE_ENDS_CORRECTED', label: 'Minor Double-Ends Corrected' },
  { value: 'FAULTY_SEPARATION_ABORT', label: 'Faulty Separation (Abort)' },
]

const KNOT_PULL_THROUGH_MODE_OPTIONS = [
  { value: 'MANUAL_HAND_CRANK_CREEP_PULL', label: 'Manual Hand-Crank Creep Pull [Default]' },
  { value: 'ELECTRONIC_SLOW_INCHING_5_PERCENT_SPEED', label: 'Electronic Slow-Inching (5% Speed)' },
  { value: 'FULL_SPEED_PULL_UNSAFE', label: 'Full Speed Pull (Unsafe)' },
]

const KNOT_PULL_THROUGH_STATUS_OPTIONS = [
  { value: 'PASSED_100_PERCENT_KNOTS_CLEARED', label: 'PASSED_100%_KNOTS_CLEARED [Default]' },
  { value: 'FAILED_KNOT_SNAG_IN_MAIL_EYE', label: 'FAILED_KNOT_SNAG_IN_MAIL_EYE' },
  { value: 'FAILED_THREAD_BREAKAGE', label: 'FAILED_THREAD_BREAKAGE' },
]

const WARP_JOINER_APPROVAL_STATE_OPTIONS = [
  { value: 'TYING_IN_PROGRESS', label: 'TYING_IN_PROGRESS [Default]' },
  { value: 'PASSED_READY_FOR_WEAVER_START', label: 'PASSED_READY_FOR_WEAVER_START' },
  { value: 'REJECTED_HIGH_KNOT_FAILURE', label: 'REJECTED_HIGH_KNOT_FAILURE' },
  { value: 'REJECTED_MISALIGNED_LEASE', label: 'REJECTED_MISALIGNED_LEASE' },
]

export default function DashboardWarpJoiner() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [harnessLogs, setHarnessLogs] = useState([])
  const [jobs, setJobs] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [jobForm, setJobForm] = useState({
    joining_job_card_id: '',
    loom_number_id: '',
    warp_set_id: '',
    tying_machine_model: 'STAUBLI_TOPMATIC',
    separation_needle_type: 'ULTRA_FINE_SILK_NEEDLE_LE_0_3MM',
    target_tying_speed_kpm: '',
    knot_type_selection: 'DOUBLE_LOOP_MICRO_KNOT',
    knot_tail_length_mm: '',
    double_end_detection_status: 'ZERO_DOUBLE_ENDS_DETECTED',
    manual_repair_knot_count: '0',
    knot_pull_through_mode: 'MANUAL_HAND_CRANK_CREEP_PULL',
    knot_pull_through_status: 'PASSED_100_PERCENT_KNOTS_CLEARED',
    warp_joiner_approval_state: 'TYING_IN_PROGRESS',
    new_warp_beam_lot_no: '',
    total_ends_to_join: '',
    missed_ends_count: '0',
    knots_completed_count: '0',
    wage_per_hundred_knots: '',
    harness_setup_log_id: ''
  })

  useEffect(() => {
    fetchHarnessLogs()
    fetchJobs()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchHarnessLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/loom-harness-setter/logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setHarnessLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch harness logs:', error)
    }
  }

  const fetchJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/warp-joiner/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setJobs(data.jobs || [])
    } catch (error) {
      console.error('Failed to fetch warp joining jobs:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/warp-joiner/certificates`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setCertificates(data.certificates || [])
    } catch (error) {
      console.error('Failed to fetch warp joining certificates:', error)
    }
  }

  const fetchSalesForecast = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/sales/forecast/warp-joiner`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setForecast(data)
    } catch (error) {
      console.error('Failed to fetch sales forecast:', error)
    }
  }

  const handleJobSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...jobForm,
        target_tying_speed_kpm: jobForm.target_tying_speed_kpm ? parseInt(jobForm.target_tying_speed_kpm) : null,
        knot_tail_length_mm: jobForm.knot_tail_length_mm ? parseFloat(jobForm.knot_tail_length_mm) : null,
        manual_repair_knot_count: jobForm.manual_repair_knot_count ? parseInt(jobForm.manual_repair_knot_count) : 0,
        total_ends_to_join: jobForm.total_ends_to_join ? parseInt(jobForm.total_ends_to_join) : null,
        missed_ends_count: jobForm.missed_ends_count ? parseInt(jobForm.missed_ends_count) : 0,
        knots_completed_count: jobForm.knots_completed_count ? parseInt(jobForm.knots_completed_count) : 0,
        wage_per_hundred_knots: jobForm.wage_per_hundred_knots ? parseFloat(jobForm.wage_per_hundred_knots) : null,
        harness_setup_log_id: jobForm.harness_setup_log_id || null
      }

      const response = await fetch(`${API_URL}/warp-joiner/jobs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Warp joining job ${data.joining_job_card_id} created`, 'success')
        setValidationResult({ type: 'success', data })
        setJobForm({
          joining_job_card_id: '', loom_number_id: '', warp_set_id: '',
          tying_machine_model: 'STAUBLI_TOPMATIC',
          separation_needle_type: 'ULTRA_FINE_SILK_NEEDLE_LE_0_3MM',
          target_tying_speed_kpm: '', knot_type_selection: 'DOUBLE_LOOP_MICRO_KNOT',
          knot_tail_length_mm: '', double_end_detection_status: 'ZERO_DOUBLE_ENDS_DETECTED',
          manual_repair_knot_count: '0', knot_pull_through_mode: 'MANUAL_HAND_CRANK_CREEP_PULL',
          knot_pull_through_status: 'PASSED_100_PERCENT_KNOTS_CLEARED',
          warp_joiner_approval_state: 'TYING_IN_PROGRESS', new_warp_beam_lot_no: '',
          total_ends_to_join: '', missed_ends_count: '0', knots_completed_count: '0',
          wage_per_hundred_knots: '', harness_setup_log_id: ''
        })
        fetchJobs()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create warp joining job', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCertify = async (jobId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/warp-joiner/jobs/${jobId}/certify`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Warp joining certified: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchJobs()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify warp joining job', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'LOOM_ACTIVE_PRODUCTION': return 'success'
      case 'PASSED_READY_FOR_WEAVER_START': return 'success'
      case 'TYING_IN_PROGRESS': return 'warning'
      case 'REJECTED_HIGH_KNOT_FAILURE': return 'error'
      case 'REJECTED_MISALIGNED_LEASE': return 'error'
      case 'LOOM_DOWNTIME_GATE': return 'error'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'READY_FOR_WEAVER_START': return 'success'
      case 'JOIN_QC_HOLD': return 'error'
      case 'TYING_IN_PROGRESS': return 'warning'
      case 'LOOM_IDLE_VARIANCE_ALERT': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Warp Joining (Tie-in Master / Knotting Specialist)
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
        <Button variant="outlined" onClick={() => navigate('/scanner')}>
          Open Scanner
        </Button>
      </Box>

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="fullWidth">
          {WARP_JOINER_TABS.map((t) => (
            <Tab key={t.id} label={t.label} value={t.id} />
          ))}
        </Tabs>
      </Paper>

      {validationResult && validationResult.type === 'error' && validationResult.data.validation_errors && (
        <Alert severity="error" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Guardrail Violations</Typography>
          {validationResult.data.validation_errors.map((err, idx) => (
            <Typography key={idx} variant="body2">• [{err.code}] {err.message}</Typography>
          ))}
        </Alert>
      )}

      {validationResult && validationResult.type === 'success' && (
        <Alert severity="success" sx={{ mb: 3 }}>
          Warp joining job {validationResult.data.joining_job_card_id} created successfully
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Job:</strong> {certificateDetail.joining_job_card_id}</Typography>
          <Typography variant="body2"><strong>Hash:</strong> {certificateDetail.certificate_hash}</Typography>
          <Typography variant="body2"><strong>QR Tag:</strong> {certificateDetail.qr_tag_id}</Typography>
          <Typography variant="body2"><strong>Routing:</strong> {certificateDetail.auto_assigned_routing}</Typography>
        </Alert>
      )}

      {/* ===================== PRE-PROCESS TAB ===================== */}
      {tab === 'preprocess' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Loom Harness Setter Logs (Pre-Process)</Typography>
              {harnessLogs.length === 0 ? (
                <Typography color="text.secondary">No approved harness setup logs found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job ID</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Jacquard Type</TableCell>
                        <TableCell>Hooks</TableCell>
                        <TableCell>Shed Height (mm)</TableCell>
                        <TableCell>Lingo (g)</TableCell>
                        <TableCell>Approval State</TableCell>
                        <TableCell>Routing</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {harnessLogs.map((log) => (
                        <TableRow key={log.id}>
                          <TableCell>{log.harness_setup_job_id}</TableCell>
                          <TableCell>{log.loom_hardware_id}</TableCell>
                          <TableCell>{log.jacquard_capacity_type}</TableCell>
                          <TableCell>{log.total_active_harness_cords}</TableCell>
                          <TableCell>{log.shed_opening_height_mm}</TableCell>
                          <TableCell>{log.lingo_weight_per_cord_grams}</TableCell>
                          <TableCell><Chip label={log.harness_setup_approval_state} color={getStatusColor(log.harness_setup_approval_state)} size="small" /></TableCell>
                          <TableCell><Chip label={log.auto_assigned_routing} color={getRoutingColor(log.auto_assigned_routing)} size="small" /></TableCell>
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

      {/* ===================== JOB CREATION & SETUP TAB ===================== */}
      {tab === 'job-creation' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>1. Design Linkage</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel>Link Harness Setup Log</InputLabel>
                    <Select value={jobForm.harness_setup_log_id} label="Link Harness Setup Log"
                      onChange={(e) => setJobForm({ ...jobForm, harness_setup_log_id: e.target.value })}>
                      <MenuItem value="">Select log</MenuItem>
                      {harnessLogs.map((log) => (
                        <MenuItem key={log.id} value={log.id}>{log.harness_setup_job_id} — {log.loom_hardware_id}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField fullWidth label="Joining Job Card ID" value={jobForm.joining_job_card_id}
                    onChange={(e) => setJobForm({ ...jobForm, joining_job_card_id: e.target.value })}
                    placeholder="e.g., WARP-JOIN-2024-001" required />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>2. Category A: Warp Setup & Tying Machine Configuration</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Loom Number ID" value={jobForm.loom_number_id}
                    onChange={(e) => setJobForm({ ...jobForm, loom_number_id: e.target.value })}
                    placeholder="e.g., LOOM-2400-001" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Warp Set ID" value={jobForm.warp_set_id}
                    onChange={(e) => setJobForm({ ...jobForm, warp_set_id: e.target.value })}
                    placeholder="e.g., WARP-SET-2024-001" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Tying Machine Model</InputLabel>
                    <Select value={jobForm.tying_machine_model} label="Tying Machine Model"
                      onChange={(e) => setJobForm({ ...jobForm, tying_machine_model: e.target.value })}>
                      {TYING_MACHINE_MODEL_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Separation Needle Type</InputLabel>
                    <Select value={jobForm.separation_needle_type} label="Separation Needle Type"
                      onChange={(e) => setJobForm({ ...jobForm, separation_needle_type: e.target.value })}>
                      {SEPARATION_NEEDLE_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Target Tying Speed (KPM)" value={jobForm.target_tying_speed_kpm}
                    onChange={(e) => setJobForm({ ...jobForm, target_tying_speed_kpm: e.target.value })}
                    placeholder="180 - 240" type="number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Knot Type Selection</InputLabel>
                    <Select value={jobForm.knot_type_selection} label="Knot Type Selection"
                      onChange={(e) => setJobForm({ ...jobForm, knot_type_selection: e.target.value })}>
                      {KNOT_TYPE_SELECTION_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Total Ends to Join" value={jobForm.total_ends_to_join}
                    onChange={(e) => setJobForm({ ...jobForm, total_ends_to_join: e.target.value })}
                    placeholder="e.g., 18000" type="number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="New Warp Beam Lot No." value={jobForm.new_warp_beam_lot_no}
                    onChange={(e) => setJobForm({ ...jobForm, new_warp_beam_lot_no: e.target.value })}
                    placeholder="Scan or enter lot number" />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>3. Category B: Quality Inspection & Defect Audit</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Knot Tail Length (mm)" value={jobForm.knot_tail_length_mm}
                    onChange={(e) => setJobForm({ ...jobForm, knot_tail_length_mm: e.target.value })}
                    placeholder="1.0 - 1.5" type="number" inputProps={{ step: '0.1' }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Double-End Detection Status</InputLabel>
                    <Select value={jobForm.double_end_detection_status} label="Double-End Detection Status"
                      onChange={(e) => setJobForm({ ...jobForm, double_end_detection_status: e.target.value })}>
                      {DOUBLE_END_DETECTION_STATUS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Manual Repair Knot Count" value={jobForm.manual_repair_knot_count}
                    onChange={(e) => setJobForm({ ...jobForm, manual_repair_knot_count: e.target.value })}
                    placeholder="0" type="number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Knot Pull-Through Mode</InputLabel>
                    <Select value={jobForm.knot_pull_through_mode} label="Knot Pull-Through Mode"
                      onChange={(e) => setJobForm({ ...jobForm, knot_pull_through_mode: e.target.value })}>
                      {KNOT_PULL_THROUGH_MODE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>4. Category C: Final Verification & System Status</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Knot Pull-Through Status</InputLabel>
                    <Select value={jobForm.knot_pull_through_status} label="Knot Pull-Through Status"
                      onChange={(e) => setJobForm({ ...jobForm, knot_pull_through_status: e.target.value })}>
                      {KNOT_PULL_THROUGH_STATUS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Warp Joiner Approval State</InputLabel>
                    <Select value={jobForm.warp_joiner_approval_state} label="Warp Joiner Approval State"
                      onChange={(e) => setJobForm({ ...jobForm, warp_joiner_approval_state: e.target.value })}>
                      {WARP_JOINER_APPROVAL_STATE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Missed Ends Count" value={jobForm.missed_ends_count}
                    onChange={(e) => setJobForm({ ...jobForm, missed_ends_count: e.target.value })}
                    placeholder="0" type="number" />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Box display="flex" justifyContent="flex-end">
              <Button variant="contained" size="large" onClick={handleJobSubmit} disabled={submitting}>
                {submitting ? 'Submitting...' : 'Submit Warp Joining Job'}
              </Button>
            </Box>
          </Grid>
        </Grid>
      )}

      {/* ===================== VERIFICATION TAB ===================== */}
      {tab === 'verification' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>File Compilation & Identity Tracker</Typography>
              <Typography variant="body2" color="text.secondary">
                All fields from Category A, B, and C are stored with the warp joining job.
                The system automatically enforces validation rules, calculates wage payouts, and tracks loom idle time.
              </Typography>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Time, Efficiency, and Quality Metrics</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Total Ends to Join" value={jobForm.total_ends_to_join}
                    onChange={(e) => setJobForm({ ...jobForm, total_ends_to_join: e.target.value })}
                    placeholder="e.g., 18000" type="number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Knots Completed Count" value={jobForm.knots_completed_count}
                    onChange={(e) => setJobForm({ ...jobForm, knots_completed_count: e.target.value })}
                    placeholder="0" type="number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Wage per Hundred Knots" value={jobForm.wage_per_hundred_knots}
                    onChange={(e) => setJobForm({ ...jobForm, wage_per_hundred_knots: e.target.value })}
                    placeholder="e.g., 50.00" type="number" inputProps={{ step: '0.01' }} />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Automated Routing & System Guardrails</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 1 (2400 Hook Tying Speed Protection):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF separation_needle_type = Ultra-Fine Silk Needle (≤0.3mm) AND target_tying_speed_kpm &gt; 250
                    → BLOCK: EXCESSIVE_TYING_SPEED_WARNING
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 2 (Knot Tail Clearance Gate):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF knot_tail_length_mm &gt; 1.8
                    → BLOCK: REJECT_LONG_KNOT_TAILS
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 3 (Safe Pull-Through Enforcement):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF knot_pull_through_mode = Full Speed Pull (Unsafe)
                    → BLOCK: INVALID_PULL_THROUGH_MODE
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 4 (Loom Weaver Handover Gate):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF warp_joiner_approval_state != PASSED_READY_FOR_WEAVER_START
                    → BLOCK: DENY_LOOM_WEAVING_PRODUCTION_START
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Loom Downtime Financial Audit:</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF loom_idle_duration_hours &gt; 6.0
                    → SET loom_idle_variance_alert = TRUE
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Piecemeal Wage Calculation:</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    wage_per_hundred_knots * (knots_completed_count / 100)
                  </Typography>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Recent Warp Joining Jobs</Typography>
              {jobs.length === 0 ? (
                <Typography color="text.secondary">No warp joining jobs found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job ID</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Tying Machine</TableCell>
                        <TableCell>Needle Type</TableCell>
                        <TableCell>Speed (KPM)</TableCell>
                        <TableCell>Knot Type</TableCell>
                        <TableCell>Approval State</TableCell>
                        <TableCell>Routing</TableCell>
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {jobs.map((job) => (
                        <TableRow key={job.id}>
                          <TableCell>{job.joining_job_card_id}</TableCell>
                          <TableCell>{job.loom_number_id}</TableCell>
                          <TableCell>{job.tying_machine_model}</TableCell>
                          <TableCell>{job.separation_needle_type}</TableCell>
                          <TableCell>{job.target_tying_speed_kpm}</TableCell>
                          <TableCell>{job.knot_type_selection}</TableCell>
                          <TableCell><Chip label={job.warp_joiner_approval_state} color={getStatusColor(job.warp_joiner_approval_state)} size="small" /></TableCell>
                          <TableCell><Chip label={job.auto_assigned_routing} color={getRoutingColor(job.auto_assigned_routing)} size="small" /></TableCell>
                          <TableCell>
                            <Button size="small" variant="outlined" onClick={() => handleCertify(job.id)} disabled={job.status === 'LOOM_ACTIVE_PRODUCTION'}>
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

      {/* ===================== CERTIFICATES & FORECAST TAB ===================== */}
      {tab === 'certificates-forecast' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Warp Joining Certificates</Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No warp joining certificates found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job ID</TableCell>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Tying Machine</TableCell>
                        <TableCell>Ends Joined</TableCell>
                        <TableCell>Knots Completed</TableCell>
                        <TableCell>Wage Payout</TableCell>
                        <TableCell>Idle Alert</TableCell>
                        <TableCell>Certified At</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {certificates.map((cert) => (
                        <TableRow key={cert.id}>
                          <TableCell>{cert.joining_job_card_id}</TableCell>
                          <TableCell>{cert.certificate_hash}</TableCell>
                          <TableCell>{cert.qr_tag_id}</TableCell>
                          <TableCell>{cert.loom_number_id}</TableCell>
                          <TableCell>{cert.tying_machine_model}</TableCell>
                          <TableCell>{cert.total_ends_to_join}</TableCell>
                          <TableCell>{cert.knots_completed_count}</TableCell>
                          <TableCell>{cert.calculated_wage_payout ? `$${cert.calculated_wage_payout.toFixed(2)}` : '-'}</TableCell>
                          <TableCell>
                            <Chip label={cert.loom_idle_variance_alert ? 'IDLE ALERT' : 'OK'} color={cert.loom_idle_variance_alert ? 'error' : 'success'} size="small" />
                          </TableCell>
                          <TableCell>{cert.certified_at ? new Date(cert.certified_at).toLocaleString() : '-'}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Sales Forecast — Warp Joining Material Plan</Typography>
              {forecast ? (
                <Grid container spacing={2}>
                  <Grid item xs={12} md={6}>
                    <Typography variant="subtitle2" gutterBottom>Material Requirements</Typography>
                    {forecast.material_requirements && forecast.material_requirements.length > 0 ? (
                      <TableContainer>
                        <Table size="small">
                          <TableHead>
                            <TableRow>
                              <TableCell>Saree Category</TableCell>
                              <TableCell>Design Code</TableCell>
                              <TableCell>Tying Machine</TableCell>
                              <TableCell>Est. Joins</TableCell>
                              <TableCell>Ends to Join</TableCell>
                              <TableCell>Priority</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.material_requirements.map((item, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{item.saree_category}</TableCell>
                                <TableCell>{item.design_code}</TableCell>
                                <TableCell>{item.tying_machine_model}</TableCell>
                                <TableCell>{item.estimated_joins}</TableCell>
                                <TableCell>{item.total_ends_to_join}</TableCell>
                                <TableCell><Chip label={item.priority} color={item.priority === 'HIGH' ? 'error' : 'warning'} size="small" /></TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </TableContainer>
                    ) : (
                      <Typography color="text.secondary">No material requirements found</Typography>
                    )}
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <Typography variant="subtitle2" gutterBottom>Upcoming Lots</Typography>
                    {forecast.upcoming_lots && forecast.upcoming_lots.length > 0 ? (
                      <TableContainer>
                        <Table size="small">
                          <TableHead>
                            <TableRow>
                              <TableCell>Lot Number</TableCell>
                              <TableCell>Saree Category</TableCell>
                              <TableCell>Design Code</TableCell>
                              <TableCell>Est. Joins</TableCell>
                              <TableCell>Loom ID</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.upcoming_lots.map((lot, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{lot.lot_number}</TableCell>
                                <TableCell>{lot.saree_category}</TableCell>
                                <TableCell>{lot.design_code}</TableCell>
                                <TableCell>{lot.estimated_joins}</TableCell>
                                <TableCell>{lot.loom_number_id}</TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </TableContainer>
                    ) : (
                      <Typography color="text.secondary">No upcoming lots found</Typography>
                    )}
                  </Grid>
                </Grid>
              ) : (
                <Typography color="text.secondary">Loading sales forecast...</Typography>
              )}
            </Paper>
          </Grid>
        </Grid>
      )}
    </Container>
  )
}
