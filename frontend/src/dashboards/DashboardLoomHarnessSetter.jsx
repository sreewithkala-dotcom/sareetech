import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const LOOM_HARNESS_SETTER_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Warp Beam Prep' },
  { id: 'job-creation', label: 'Harness Setup & Mechanical Layout' },
  { id: 'verification', label: 'Shed Geometry & Verification' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const JACQUARD_CAPACITY_TYPE_OPTIONS = [
  { value: '2400_HOOK', label: '2400 Hook [Default]' },
  { value: '1536_HOOK', label: '1536 Hook' },
  { value: '1200_HOOK', label: '1200 Hook' },
  { value: '600_HOOK', label: '600 Hook' },
]

const COMBER_BOARD_DENSITY_EPI_OPTIONS = [
  { value: '144_EPI', label: '144 EPI (Ultra-Fine Silk) [Default]' },
  { value: '120_EPI', label: '120 EPI (Fine Silk)' },
  { value: '112_EPI', label: '112 EPI (Standard)' },
  { value: '96_EPI', label: '96 EPI (Coarse)' },
]

const HARNESS_CORD_MATERIAL_OPTIONS = [
  { value: 'NOMEX_CORE_LOW_STRETCH_SYNTHETIC', label: 'Nomex-Core Low-Stretch Synthetic [Default]' },
  { value: 'STANDARD_BRAIDED_POLYESTER', label: 'Standard Braided Polyester' },
  { value: 'KEVLAR_REINFORCED_CORE', label: 'Kevlar Reinforced Core' },
]

const HARNESS_TIE_UP_PROFILE_OPTIONS = [
  { value: 'STRAIGHT_TIE', label: 'Straight Tie [Default]' },
  { value: 'REPEAT_TIE', label: 'Repeat Tie' },
  { value: 'BORDER_BODY_POINT_TIE', label: 'Border-Body Point Tie' },
]

const MAIL_EYE_LEVELING_STATUS_OPTIONS = [
  { value: 'LASER_VERIFIED_PLUS_MINUS_0_5MM', label: 'Laser Verified (±0.5mm) [Default]' },
  { value: 'MANUAL_GAUGE_PLUS_MINUS_1_0MM', label: 'Manual Gauge (±1.0mm)' },
  { value: 'UNVERIFIED_OUT_OF_ALIGNMENT', label: 'Unverified / Out of Alignment' },
]

const HARNESS_DROP_ANGLE_STATUS_OPTIONS = [
  { value: 'STRAIGHT_DROP_LE_8_DEG', label: 'Straight Drop ≤8° [Default]' },
  { value: 'STANDARD_DROP_9_12_DEG', label: 'Standard Drop 9-12°' },
  { value: 'STEEP_DROP_GT_12_DEG', label: 'Steep Drop >12° (Unsafe)' },
]

const ANTISTATIC_HARNESS_LUBRICATION_OPTIONS = [
  { value: 'DRY_PTFE_LUBRICANT_APPLIED', label: 'Dry PTFE Lubricant Applied [Default]' },
  { value: 'SILICONE_SPRAY_APPLIED', label: 'Silicone Spray Applied' },
  { value: 'NONE', label: 'None' },
]

const DRY_RUN_FULL_LIFT_TEST_OPTIONS = [
  { value: 'PASSED_100_PERCENT_HOOK_CLEARANCE', label: 'PASSED_100%_HOOK_CLEARANCE [Default]' },
  { value: 'FAILED_SLUG_HESITATION', label: 'FAILED_SLUG_HESITATION' },
  { value: 'FAILED_MISALIGNED_MAIL_EYES', label: 'FAILED_MISALIGNED_MAIL_EYES' },
]

const HARNESS_SETUP_APPROVAL_STATE_OPTIONS = [
  { value: 'SETUP_IN_PROGRESS', label: 'SETUP_IN_PROGRESS [Default]' },
  { value: 'PASSED_APPROVED_FOR_GAITING', label: 'PASSED_APPROVED_FOR_GAITING' },
  { value: 'REJECTED_LEVELING_ERROR', label: 'REJECTED_LEVELING_ERROR' },
  { value: 'REJECTED_LINGO_WEIGHT_MISMATCH', label: 'REJECTED_LINGO_WEIGHT_MISMATCH' },
]

export default function DashboardLoomHarnessSetter() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [warpBeamLogs, setWarpBeamLogs] = useState([])
  const [logs, setLogs] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [logForm, setLogForm] = useState({
    harness_setup_job_id: '',
    loom_hardware_id: '',
    jacquard_capacity_type: '2400_HOOK',
    comber_board_density_epi: '144_EPI',
    harness_cord_material: 'NOMEX_CORE_LOW_STRETCH_SYNTHETIC',
    lingo_weight_per_cord_grams: '',
    harness_tie_up_profile: 'STRAIGHT_TIE',
    total_active_harness_cords: '',
    reed_count_density: '',
    mail_eye_leveling_status: 'LASER_VERIFIED_PLUS_MINUS_0_5MM',
    shed_opening_height_mm: '',
    harness_drop_angle_status: 'STRAIGHT_DROP_LE_8_DEG',
    antistatic_harness_lubrication: 'DRY_PTFE_LUBRICANT_APPLIED',
    comber_board_clearance_mm: '',
    dry_run_full_lift_test: 'PASSED_100_PERCENT_HOOK_CLEARANCE',
    harness_setup_approval_state: 'SETUP_IN_PROGRESS',
    cord_material_batch_no: '',
    accumulated_picks_on_harness: '0',
    warp_beam_production_log_id: ''
  })

  useEffect(() => {
    fetchWarpBeamLogs()
    fetchLogs()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchWarpBeamLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/warp-beam-prep/logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setWarpBeamLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch warp beam logs:', error)
    }
  }

  const fetchLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/loom-harness-setter/logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch harness logs:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/loom-harness-setter/certificates`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setCertificates(data.certificates || [])
    } catch (error) {
      console.error('Failed to fetch harness certificates:', error)
    }
  }

  const fetchSalesForecast = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/sales/forecast/loom-harness-setter`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setForecast(data)
    } catch (error) {
      console.error('Failed to fetch sales forecast:', error)
    }
  }

  const handleLogSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...logForm,
        lingo_weight_per_cord_grams: logForm.lingo_weight_per_cord_grams ? parseFloat(logForm.lingo_weight_per_cord_grams) : null,
        shed_opening_height_mm: logForm.shed_opening_height_mm ? parseFloat(logForm.shed_opening_height_mm) : null,
        total_active_harness_cords: logForm.total_active_harness_cords ? parseInt(logForm.total_active_harness_cords) : null,
        reed_count_density: logForm.reed_count_density ? parseInt(logForm.reed_count_density) : null,
        comber_board_clearance_mm: logForm.comber_board_clearance_mm ? parseInt(logForm.comber_board_clearance_mm) : null,
        accumulated_picks_on_harness: logForm.accumulated_picks_on_harness ? parseInt(logForm.accumulated_picks_on_harness) : 0,
        warp_beam_production_log_id: logForm.warp_beam_production_log_id || null
      }

      const response = await fetch(`${API_URL}/loom-harness-setter/logs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Harness setup log ${data.harness_setup_job_id} created`, 'success')
        setValidationResult({ type: 'success', data })
        setLogForm({
          harness_setup_job_id: '', loom_hardware_id: '',
          jacquard_capacity_type: '2400_HOOK', comber_board_density_epi: '144_EPI',
          harness_cord_material: 'NOMEX_CORE_LOW_STRETCH_SYNTHETIC',
          lingo_weight_per_cord_grams: '', harness_tie_up_profile: 'STRAIGHT_TIE',
          total_active_harness_cords: '', reed_count_density: '',
          mail_eye_leveling_status: 'LASER_VERIFIED_PLUS_MINUS_0_5MM',
          shed_opening_height_mm: '', harness_drop_angle_status: 'STRAIGHT_DROP_LE_8_DEG',
          antistatic_harness_lubrication: 'DRY_PTFE_LUBRICANT_APPLIED',
          comber_board_clearance_mm: '',
          dry_run_full_lift_test: 'PASSED_100_PERCENT_HOOK_CLEARANCE',
          harness_setup_approval_state: 'SETUP_IN_PROGRESS',
          cord_material_batch_no: '', accumulated_picks_on_harness: '0',
          warp_beam_production_log_id: ''
        })
        fetchLogs()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create harness setup log', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCertify = async (logId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/loom-harness-setter/logs/${logId}/certify`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Harness certified: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchLogs()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify harness log', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'VACANT_AVAILABLE_FOR_WEAVING': return 'success'
      case 'PASSED_APPROVED_FOR_GAITING': return 'success'
      case 'SETUP_IN_PROGRESS': return 'warning'
      case 'REJECTED_LEVELING_ERROR': return 'error'
      case 'REJECTED_LINGO_WEIGHT_MISMATCH': return 'error'
      case 'UNDER_REPAIR_MAINTENANCE': return 'error'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'READY_FOR_WARP_GAITING': return 'success'
      case 'HARNESS_QC_HOLD': return 'error'
      case 'SETUP_IN_PROGRESS': return 'warning'
      case 'PENDING_MAINTENANCE': return 'warning'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Loom Harness Setting (Harness Building Master)
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
          {LOOM_HARNESS_SETTER_TABS.map((t) => (
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
          Harness setup log {validationResult.data.harness_setup_job_id} created successfully
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Job:</strong> {certificateDetail.harness_setup_job_id}</Typography>
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
              <Typography variant="h6" gutterBottom>Warp Beam Preparation Logs (Pre-Process)</Typography>
              {warpBeamLogs.length === 0 ? (
                <Typography color="text.secondary">No approved warp beam logs found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Warp Set ID</TableCell>
                        <TableCell>Target Loom</TableCell>
                        <TableCell>Length (m)</TableCell>
                        <TableCell>Tension (g)</TableCell>
                        <TableCell>Beam Density (Shore D)</TableCell>
                        <TableCell>Approval State</TableCell>
                        <TableCell>Routing</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {warpBeamLogs.map((log) => (
                        <TableRow key={log.id}>
                          <TableCell>{log.warp_set_id}</TableCell>
                          <TableCell>{log.target_loom_type}</TableCell>
                          <TableCell>{log.total_warp_length_meters}</TableCell>
                          <TableCell>{log.creel_tension_setting_grams}</TableCell>
                          <TableCell>{log.beam_density_shore_d}</TableCell>
                          <TableCell><Chip label={log.warp_beam_approval_state} color={getStatusColor(log.warp_beam_approval_state)} size="small" /></TableCell>
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
                    <InputLabel>Link Warp Beam Production Log</InputLabel>
                    <Select value={logForm.warp_beam_production_log_id} label="Link Warp Beam Production Log"
                      onChange={(e) => setLogForm({ ...logForm, warp_beam_production_log_id: e.target.value })}>
                      <MenuItem value="">Select log</MenuItem>
                      {warpBeamLogs.map((log) => (
                        <MenuItem key={log.id} value={log.id}>{log.warp_set_id} — {log.target_loom_type}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField fullWidth label="Harness Setup Job ID" value={logForm.harness_setup_job_id}
                    onChange={(e) => setLogForm({ ...logForm, harness_setup_job_id: e.target.value })}
                    placeholder="e.g., HARNESS-SETUP-2024-001" required />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>2. Category A: Harness Structure & Mechanical Layout</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Loom Hardware ID" value={logForm.loom_hardware_id}
                    onChange={(e) => setLogForm({ ...logForm, loom_hardware_id: e.target.value })}
                    placeholder="e.g., LOOM-2400-001" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Jacquard Capacity Type</InputLabel>
                    <Select value={logForm.jacquard_capacity_type} label="Jacquard Capacity Type"
                      onChange={(e) => setLogForm({ ...logForm, jacquard_capacity_type: e.target.value })}>
                      {JACQUARD_CAPACITY_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Comber Board Density (EPI)</InputLabel>
                    <Select value={logForm.comber_board_density_epi} label="Comber Board Density (EPI)"
                      onChange={(e) => setLogForm({ ...logForm, comber_board_density_epi: e.target.value })}>
                      {COMBER_BOARD_DENSITY_EPI_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Harness Cord Material</InputLabel>
                    <Select value={logForm.harness_cord_material} label="Harness Cord Material"
                      onChange={(e) => setLogForm({ ...logForm, harness_cord_material: e.target.value })}>
                      {HARNESS_CORD_MATERIAL_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Lingo Weight per Cord (grams)" value={logForm.lingo_weight_per_cord_grams}
                    onChange={(e) => setLogForm({ ...logForm, lingo_weight_per_cord_grams: e.target.value })}
                    placeholder="18.0 - 22.0" type="number" inputProps={{ step: '0.1' }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Harness Tie-Up Profile</InputLabel>
                    <Select value={logForm.harness_tie_up_profile} label="Harness Tie-Up Profile"
                      onChange={(e) => setLogForm({ ...logForm, harness_tie_up_profile: e.target.value })}>
                      {HARNESS_TIE_UP_PROFILE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Total Active Harness Cords" value={logForm.total_active_harness_cords}
                    onChange={(e) => setLogForm({ ...logForm, total_active_harness_cords: e.target.value })}
                    placeholder="e.g., 2400" type="number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Reed Count Density" value={logForm.reed_count_density}
                    onChange={(e) => setLogForm({ ...logForm, reed_count_density: e.target.value })}
                    placeholder="e.g., 100" type="number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Cord Material Batch No." value={logForm.cord_material_batch_no}
                    onChange={(e) => setLogForm({ ...logForm, cord_material_batch_no: e.target.value })}
                    placeholder="e.g., NYLON-BATCH-2024-001" />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>3. Category B: Shed Geometry & Alignment Audit</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Mail Eye Leveling Status</InputLabel>
                    <Select value={logForm.mail_eye_leveling_status} label="Mail Eye Leveling Status"
                      onChange={(e) => setLogForm({ ...logForm, mail_eye_leveling_status: e.target.value })}>
                      {MAIL_EYE_LEVELING_STATUS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Shed Opening Height (mm)" value={logForm.shed_opening_height_mm}
                    onChange={(e) => setLogForm({ ...logForm, shed_opening_height_mm: e.target.value })}
                    placeholder="42.0 - 48.0" type="number" inputProps={{ step: '0.1' }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Harness Drop Angle Status</InputLabel>
                    <Select value={logForm.harness_drop_angle_status} label="Harness Drop Angle Status"
                      onChange={(e) => setLogForm({ ...logForm, harness_drop_angle_status: e.target.value })}>
                      {HARNESS_DROP_ANGLE_STATUS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Antistatic Harness Lubrication</InputLabel>
                    <Select value={logForm.antistatic_harness_lubrication} label="Antistatic Harness Lubrication"
                      onChange={(e) => setLogForm({ ...logForm, antistatic_harness_lubrication: e.target.value })}>
                      {ANTISTATIC_HARNESS_LUBRICATION_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Comber Board Clearance (mm)" value={logForm.comber_board_clearance_mm}
                    onChange={(e) => setLogForm({ ...logForm, comber_board_clearance_mm: e.target.value })}
                    placeholder="e.g., 15" type="number" />
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
                    <InputLabel>Dry-Run Full Lift Test</InputLabel>
                    <Select value={logForm.dry_run_full_lift_test} label="Dry-Run Full Lift Test"
                      onChange={(e) => setLogForm({ ...logForm, dry_run_full_lift_test: e.target.value })}>
                      {DRY_RUN_FULL_LIFT_TEST_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Harness Setup Approval State</InputLabel>
                    <Select value={logForm.harness_setup_approval_state} label="Harness Setup Approval State"
                      onChange={(e) => setLogForm({ ...logForm, harness_setup_approval_state: e.target.value })}>
                      {HARNESS_SETUP_APPROVAL_STATE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Accumulated Picks on Harness" value={logForm.accumulated_picks_on_harness}
                    onChange={(e) => setLogForm({ ...logForm, accumulated_picks_on_harness: e.target.value })}
                    placeholder="0" type="number" />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Box display="flex" justifyContent="flex-end">
              <Button variant="contained" size="large" onClick={handleLogSubmit} disabled={submitting}>
                {submitting ? 'Submitting...' : 'Submit Harness Setup Log'}
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
                All fields from Category A, B, and C are stored with the harness setup log.
                The system automatically enforces validation rules and tracks preventive maintenance.
              </Typography>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Maintenance, Consumables, and Lifecycle Fields</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Cord Material Batch No." value={logForm.cord_material_batch_no}
                    onChange={(e) => setLogForm({ ...logForm, cord_material_batch_no: e.target.value })}
                    placeholder="e.g., NYLON-BATCH-2024-001" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Accumulated Picks on Harness" value={logForm.accumulated_picks_on_harness}
                    onChange={(e) => setLogForm({ ...logForm, accumulated_picks_on_harness: e.target.value })}
                    placeholder="0" type="number" />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Automated Routing & System Guardrails</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 1 (2400 Hook Shed Height Protection):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF jacquard_capacity_type = 2400 Hook AND shed_opening_height_mm &gt; 48.0
                    → BLOCK: EXCESSIVE_SHED_HEIGHT_WARNING
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 2 (Lingo Weight Compliance):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF jacquard_capacity_type = 2400 Hook AND (lingo_weight_grams &gt; 23.0 OR &lt; 17.0)
                    → BLOCK: INVALID_LINGO_WEIGHT
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 3 (Mail Eye Alignment Clearance):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF mail_eye_leveling_status = Unverified / Out of Alignment
                    → BLOCK: CANNOT_CLEAR_HARNESS_FOR_WARP_GAITING
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 4 (Warp Drawing-In Clearance):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF harness_setup_approval_state != PASSED_APPROVED_FOR_GAITING
                    → BLOCK: DENY_WARP_BEAM_GAITING_WORK_ORDER
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 5 (2400 Hook Capacity Logic):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF jacquard_capacity_type = 2400 Hook AND total_active_harness_cords &lt; 2400
                    → BLOCK: LOOM_CAPABILITY_MISMATCH
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Preventive Maintenance Tracker:</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF accumulated_picks_on_harness &gt;= 10,000,000
                    → SET preventive_maintenance_flag = TRUE
                  </Typography>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Recent Harness Setup Logs</Typography>
              {logs.length === 0 ? (
                <Typography color="text.secondary">No harness setup logs found</Typography>
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
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {logs.map((log) => (
                        <TableRow key={log.id}>
                          <TableCell>{log.harness_setup_job_id}</TableCell>
                          <TableCell>{log.loom_hardware_id}</TableCell>
                          <TableCell>{log.jacquard_capacity_type}</TableCell>
                          <TableCell>{log.total_active_harness_cords}</TableCell>
                          <TableCell>{log.shed_opening_height_mm}</TableCell>
                          <TableCell>{log.lingo_weight_per_cord_grams}</TableCell>
                          <TableCell><Chip label={log.harness_setup_approval_state} color={getStatusColor(log.harness_setup_approval_state)} size="small" /></TableCell>
                          <TableCell><Chip label={log.auto_assigned_routing} color={getRoutingColor(log.auto_assigned_routing)} size="small" /></TableCell>
                          <TableCell>
                            <Button size="small" variant="outlined" onClick={() => handleCertify(log.id)} disabled={log.status === 'VACANT_AVAILABLE_FOR_WEAVING'}>
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
              <Typography variant="h6" gutterBottom>Harness Certificates</Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No harness certificates found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job ID</TableCell>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Jacquard Type</TableCell>
                        <TableCell>Shed Height (mm)</TableCell>
                        <TableCell>Lingo (g)</TableCell>
                        <TableCell>Maintenance Flag</TableCell>
                        <TableCell>Routing</TableCell>
                        <TableCell>Certified At</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {certificates.map((cert) => (
                        <TableRow key={cert.id}>
                          <TableCell>{cert.harness_setup_job_id}</TableCell>
                          <TableCell>{cert.certificate_hash}</TableCell>
                          <TableCell>{cert.qr_tag_id}</TableCell>
                          <TableCell>{cert.loom_hardware_id}</TableCell>
                          <TableCell>{cert.jacquard_capacity_type}</TableCell>
                          <TableCell>{cert.shed_opening_height_mm}</TableCell>
                          <TableCell>{cert.lingo_weight_per_cord_grams}</TableCell>
                          <TableCell>
                            <Chip label={cert.preventive_maintenance_flag ? 'MAINTENANCE DUE' : 'OK'} color={cert.preventive_maintenance_flag ? 'error' : 'success'} size="small" />
                          </TableCell>
                          <TableCell><Chip label={cert.auto_assigned_routing} color={getRoutingColor(cert.auto_assigned_routing)} size="small" /></TableCell>
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
              <Typography variant="h6" gutterBottom>Sales Forecast — Harness Setup Material Plan</Typography>
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
                              <TableCell>Jacquard Type</TableCell>
                              <TableCell>Est. Setups</TableCell>
                              <TableCell>Shed Height (mm)</TableCell>
                              <TableCell>Priority</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.material_requirements.map((item, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{item.saree_category}</TableCell>
                                <TableCell>{item.design_code}</TableCell>
                                <TableCell>{item.jacquard_capacity_type}</TableCell>
                                <TableCell>{item.estimated_setups}</TableCell>
                                <TableCell>{item.shed_opening_height_mm}</TableCell>
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
                              <TableCell>Est. Setups</TableCell>
                              <TableCell>Loom ID</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.upcoming_lots.map((lot, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{lot.lot_number}</TableCell>
                                <TableCell>{lot.saree_category}</TableCell>
                                <TableCell>{lot.design_code}</TableCell>
                                <TableCell>{lot.estimated_setups}</TableCell>
                                <TableCell>{lot.loom_hardware_id}</TableCell>
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
