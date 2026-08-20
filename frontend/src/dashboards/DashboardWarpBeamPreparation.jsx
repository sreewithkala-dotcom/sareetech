import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const WARP_BEAM_PREP_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Card Puncher' },
  { id: 'job-creation', label: 'Warp Beam Production Log & Setup' },
  { id: 'verification', label: 'Quality Audit & Verification' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const TARGET_LOOM_TYPE_OPTIONS = [
  { value: '2400_HOOK_ELECTRONIC_JACQUARD', label: '2400 Hook Electronic Jacquard [Default]' },
  { value: '1536_HOOK_ELECTRONIC_JACQUARD', label: '1536 Hook Electronic Jacquard' },
  { value: 'PLAIN_MECHANICAL_DOBBY', label: 'Plain Mechanical Dobby' },
]

const WARPING_MACHINE_TYPE_OPTIONS = [
  { value: 'AUTOMATIC_SECTIONAL_WARPER', label: 'Automatic Sectional Warper [Default]' },
  { value: 'MANUAL_SECTIONAL_WARPER', label: 'Manual Sectional Warper' },
  { value: 'DIRECT_HIGH_SPEED_BEAM_WARPER', label: 'Direct High-Speed Beam Warper' },
]

const STATIC_CONTROL_STATUS_OPTIONS = [
  { value: 'ACTIVE_IONIZING_BARS_65RH', label: 'Active Ionizing Bars ON (65% RH) [Default]' },
  { value: 'PASSIVE_RODS_ONLY', label: 'Passive Rods Only' },
  { value: 'DISABLED', label: 'Disabled' },
]

const LEASING_METHOD_USED_OPTIONS = [
  { value: 'AUTOMATIC_LEASE_REED_1X1_LOCK', label: 'Automatic Lease Reed (1x1 Lock) [Default]' },
  { value: 'MANUAL_SPLIT_LEASE', label: 'Manual Split Lease' },
  { value: 'STANDARD_END_TO_END_LEASE', label: 'Standard End-to-End Lease' },
]

const WARP_WAX_CONDITIONING_OPTIONS = [
  { value: 'LIQUID_ANTISTATIC_WAX_EMULSION', label: 'Liquid Antistatic Wax Emulsion [Default]' },
  { value: 'DRY_WAX_BAR_APPLICATION', label: 'Dry Wax Bar Application' },
  { value: 'NONE_RAW_SILK', label: 'None (Raw Silk)' },
]

const SECTION_GAP_OVERLAP_INSPECTION_OPTIONS = [
  { value: 'ZERO_GAP_ZERO_OVERLAP', label: 'Zero Gap / Zero Overlap (<0.05mm) [Default]' },
  { value: 'SLIGHT_RIDGE_WARNING', label: 'Slight Ridge (Warning)' },
  { value: 'SEVERE_GAP_REJECT', label: 'Severe Gap (Reject)' },
]

const WARP_BEAM_APPROVAL_STATE_OPTIONS = [
  { value: 'PENDING_WARPING', label: 'PENDING_WARPING [Default]' },
  { value: 'PASSED_APPROVED_FOR_LOOM', label: 'PASSED_APPROVED_FOR_LOOM' },
  { value: 'REJECTED_TENSION_VARIATION', label: 'REJECTED_TENSION_VARIATION' },
  { value: 'REJECTED_LENGTH_SHORTAGE', label: 'REJECTED_LENGTH_SHORTAGE' },
]

export default function DashboardWarpBeamPreparation() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [cardPuncherJobs, setCardPuncherJobs] = useState([])
  const [logs, setLogs] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [logForm, setLogForm] = useState({
    warp_set_id: '',
    target_loom_type: '2400_HOOK_ELECTRONIC_JACQUARD',
    warping_machine_type: 'AUTOMATIC_SECTIONAL_WARPER',
    total_warp_length_meters: '',
    creel_capacity_bobbins: '',
    number_of_sections: '',
    creel_tension_setting_grams: '',
    static_control_status: 'ACTIVE_IONIZING_BARS_65RH',
    leasing_method_used: 'AUTOMATIC_LEASE_REED_1X1_LOCK',
    warp_wax_conditioning: 'LIQUID_ANTISTATIC_WAX_EMULSION',
    beam_density_shore_d: '',
    section_gap_overlap_inspection: 'ZERO_GAP_ZERO_OVERLAP',
    broken_ends_repaired_count: '0',
    warp_beam_approval_state: 'PENDING_WARPING',
    total_allocated_yarn_weight_kg: '',
    post_job_returned_yarn_weight_kg: '',
    actual_scale_beam_weight_kg: '',
    card_puncher_job_id: ''
  })

  useEffect(() => {
    fetchCardPuncherJobs()
    fetchLogs()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchCardPuncherJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/card-puncher/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setCardPuncherJobs(data.jobs || [])
    } catch (error) {
      console.error('Failed to fetch card puncher jobs:', error)
    }
  }

  const fetchLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/warp-beam-prep/logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch warp beam logs:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/warp-beam-prep/certificates`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setCertificates(data.certificates || [])
    } catch (error) {
      console.error('Failed to fetch warp beam certificates:', error)
    }
  }

  const fetchSalesForecast = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/sales/forecast/warp-beam-prep`, {
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
        total_warp_length_meters: logForm.total_warp_length_meters ? parseFloat(logForm.total_warp_length_meters) : null,
        creel_capacity_bobbins: logForm.creel_capacity_bobbins ? parseInt(logForm.creel_capacity_bobbins) : null,
        number_of_sections: logForm.number_of_sections ? parseInt(logForm.number_of_sections) : null,
        creel_tension_setting_grams: logForm.creel_tension_setting_grams ? parseFloat(logForm.creel_tension_setting_grams) : null,
        beam_density_shore_d: logForm.beam_density_shore_d ? parseFloat(logForm.beam_density_shore_d) : null,
        broken_ends_repaired_count: logForm.broken_ends_repaired_count ? parseInt(logForm.broken_ends_repaired_count) : 0,
        total_allocated_yarn_weight_kg: logForm.total_allocated_yarn_weight_kg ? parseFloat(logForm.total_allocated_yarn_weight_kg) : null,
        post_job_returned_yarn_weight_kg: logForm.post_job_returned_yarn_weight_kg ? parseFloat(logForm.post_job_returned_yarn_weight_kg) : null,
        actual_scale_beam_weight_kg: logForm.actual_scale_beam_weight_kg ? parseFloat(logForm.actual_scale_beam_weight_kg) : null,
        card_puncher_job_id: logForm.card_puncher_job_id || null
      }

      const response = await fetch(`${API_URL}/warp-beam-prep/logs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Warp beam production log ${data.warp_set_id} created`, 'success')
        setValidationResult({ type: 'success', data })
        setLogForm({
          warp_set_id: '', target_loom_type: '2400_HOOK_ELECTRONIC_JACQUARD',
          warping_machine_type: 'AUTOMATIC_SECTIONAL_WARPER',
          total_warp_length_meters: '', creel_capacity_bobbins: '', number_of_sections: '',
          creel_tension_setting_grams: '', static_control_status: 'ACTIVE_IONIZING_BARS_65RH',
          leasing_method_used: 'AUTOMATIC_LEASE_REED_1X1_LOCK',
          warp_wax_conditioning: 'LIQUID_ANTISTATIC_WAX_EMULSION',
          beam_density_shore_d: '', section_gap_overlap_inspection: 'ZERO_GAP_ZERO_OVERLAP',
          broken_ends_repaired_count: '0', warp_beam_approval_state: 'PENDING_WARPING',
          total_allocated_yarn_weight_kg: '', post_job_returned_yarn_weight_kg: '',
          actual_scale_beam_weight_kg: '', card_puncher_job_id: ''
        })
        fetchLogs()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create warp beam production log', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCertify = async (logId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/warp-beam-prep/logs/${logId}/certify`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Warp beam certified: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchLogs()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify warp beam log', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'READY_FOR_LOOM_MOUNTING': return 'success'
      case 'PASSED_APPROVED_FOR_LOOM': return 'success'
      case 'PENDING_WARPING': return 'warning'
      case 'BEAM_QC_HOLD': return 'error'
      case 'REJECTED_TENSION_VARIATION': return 'error'
      case 'REJECTED_LENGTH_SHORTAGE': return 'error'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'READY_FOR_LOOM_MOUNTING': return 'success'
      case 'BEAM_QC_HOLD': return 'error'
      case 'PENDING_WARPING': return 'warning'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Warp Beam Preparation (80 Saree Length)
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
          {WARP_BEAM_PREP_TABS.map((t) => (
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
          Warp beam production log {validationResult.data.warp_set_id} created successfully
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Warp Set ID:</strong> {certificateDetail.warp_set_id}</Typography>
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
              <Typography variant="h6" gutterBottom>Card Puncher Programming Jobs (Pre-Process)</Typography>
              {cardPuncherJobs.length === 0 ? (
                <Typography color="text.secondary">No approved card puncher jobs found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job ID</TableCell>
                        <TableCell>Design ID</TableCell>
                        <TableCell>CAM File</TableCell>
                        <TableCell>Target Loom</TableCell>
                        <TableCell>Controller</TableCell>
                        <TableCell>Hook Matrix</TableCell>
                        <TableCell>Approval State</TableCell>
                        <TableCell>Routing</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {cardPuncherJobs.map((job) => (
                        <TableRow key={job.id}>
                          <TableCell>{job.programming_job_id}</TableCell>
                          <TableCell>{job.design_master_id}</TableCell>
                          <TableCell>{job.compiled_cam_file_name || '-'}</TableCell>
                          <TableCell>{job.target_loom_type}</TableCell>
                          <TableCell>{job.controller_brand_type}</TableCell>
                          <TableCell>{job.physical_hook_matrix}</TableCell>
                          <TableCell><Chip label={job.card_program_approval_state} color={getStatusColor(job.card_program_approval_state)} size="small" /></TableCell>
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

      {/* ===================== JOB CREATION & SETUP TAB ===================== */}
      {tab === 'job-creation' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>1. Design Linkage</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel>Link Card Puncher Job</InputLabel>
                    <Select value={logForm.card_puncher_job_id} label="Link Card Puncher Job"
                      onChange={(e) => setLogForm({ ...logForm, card_puncher_job_id: e.target.value })}>
                      <MenuItem value="">Select job</MenuItem>
                      {cardPuncherJobs.map((job) => (
                        <MenuItem key={job.id} value={job.id}>{job.programming_job_id} — {job.target_loom_type}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField fullWidth label="Warp Set ID" value={logForm.warp_set_id}
                    onChange={(e) => setLogForm({ ...logForm, warp_set_id: e.target.value })}
                    placeholder="e.g., WARP-SET-2024-001" required />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>2. Category A: Beam Setup & Sectional Warping Parameters</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Target Loom Type</InputLabel>
                    <Select value={logForm.target_loom_type} label="Target Loom Type"
                      onChange={(e) => setLogForm({ ...logForm, target_loom_type: e.target.value })}>
                      {TARGET_LOOM_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Warping Machine Type</InputLabel>
                    <Select value={logForm.warping_machine_type} label="Warping Machine Type"
                      onChange={(e) => setLogForm({ ...logForm, warping_machine_type: e.target.value })}>
                      {WARPING_MACHINE_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Total Warp Length (meters)" value={logForm.total_warp_length_meters}
                    onChange={(e) => setLogForm({ ...logForm, total_warp_length_meters: e.target.value })}
                    placeholder="480.00 - 520.00" type="number" inputProps={{ step: '0.01' }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Creel Capacity (Bobbins)" value={logForm.creel_capacity_bobbins}
                    onChange={(e) => setLogForm({ ...logForm, creel_capacity_bobbins: e.target.value })}
                    placeholder="800 - 1200" type="number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Number of Sections" value={logForm.number_of_sections}
                    onChange={(e) => setLogForm({ ...logForm, number_of_sections: e.target.value })}
                    placeholder="15 - 25" type="number" />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>3. Category B: Quality Controls & Tension Audit</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Creel Tension Setting (grams)" value={logForm.creel_tension_setting_grams}
                    onChange={(e) => setLogForm({ ...logForm, creel_tension_setting_grams: e.target.value })}
                    placeholder="3.5 - 5.0" type="number" inputProps={{ step: '0.1' }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Static Control Status</InputLabel>
                    <Select value={logForm.static_control_status} label="Static Control Status"
                      onChange={(e) => setLogForm({ ...logForm, static_control_status: e.target.value })}>
                      {STATIC_CONTROL_STATUS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Leasing Method Used</InputLabel>
                    <Select value={logForm.leasing_method_used} label="Leasing Method Used"
                      onChange={(e) => setLogForm({ ...logForm, leasing_method_used: e.target.value })}>
                      {LEASING_METHOD_USED_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Warp Wax Conditioning</InputLabel>
                    <Select value={logForm.warp_wax_conditioning} label="Warp Wax Conditioning"
                      onChange={(e) => setLogForm({ ...logForm, warp_wax_conditioning: e.target.value })}>
                      {WARP_WAX_CONDITIONING_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Beam Density (Shore D)" value={logForm.beam_density_shore_d}
                    onChange={(e) => setLogForm({ ...logForm, beam_density_shore_d: e.target.value })}
                    placeholder="75.0 - 82.0" type="number" inputProps={{ step: '0.1' }} />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>4. Category C: Final Inspection & System Status</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Section Gap/Overlap Inspection</InputLabel>
                    <Select value={logForm.section_gap_overlap_inspection} label="Section Gap/Overlap Inspection"
                      onChange={(e) => setLogForm({ ...logForm, section_gap_overlap_inspection: e.target.value })}>
                      {SECTION_GAP_OVERLAP_INSPECTION_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Broken Ends Repaired Count" value={logForm.broken_ends_repaired_count}
                    onChange={(e) => setLogForm({ ...logForm, broken_ends_repaired_count: e.target.value })}
                    placeholder="0" type="number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Warp Beam Approval State</InputLabel>
                    <Select value={logForm.warp_beam_approval_state} label="Warp Beam Approval State"
                      onChange={(e) => setLogForm({ ...logForm, warp_beam_approval_state: e.target.value })}>
                      {WARP_BEAM_APPROVAL_STATE_OPTIONS.map((opt) => (
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
              <Typography variant="h6" gutterBottom>5. Inventory Consumption & Mass Balance Accounting</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Total Allocated Yarn Weight (kg)" value={logForm.total_allocated_yarn_weight_kg}
                    onChange={(e) => setLogForm({ ...logForm, total_allocated_yarn_weight_kg: e.target.value })}
                    placeholder="e.g., 45.0" type="number" inputProps={{ step: '0.001' }} />
                </Grid>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Post-Job Returned Yarn Weight (kg)" value={logForm.post_job_returned_yarn_weight_kg}
                    onChange={(e) => setLogForm({ ...logForm, post_job_returned_yarn_weight_kg: e.target.value })}
                    placeholder="e.g., 2.5" type="number" inputProps={{ step: '0.001' }} />
                </Grid>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Calculated Net Beam Weight (kg)" value={
                    (logForm.total_allocated_yarn_weight_kg && logForm.post_job_returned_yarn_weight_kg)
                      ? (parseFloat(logForm.total_allocated_yarn_weight_kg) - parseFloat(logForm.post_job_returned_yarn_weight_kg)).toFixed(3)
                      : ''
                  } InputProps={{ readOnly: true }} helperText="System computed" />
                </Grid>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Actual Scale Beam Weight (kg)" value={logForm.actual_scale_beam_weight_kg}
                    onChange={(e) => setLogForm({ ...logForm, actual_scale_beam_weight_kg: e.target.value })}
                    placeholder="Physical scale reading" type="number" inputProps={{ step: '0.001' }} />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Box display="flex" justifyContent="flex-end">
              <Button variant="contained" size="large" onClick={handleLogSubmit} disabled={submitting}>
                {submitting ? 'Submitting...' : 'Submit Beam Log'}
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
                All fields from Category A, B, and C are stored with the warp beam production log.
                The system automatically calculates weight variance and enforces validation rules.
              </Typography>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Material Consumables (Mechanical Track)</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Total Allocated Yarn Weight (kg)" value={logForm.total_allocated_yarn_weight_kg}
                    onChange={(e) => setLogForm({ ...logForm, total_allocated_yarn_weight_kg: e.target.value })}
                    placeholder="e.g., 45.0" type="number" inputProps={{ step: '0.001' }} />
                </Grid>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Post-Job Returned Yarn Weight (kg)" value={logForm.post_job_returned_yarn_weight_kg}
                    onChange={(e) => setLogForm({ ...logForm, post_job_returned_yarn_weight_kg: e.target.value })}
                    placeholder="e.g., 2.5" type="number" inputProps={{ step: '0.001' }} />
                </Grid>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Calculated Net Beam Weight (kg)" value={
                    (logForm.total_allocated_yarn_weight_kg && logForm.post_job_returned_yarn_weight_kg)
                      ? (parseFloat(logForm.total_allocated_yarn_weight_kg) - parseFloat(logForm.post_job_returned_yarn_weight_kg)).toFixed(3)
                      : ''
                  } InputProps={{ readOnly: true }} helperText="System computed" />
                </Grid>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Actual Scale Beam Weight (kg)" value={logForm.actual_scale_beam_weight_kg}
                    onChange={(e) => setLogForm({ ...logForm, actual_scale_beam_weight_kg: e.target.value })}
                    placeholder="Physical scale reading" type="number" inputProps={{ step: '0.001' }} />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Automated Routing & System Guardrails</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 1 (2400 Hook Long-Warp Tension Gate):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF target_loom_type = 2400 Hook Electronic Jacquard AND creel_tension_setting_grams &gt; 5.5
                    → BLOCK: HIGH_TENSION_WARNING
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 2 (Beam Hardness Clearance Gate):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF total_warp_length_meters &gt; 400.0 AND beam_density_shore_d &lt; 75.0
                    → BLOCK: REJECT_SOFT_BEAM
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 3 (Section Join Inspection Enforcement):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF section_gap_overlap_inspection = Severe Gap (Reject)
                    → BLOCK: CANNOT_CLEAR_BEAM_FOR_WEAVING
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 4 (Loom Mounting Approval):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF warp_beam_approval_state != PASSED_APPROVED_FOR_LOOM
                    → BLOCK: DENY_LOOM_LOADING_WORK_ORDER
                  </Typography>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Recent Warp Beam Production Logs</Typography>
              {logs.length === 0 ? (
                <Typography color="text.secondary">No warp beam production logs found</Typography>
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
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {logs.map((log) => (
                        <TableRow key={log.id}>
                          <TableCell>{log.warp_set_id}</TableCell>
                          <TableCell>{log.target_loom_type}</TableCell>
                          <TableCell>{log.total_warp_length_meters}</TableCell>
                          <TableCell>{log.creel_tension_setting_grams}</TableCell>
                          <TableCell>{log.beam_density_shore_d}</TableCell>
                          <TableCell><Chip label={log.warp_beam_approval_state} color={getStatusColor(log.warp_beam_approval_state)} size="small" /></TableCell>
                          <TableCell><Chip label={log.auto_assigned_routing} color={getRoutingColor(log.auto_assigned_routing)} size="small" /></TableCell>
                          <TableCell>
                            <Button size="small" variant="outlined" onClick={() => handleCertify(log.id)} disabled={log.status === 'READY_FOR_LOOM_MOUNTING'}>
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
              <Typography variant="h6" gutterBottom>Warp Beam Certificates</Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No warp beam certificates found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Warp Set ID</TableCell>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag</TableCell>
                        <TableCell>Target Loom</TableCell>
                        <TableCell>Tension (g)</TableCell>
                        <TableCell>Beam Density (Shore D)</TableCell>
                        <TableCell>Weight Variance %</TableCell>
                        <TableCell>Routing</TableCell>
                        <TableCell>Certified At</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {certificates.map((cert) => (
                        <TableRow key={cert.id}>
                          <TableCell>{cert.warp_set_id}</TableCell>
                          <TableCell>{cert.certificate_hash}</TableCell>
                          <TableCell>{cert.qr_tag_id}</TableCell>
                          <TableCell>{cert.target_loom_type}</TableCell>
                          <TableCell>{cert.creel_tension_setting_grams}</TableCell>
                          <TableCell>{cert.beam_density_shore_d}</TableCell>
                          <TableCell>{cert.weight_variance_percent ? `${cert.weight_variance_percent.toFixed(2)}%` : '-'}</TableCell>
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
              <Typography variant="h6" gutterBottom>Sales Forecast — Warp Beam Material Plan</Typography>
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
                              <TableCell>Loom Type</TableCell>
                              <TableCell>Est. Beams</TableCell>
                              <TableCell>Length (m)</TableCell>
                              <TableCell>Priority</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.material_requirements.map((item, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{item.saree_category}</TableCell>
                                <TableCell>{item.design_code}</TableCell>
                                <TableCell>{item.target_loom_type}</TableCell>
                                <TableCell>{item.estimated_beams}</TableCell>
                                <TableCell>{item.total_warp_length_meters}</TableCell>
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
                              <TableCell>Est. Beams</TableCell>
                              <TableCell>Loom Type</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.upcoming_lots.map((lot, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{lot.lot_number}</TableCell>
                                <TableCell>{lot.saree_category}</TableCell>
                                <TableCell>{lot.design_code}</TableCell>
                                <TableCell>{lot.estimated_beams}</TableCell>
                                <TableCell>{lot.target_loom_type}</TableCell>
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
