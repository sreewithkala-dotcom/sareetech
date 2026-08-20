import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const CARD_PUNCHER_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Graph Drafter' },
  { id: 'job-creation', label: 'Programming Job & Controller Setup' },
  { id: 'verification', label: 'Verification & Simulation' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const TARGET_LOOM_TYPE_OPTIONS = [
  { value: 'MECHANICAL_JACQUARD_PHYSICAL_CARDS', label: 'Mechanical Jacquard (Physical Cards)' },
  { value: 'ELECTRONIC_JACQUARD_DIGITAL_FILE', label: 'Electronic Jacquard (Digital File Upload)' },
]

const CONTROLLER_BRAND_TYPE_OPTIONS = [
  { value: 'STAUBLI_JC5_JC6', label: 'Stäubli (JC5/JC6) [Default]' },
  { value: 'BONAS_EP_SI', label: 'Bonas (EP/Si)' },
  { value: 'EZY_JACQUARD_DLC', label: 'Ezy-Jacquard (DLC)' },
  { value: 'GROSSE_EJP', label: 'Grosse (EJP)' },
  { value: 'GENERIC_MECHANICAL_PUNCHER', label: 'Generic Mechanical Puncher' },
]

const PHYSICAL_HOOK_MATRIX_OPTIONS = [
  { value: '2400_HOOK_2250_DESIGN_150_BORDER', label: '2400 Hook (2250 Design + 150 Border) [Default]' },
  { value: '1536_HOOK_1440_DESIGN_96_BORDER', label: '1536 Hook (1440 Design + 96 Border)' },
  { value: '1200_HOOK_1080_DESIGN_120_BORDER', label: '1200 Hook (1080 Design + 120 Border)' },
  { value: '600_HOOK_540_DESIGN_60_BORDER', label: '600 Hook (540 Design + 60 Border)' },
]

const DATA_TRANSFER_METHOD_OPTIONS = [
  { value: 'DIRECT_LOOM_NETWORK_LAN', label: 'Direct Loom Network (LAN) [Default]' },
  { value: 'USB_STORAGE_MEDIA', label: 'USB Storage Media' },
  { value: 'DIRECT_SERIAL_LINK_RS422', label: 'Direct Serial Link (RS-422)' },
  { value: 'PHYSICAL_CARD_PUNCH_MACHINE', label: 'Physical Card Punch Machine' },
]

const PATTERN_REPEAT_MODE_OPTIONS = [
  { value: 'STRAIGHT_REPEAT', label: 'Straight Repeat [Default]' },
  { value: 'MIRROR_FLIP_REPEAT', label: 'Mirror / Flip Repeat' },
  { value: 'CENTERED_DOUBLE_REPEAT', label: 'Centered Double Repeat' },
  { value: 'PANEL_BORDER_SYNC_MODE', label: 'Panel / Border Sync Mode' },
]

const SOLENOID_FIRING_PROFILE_OPTIONS = [
  { value: 'HIGH_DENSITY_FAST_PULSE_LE_8MS', label: 'High-Density Fast Pulse (≤ 8ms) [Default]' },
  { value: 'STANDARD_PULSE_12_15MS', label: 'Standard Pulse (12-15ms)' },
  { value: 'EXTENDED_HOLD_PULSE', label: 'Extended Hold Pulse' },
]

const PICK_SEQUENCE_INTERLOCK_OPTIONS = [
  { value: 'GROUND_1_1_EXTRA_WEFT', label: 'Ground 1:1 Extra Weft [Default]' },
  { value: 'GROUND_2_1_ZARI_PICK', label: 'Ground 2:1 Zari Pick' },
  { value: 'GROUND_3_1_HEAVY_WEFT', label: 'Ground 3:1 Heavy Weft' },
  { value: 'MULTI_SHUTTLE_CONTINUOUS', label: 'Multi-Shuttle Continuous' },
]

const DRY_RUN_SIMULATION_STATUS_OPTIONS = [
  { value: 'PASSED_ZERO_ERRORS', label: 'PASSED_ZERO_ERRORS [Default]' },
  { value: 'WARNING_HIGH_SOLENOID_LOAD', label: 'WARNING_HIGH_SOLENOID_LOAD' },
  { value: 'FAILED_UNMAPPED_PINS', label: 'FAILED_UNMAPPED_PINS' },
  { value: 'FAILED_CHECKSUM_MISMATCH', label: 'FAILED_CHECKSUM_MISMATCH' },
]

const CARD_PROGRAM_APPROVAL_STATE_OPTIONS = [
  { value: 'PENDING_COMPILATION', label: 'PENDING_COMPILATION [Default]' },
  { value: 'SIMULATION_PASSED', label: 'SIMULATION_PASSED' },
  { value: 'LOADED_TO_LOOM', label: 'LOADED_TO_LOOM' },
  { value: 'REJECTED_MISMATCH', label: 'REJECTED_MISMATCH' },
  { value: 'ARCHIVED', label: 'ARCHIVED' },
]

export default function DashboardCardPuncher() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [designs, setDesigns] = useState([])
  const [jobs, setJobs] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [jobForm, setJobForm] = useState({
    programming_job_id: '',
    design_master_id: '',
    design_certificate_id: '',
    pirn_winding_job_id: '',
    bobbin_winder_job_card_id: '',
    master_colorist_recipe_id: '',
    skein_dye_job_id: '',
    throwster_record_id: '',
    compiled_cam_file_name: '',
    target_loom_type: 'ELECTRONIC_JACQUARD_DIGITAL_FILE',
    loom_hardware_id: '',
    controller_brand_type: 'STAUBLI_JC5_JC6',
    physical_hook_matrix: '2400_HOOK_2250_DESIGN_150_BORDER',
    data_transfer_method: 'DIRECT_LOOM_NETWORK_LAN',
    pattern_repeat_mode: 'STRAIGHT_REPEAT',
    solenoid_firing_profile: 'HIGH_DENSITY_FAST_PULSE_LE_8MS',
    pick_sequence_interlock: 'GROUND_1_1_EXTRA_WEFT',
    file_integrity_checksum: '',
    dry_run_simulation_status: 'PENDING',
    card_program_approval_state: 'PENDING_COMPILATION',
    input_blank_cards_weight_kg: '',
    actual_punched_cards_count: '',
    punch_waste_scrap_weight_gm: ''
  })

  useEffect(() => {
    fetchDesigns()
    fetchJobs()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchDesigns = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/design/graphs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setDesigns(data.designs || [])
    } catch (error) {
      console.error('Failed to fetch designs:', error)
    }
  }

  const fetchJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/card-puncher/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setJobs(data.jobs || [])
    } catch (error) {
      console.error('Failed to fetch jobs:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/card-puncher/certificates`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/card-puncher`, {
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
        design_master_id: jobForm.design_master_id || null,
        design_certificate_id: jobForm.design_certificate_id || null,
        pirn_winding_job_id: jobForm.pirn_winding_job_id || null,
        bobbin_winder_job_card_id: jobForm.bobbin_winder_job_card_id || null,
        master_colorist_recipe_id: jobForm.master_colorist_recipe_id || null,
        skein_dye_job_id: jobForm.skein_dye_job_id || null,
        throwster_record_id: jobForm.throwster_record_id || null,
        input_blank_cards_weight_kg: jobForm.input_blank_cards_weight_kg ? parseFloat(jobForm.input_blank_cards_weight_kg) : null,
        actual_punched_cards_count: jobForm.actual_punched_cards_count ? parseInt(jobForm.actual_punched_cards_count) : null,
        punch_waste_scrap_weight_gm: jobForm.punch_waste_scrap_weight_gm ? parseFloat(jobForm.punch_waste_scrap_weight_gm) : null
      }

      const response = await fetch(`${API_URL}/card-puncher/jobs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Job ${data.programming_job_id} created`, 'success')
        setValidationResult({ type: 'success', data })
        setJobForm({
          programming_job_id: '', design_master_id: '', design_certificate_id: '',
          pirn_winding_job_id: '', bobbin_winder_job_card_id: '', master_colorist_recipe_id: '',
          skein_dye_job_id: '', throwster_record_id: '', compiled_cam_file_name: '',
          target_loom_type: 'ELECTRONIC_JACQUARD_DIGITAL_FILE', loom_hardware_id: '',
          controller_brand_type: 'STAUBLI_JC5_JC6', physical_hook_matrix: '2400_HOOK_2250_DESIGN_150_BORDER',
          data_transfer_method: 'DIRECT_LOOM_NETWORK_LAN', pattern_repeat_mode: 'STRAIGHT_REPEAT',
          solenoid_firing_profile: 'HIGH_DENSITY_FAST_PULSE_LE_8MS', pick_sequence_interlock: 'GROUND_1_1_EXTRA_WEFT',
          file_integrity_checksum: '', dry_run_simulation_status: 'PENDING',
          card_program_approval_state: 'PENDING_COMPILATION',
          input_blank_cards_weight_kg: '', actual_punched_cards_count: '', punch_waste_scrap_weight_gm: ''
        })
        fetchJobs()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create job', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleSimulate = async (jobId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/card-puncher/jobs/${jobId}/simulate`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dry_run_simulation_status: 'PASSED_ZERO_ERRORS',
          file_integrity_checksum: 'CRC32-' + Math.random().toString(36).substring(2, 15)
        })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Simulation passed', 'success')
        fetchJobs()
      } else {
        addNotification(data.error || 'Simulation failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to simulate job', 'error')
    }
  }

  const handleCertify = async (jobId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/card-puncher/jobs/${jobId}/certify`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Job certified: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchJobs()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify job', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'LOADED_TO_LOOM': return 'success'
      case 'SIMULATION_PASSED': return 'success'
      case 'COMPILED': return 'info'
      case 'PENDING_COMPILATION': return 'warning'
      case 'REJECTED_MISMATCH': return 'error'
      case 'CORRUPT_HOLD': return 'error'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'DESIGN_PRODUCTION_READY': return 'success'
      case 'CORRUPT_HOLD': return 'error'
      case 'PENDING_THERMAL_REVIEW': return 'warning'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Card Puncher (Digital/E-Jacquard Programmer) — CAD-to-Loom Programming
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
          {CARD_PUNCHER_TABS.map((t) => (
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
          Job {validationResult.data.programming_job_id} created successfully
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Job:</strong> {certificateDetail.programming_job_id}</Typography>
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
              <Typography variant="h6" gutterBottom>Graph Drafter Designs (Pre-Process)</Typography>
              {designs.length === 0 ? (
                <Typography color="text.secondary">No approved designs found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Design ID</TableCell>
                        <TableCell>Iteration</TableCell>
                        <TableCell>Hook Capacity</TableCell>
                        <TableCell>Allocated Hooks</TableCell>
                        <TableCell>Grid (WxH)</TableCell>
                        <TableCell>EPI/PPI</TableCell>
                        <TableCell>Max Float</TableCell>
                        <TableCell>Approval State</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {designs.map((design) => (
                        <TableRow key={design.id}>
                          <TableCell>{design.design_master_id}</TableCell>
                          <TableCell>v{design.graph_iteration_v}</TableCell>
                          <TableCell>{design.target_hook_capacity}</TableCell>
                          <TableCell>{design.total_allocated_hooks}</TableCell>
                          <TableCell>{design.grid_width_pixels} x {design.grid_height_picks}</TableCell>
                          <TableCell>{design.warp_ends_per_inch_epi}/{design.weft_picks_per_inch_ppi}</TableCell>
                          <TableCell>{design.max_warp_float_ends} / {design.max_weft_float_picks}</TableCell>
                          <TableCell><Chip label={design.design_approval_state} color={getStatusColor(design.design_approval_state)} size="small" /></TableCell>
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

      {/* ===================== JOB CREATION & CONTROLLER SETUP TAB ===================== */}
      {tab === 'job-creation' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>1. Design Linkage</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel>Select Design Master</InputLabel>
                    <Select value={jobForm.design_master_id} label="Select Design Master"
                      onChange={(e) => setJobForm({ ...jobForm, design_master_id: e.target.value })}>
                      <MenuItem value="">Select design</MenuItem>
                      {designs.map((design) => (
                        <MenuItem key={design.id} value={design.id}>{design.design_master_id} — {design.target_hook_capacity} Hook</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField fullWidth label="Programming Job ID" value={jobForm.programming_job_id}
                    onChange={(e) => setJobForm({ ...jobForm, programming_job_id: e.target.value })}
                    placeholder="Auto-generated or scan" required />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>2. Category A: Controller & Machine Hardware Setup</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Target Loom Type</InputLabel>
                    <Select value={jobForm.target_loom_type} label="Target Loom Type"
                      onChange={(e) => setJobForm({ ...jobForm, target_loom_type: e.target.value })}>
                      {TARGET_LOOM_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Controller Brand Type</InputLabel>
                    <Select value={jobForm.controller_brand_type} label="Controller Brand Type"
                      onChange={(e) => setJobForm({ ...jobForm, controller_brand_type: e.target.value })}>
                      {CONTROLLER_BRAND_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Physical Hook Matrix</InputLabel>
                    <Select value={jobForm.physical_hook_matrix} label="Physical Hook Matrix"
                      onChange={(e) => setJobForm({ ...jobForm, physical_hook_matrix: e.target.value })}>
                      {PHYSICAL_HOOK_MATRIX_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Loom Hardware ID" value={jobForm.loom_hardware_id}
                    onChange={(e) => setJobForm({ ...jobForm, loom_hardware_id: e.target.value })}
                    placeholder="e.g., LOOM-2400-001" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Data Transfer Method</InputLabel>
                    <Select value={jobForm.data_transfer_method} label="Data Transfer Method"
                      onChange={(e) => setJobForm({ ...jobForm, data_transfer_method: e.target.value })}>
                      {DATA_TRANSFER_METHOD_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Compiled CAM File Name" value={jobForm.compiled_cam_file_name}
                    onChange={(e) => setJobForm({ ...jobForm, compiled_cam_file_name: e.target.value })}
                    placeholder="e.g., DES-2026-BR-09_v1.0.jc5" />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>3. Category B: Programming Rules & Repeat Logic</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Pattern Repeat Mode</InputLabel>
                    <Select value={jobForm.pattern_repeat_mode} label="Pattern Repeat Mode"
                      onChange={(e) => setJobForm({ ...jobForm, pattern_repeat_mode: e.target.value })}>
                      {PATTERN_REPEAT_MODE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Solenoid Firing Profile</InputLabel>
                    <Select value={jobForm.solenoid_firing_profile} label="Solenoid Firing Profile"
                      onChange={(e) => setJobForm({ ...jobForm, solenoid_firing_profile: e.target.value })}>
                      {SOLENOID_FIRING_PROFILE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Pick Sequence Interlock</InputLabel>
                    <Select value={jobForm.pick_sequence_interlock} label="Pick Sequence Interlock"
                      onChange={(e) => setJobForm({ ...jobForm, pick_sequence_interlock: e.target.value })}>
                      {PICK_SEQUENCE_INTERLOCK_OPTIONS.map((opt) => (
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
              <Typography variant="h6" gutterBottom>4. Category C: Verification & System Status</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="File Integrity Checksum" value={jobForm.file_integrity_checksum}
                    onChange={(e) => setJobForm({ ...jobForm, file_integrity_checksum: e.target.value })}
                    placeholder="CRC-32 hash" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Dry-Run Simulation Status</InputLabel>
                    <Select value={jobForm.dry_run_simulation_status} label="Dry-Run Simulation Status"
                      onChange={(e) => setJobForm({ ...jobForm, dry_run_simulation_status: e.target.value })}>
                      {DRY_RUN_SIMULATION_STATUS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Card Program Approval State</InputLabel>
                    <Select value={jobForm.card_program_approval_state} label="Card Program Approval State"
                      onChange={(e) => setJobForm({ ...jobForm, card_program_approval_state: e.target.value })}>
                      {CARD_PROGRAM_APPROVAL_STATE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
              <Box sx={{ mt: 3 }}>
                <Button variant="contained" onClick={handleJobSubmit} disabled={submitting}>
                  {submitting ? 'Submitting...' : 'Save Programming Job'}
                </Button>
              </Box>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Programming Jobs</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Job ID</TableCell>
                      <TableCell>Design</TableCell>
                      <TableCell>Controller</TableCell>
                      <TableCell>Hook Matrix</TableCell>
                      <TableCell>Transfer Method</TableCell>
                      <TableCell>Simulation</TableCell>
                      <TableCell>Approval State</TableCell>
                      <TableCell>Routing</TableCell>
                      <TableCell>Status</TableCell>
                      <TableCell>Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {jobs.map((job) => (
                      <TableRow key={job.id}>
                        <TableCell>{job.programming_job_id}</TableCell>
                        <TableCell>{job.design_master_id}</TableCell>
                        <TableCell>{job.controller_brand_type}</TableCell>
                        <TableCell>{job.physical_hook_matrix}</TableCell>
                        <TableCell>{job.data_transfer_method}</TableCell>
                        <TableCell><Chip label={job.dry_run_simulation_status} color={job.dry_run_simulation_status === 'PASSED_ZERO_ERRORS' ? 'success' : 'error'} size="small" /></TableCell>
                        <TableCell><Chip label={job.card_program_approval_state} color={getStatusColor(job.card_program_approval_state)} size="small" /></TableCell>
                        <TableCell><Chip label={job.auto_assigned_routing} color={getRoutingColor(job.auto_assigned_routing)} size="small" /></TableCell>
                        <TableCell><Chip label={job.status} color={getStatusColor(job.status)} size="small" /></TableCell>
                        <TableCell>
                          {job.status === 'COMPILED' && (
                            <>
                              <Button size="small" variant="outlined" color="primary" onClick={() => handleSimulate(job.id)}>Simulate</Button>
                              <Button size="small" variant="outlined" color="success" onClick={() => handleCertify(job.id)} sx={{ ml: 1 }}>Certify</Button>
                            </>
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== VERIFICATION & SIMULATION TAB ===================== */}
      {tab === 'verification' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>1. File Compilation & Identity Tracker</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Select Programming Job</InputLabel>
                    <Select value={jobForm.design_master_id} label="Select Programming Job"
                      onChange={(e) => setJobForm({ ...jobForm, design_master_id: e.target.value })}>
                      <MenuItem value="">Select job</MenuItem>
                      {jobs.map((job) => (
                        <MenuItem key={job.id} value={job.id}>{job.programming_job_id} — {job.compiled_cam_file_name}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Compiled CAM File Name" value={jobForm.compiled_cam_file_name}
                    onChange={(e) => setJobForm({ ...jobForm, compiled_cam_file_name: e.target.value })}
                    placeholder="e.g., DES-2026-BR-09_v1.0.jc5" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="File Integrity Checksum" value={jobForm.file_integrity_checksum}
                    onChange={(e) => setJobForm({ ...jobForm, file_integrity_checksum: e.target.value })}
                    placeholder="CRC-32 hash" helperText="Auto-generated or paste checksum" />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>2. Material Consumables (Mechanical Track)</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Input Blank Cards Weight (kg)" type="number"
                    value={jobForm.input_blank_cards_weight_kg}
                    onChange={(e) => setJobForm({ ...jobForm, input_blank_cards_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Actual Punched Cards Count" type="number"
                    value={jobForm.actual_punched_cards_count}
                    onChange={(e) => setJobForm({ ...jobForm, actual_punched_cards_count: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Punch Waste Scrap Weight (gm)" type="number"
                    value={jobForm.punch_waste_scrap_weight_gm}
                    onChange={(e) => setJobForm({ ...jobForm, punch_waste_scrap_weight_gm: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>3. Automated ERP Routing & System Guardrails</Typography>
              <Alert severity="info">
                <Typography variant="subtitle2">Automated Design Validation Gates</Typography>
                <Typography variant="body2">
                  • 2400 Hook + Generic Mechanical Puncher → INVALID_CONTROLLER_SELECTION (ELECTRONIC_CONTROLLER_MANDATORY_FOR_2400)<br/>
                  • Simulation FAILED_UNMAPPED_PINS or FAILED_CHECKSUM_MISMATCH → PREVENT_LOOM_TRANSFER (CORRUPT_OR_UNMAPPED_FILE)<br/>
                  • Simulation WARNING_HIGH_SOLENOID_LOAD → HIGH_THERMAL_LOAD_REDUCE_LOOM_SPEED_OR_ADJUST_WEAVE<br/>
                  • Card Program Approval State ≠ SIMULATION_PASSED → DENY_DIRECT_LOOM_NETWORK_WRITE
                </Typography>
              </Alert>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== CERTIFICATES & SALES FORECAST TAB ===================== */}
      {tab === 'certificates-forecast' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Programming Certificates — Post-Process Output</Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates issued yet</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag</TableCell>
                        <TableCell>Job ID</TableCell>
                        <TableCell>Design</TableCell>
                        <TableCell>Controller</TableCell>
                        <TableCell>Hook Matrix</TableCell>
                        <TableCell>Transfer Method</TableCell>
                        <TableCell>Simulation</TableCell>
                        <TableCell>Routing</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell>Certified At</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {certificates.map((cert) => (
                        <TableRow key={cert.id}>
                          <TableCell>{cert.certificate_hash}</TableCell>
                          <TableCell>{cert.qr_tag_id}</TableCell>
                          <TableCell>{cert.programming_job_id}</TableCell>
                          <TableCell>{cert.design_master_id_ref}</TableCell>
                          <TableCell>{cert.controller_brand_type}</TableCell>
                          <TableCell>{cert.physical_hook_matrix}</TableCell>
                          <TableCell>{cert.data_transfer_method}</TableCell>
                          <TableCell><Chip label={cert.dry_run_simulation_status} color={cert.dry_run_simulation_status === 'PASSED_ZERO_ERRORS' ? 'success' : 'error'} size="small" /></TableCell>
                          <TableCell><Chip label={cert.auto_assigned_routing} color={getRoutingColor(cert.auto_assigned_routing)} size="small" /></TableCell>
                          <TableCell><Chip label={cert.status} color={getStatusColor(cert.status)} size="small" /></TableCell>
                          <TableCell>{new Date(cert.certified_at).toLocaleString()}</TableCell>
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
              <Typography variant="h6" gutterBottom>Sales Forecast — Card Puncher Material Processing Plan</Typography>
              {forecast ? (
                <>
                  <Grid container spacing={3} sx={{ mb: 3 }}>
                    <Grid item xs={12} md={3}>
                      <Card>
                        <CardContent>
                          <Typography color="textSecondary" gutterBottom>Forecast Period</Typography>
                          <Typography variant="h5">{forecast.forecast_period}</Typography>
                        </CardContent>
                      </Card>
                    </Grid>
                    <Grid item xs={12} md={3}>
                      <Card>
                        <CardContent>
                          <Typography color="textSecondary" gutterBottom>Factory</Typography>
                          <Typography variant="h5">{forecast.factory_node_id}</Typography>
                        </CardContent>
                      </Card>
                    </Grid>
                    <Grid item xs={12} md={3}>
                      <Card>
                        <CardContent>
                          <Typography color="textSecondary" gutterBottom>Program Lines</Typography>
                          <Typography variant="h5">{forecast.material_requirements.length}</Typography>
                        </CardContent>
                      </Card>
                    </Grid>
                    <Grid item xs={12} md={3}>
                      <Card>
                        <CardContent>
                          <Typography color="textSecondary" gutterBottom>Upcoming Lots</Typography>
                          <Typography variant="h5">{forecast.upcoming_lots.length}</Typography>
                        </CardContent>
                      </Card>
                    </Grid>
                  </Grid>

                  <Typography variant="h6" gutterBottom sx={{ mt: 2 }}>Material Requirements</Typography>
                  <TableContainer>
                    <Table>
                      <TableHead>
                        <TableRow>
                          <TableCell>Saree Category</TableCell>
                          <TableCell>Design Code</TableCell>
                          <TableCell>Hook Capacity</TableCell>
                          <TableCell>Controller</TableCell>
                          <TableCell>Est. Programs</TableCell>
                          <TableCell>CAD Format</TableCell>
                          <TableCell>Priority</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.material_requirements.map((item, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{item.saree_category}</TableCell>
                            <TableCell>{item.design_code}</TableCell>
                            <TableCell>{item.target_hook_capacity}</TableCell>
                            <TableCell>{item.controller_brand_type}</TableCell>
                            <TableCell>{item.estimated_programs}</TableCell>
                            <TableCell>{item.cad_output_format}</TableCell>
                            <TableCell><Chip label={item.priority} color={item.priority === 'HIGH' ? 'error' : 'warning'} size="small" /></TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>

                  <Typography variant="h6" gutterBottom sx={{ mt: 3 }}>Upcoming Lots</Typography>
                  <TableContainer>
                    <Table>
                      <TableHead>
                        <TableRow>
                          <TableCell>Lot Number</TableCell>
                          <TableCell>Saree Category</TableCell>
                          <TableCell>Design Code</TableCell>
                          <TableCell>Est. Programs</TableCell>
                          <TableCell>Hook Capacity</TableCell>
                          <TableCell>Controller</TableCell>
                          <TableCell>CAD Format</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.upcoming_lots.map((lot, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{lot.lot_number}</TableCell>
                            <TableCell>{lot.saree_category}</TableCell>
                            <TableCell>{lot.design_code}</TableCell>
                            <TableCell>{lot.estimated_programs}</TableCell>
                            <TableCell>{lot.target_hook_capacity}</TableCell>
                            <TableCell>{lot.controller_brand_type}</TableCell>
                            <TableCell>{lot.cad_output_format}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                </>
              ) : (
                <Typography color="text.secondary">No forecast data available</Typography>
              )}
            </Paper>
          </Grid>
        </Grid>
      )}
    </Container>
  )
}
