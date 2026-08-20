import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'
import EnterprisePanel from '../components/EnterprisePanel'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const MASTER_WEAVER_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Petni Master' },
  { id: 'job-creation', label: 'Weaving Production Run & Setup' },
  { id: 'verification', label: 'Quality Checks & On-Loom Audit' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const WEFT_INSERTION_FEEDER_PROFILE_OPTIONS = [
  { value: 'DUAL_FEEDER_SILK_GROUND_PLUS_ZARI_MICRO_TENSION', label: 'Dual-Feeder Silk Ground + Zari Micro-Tension [Default]' },
  { value: 'STANDARD_SINGLE_FEEDER', label: 'Standard Single Feeder' },
  { value: 'HEAVY_EXTRA_WEFT_MULTI_FEEDER', label: 'Heavy Extra-Weft Multi-Feeder' },
]

const DYNAMIC_PPI_CONTROL_MODE_OPTIONS = [
  { value: 'AUTOMATED_MULTI_DENSITY_BODY_PALLU_SWITCH', label: 'Automated Multi-Density (Body/Pallu Switch) [Default]' },
  { value: 'FIXED_SINGLE_PPI', label: 'Fixed Single PPI' },
  { value: 'MANUAL_GEAR_SHIFT', label: 'Manual Gear Shift' },
]

const ON_LOOM_DEFECT_CATEGORY_OPTIONS = [
  { value: 'NONE_ZERO_DEFECTS', label: 'NONE_ZERO_DEFECTS [Default]' },
  { value: 'WEFT_PICK_GAP_LOOM_STOP', label: 'WEFT_PICK_GAP_LOOM_STOP' },
  { value: 'ZARI_LOOPING_TENSION_FAULT', label: 'ZARI_LOOPING_TENSION_FAULT' },
  { value: 'WARP_END_BREAKAGE', label: 'WARP_END_BREAKAGE' },
  { value: 'BORDER_MISALIGNMENT', label: 'BORDER_MISALIGNMENT' },
]

const ZARI_CATCH_SELVAGE_STATUS_OPTIONS = [
  { value: 'PERFECT_CATCH_SMOOTH_EDGE', label: 'Perfect Catch / Smooth Edge [Default]' },
  { value: 'LOOSE_ZARI_LOOPS_OVER_TENSION_NEEDED', label: 'Loose Zari Loops (Over-Tension Needed)' },
  { value: 'TIGHT_EDGE_PUCKERING_REDUCE_TENSION', label: 'Tight Edge Puckering (Reduce Tension)' },
]

const SAREE_SECTION_PHASE_OPTIONS = [
  { value: 'PALLU_HIGH_DENSITY', label: 'PALLU_HIGH_DENSITY [Default]' },
  { value: 'MAIN_BODY_MOTIF', label: 'MAIN_BODY_MOTIF' },
  { value: 'SKIRT_BORDER', label: 'SKIRT_BORDER' },
  { value: 'SAREE_CUT_LINE_TRANSITION', label: 'SAREE_CUT_LINE_TRANSITION' },
]

const SAREE_PIECE_CLEARANCE_STATUS_OPTIONS = [
  { value: 'PASSED_GRADE_A_QUALITY', label: 'PASSED_GRADE_A_QUALITY [Default]' },
  { value: 'GRADE_B_MINOR_DEFECT_LOGGED', label: 'GRADE_B_MINOR_DEFECT_LOGGED' },
  { value: 'REJECTED_CRITICAL_WEAVE_FAULT', label: 'REJECTED_CRITICAL_WEAVE_FAULT' },
]

const WEAVER_APPROVAL_STATE_OPTIONS = [
  { value: 'WEAVING_IN_PROGRESS', label: 'WEAVING_IN_PROGRESS [Default]' },
  { value: 'SAREE_COMPLETED_PENDING_CUT', label: 'SAREE_COMPLETED_PENDING_CUT' },
  { value: 'LOOM_STOPPED_MAINTENANCE_REQUIRED', label: 'LOOM_STOPPED_MAINTENANCE_REQUIRED' },
  { value: 'SHIFT_HANDOVER_COMPLETE', label: 'SHIFT_HANDOVER_COMPLETE' },
]

const FIRST_PICK_SAMPLE_STATUS_OPTIONS = [
  { value: 'APPROVED_FLAWLESS', label: 'Approved - Flawless [Default]' },
  { value: 'MINOR_ADJUSTMENT_NEEDED', label: 'Minor Adjustment Needed' },
  { value: 'REJECTED_DESIGN_CORRUPTION', label: 'Rejected (Design Corruption)' },
]

const SHUTTLE_SETUP_MODE_OPTIONS = [
  { value: 'SINGLE_SHUTTLE', label: 'Single Shuttle [Default]' },
  { value: 'TWO_SHUTTLE_DROP_BOX', label: 'Two-Shuttle Drop Box' },
  { value: 'THREE_SHUTTLE_KORVAI_MANUAL_SPLIT', label: 'Three-Shuttle Korvai (Manual Split)' },
]

export default function DashboardMasterWeaver() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [petniJobs, setPetniJobs] = useState([])
  const [jobs, setJobs] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [jobForm, setJobForm] = useState({
    production_run_id: '',
    loom_id: '',
    saree_production_id: '',
    loom_operating_speed_ppm: '',
    warp_let_off_tension_cn: '',
    weft_insertion_feeder_profile: 'DUAL_FEEDER_SILK_GROUND_PLUS_ZARI_MICRO_TENSION',
    dynamic_ppi_control_mode: 'AUTOMATED_MULTI_DENSITY_BODY_PALLU_SWITCH',
    on_loom_defect_category: 'NONE_ZERO_DEFECTS',
    zari_catch_selvage_status: 'PERFECT_CATCH_SMOOTH_EDGE',
    saree_section_phase: 'PALLU_HIGH_DENSITY',
    saree_length_measured_meters: '',
    saree_piece_clearance_status: 'PASSED_GRADE_A_QUALITY',
    weaver_approval_state: 'WEAVING_IN_PROGRESS',
    saree_production_order_no: '',
    reed_width_inches: '',
    picks_per_inch_ppi: '',
    first_pick_sample_status: 'APPROVED_FLAWLESS',
    shuttle_setup_mode: 'SINGLE_SHUTTLE',
    allocated_weft_silk_lot_no: '',
    allocated_zari_lot_no: '',
    issued_zari_weight_gm: '',
    loom_rpm: '',
    target_ppi: '',
    efficiency_percent: '',
    actual_output_yards: '',
    petni_master_job_id: ''
  })

  useEffect(() => {
    fetchPetniJobs()
    fetchJobs()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchPetniJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/petni-master/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setPetniJobs(data.jobs || [])
    } catch (error) {
      console.error('Failed to fetch petni master jobs:', error)
    }
  }

  const fetchJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/master-weaver/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setJobs(data.jobs || [])
    } catch (error) {
      console.error('Failed to fetch master weaver jobs:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/master-weaver/certificates`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setCertificates(data.certificates || [])
    } catch (error) {
      console.error('Failed to fetch master weaver certificates:', error)
    }
  }

  const fetchSalesForecast = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/sales/forecast/master-weaver`, {
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
        loom_operating_speed_ppm: jobForm.loom_operating_speed_ppm ? parseInt(jobForm.loom_operating_speed_ppm) : null,
        warp_let_off_tension_cn: jobForm.warp_let_off_tension_cn ? parseFloat(jobForm.warp_let_off_tension_cn) : null,
        saree_length_measured_meters: jobForm.saree_length_measured_meters ? parseFloat(jobForm.saree_length_measured_meters) : null,
        reed_width_inches: jobForm.reed_width_inches ? parseFloat(jobForm.reed_width_inches) : null,
        picks_per_inch_ppi: jobForm.picks_per_inch_ppi ? parseInt(jobForm.picks_per_inch_ppi) : null,
        issued_zari_weight_gm: jobForm.issued_zari_weight_gm ? parseFloat(jobForm.issued_zari_weight_gm) : null,
        loom_rpm: jobForm.loom_rpm ? parseInt(jobForm.loom_rpm) : null,
        target_ppi: jobForm.target_ppi ? parseInt(jobForm.target_ppi) : null,
        efficiency_percent: jobForm.efficiency_percent ? parseFloat(jobForm.efficiency_percent) : null,
        actual_output_yards: jobForm.actual_output_yards ? parseFloat(jobForm.actual_output_yards) : null,
        petni_master_job_id: jobForm.petni_master_job_id || null
      }

      const response = await fetch(`${API_URL}/master-weaver/jobs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Master weaver production run ${data.production_run_id} created`, 'success')
        setValidationResult({ type: 'success', data })
        setJobForm({
          production_run_id: '', loom_id: '', saree_production_id: '',
          loom_operating_speed_ppm: '', warp_let_off_tension_cn: '',
          weft_insertion_feeder_profile: 'DUAL_FEEDER_SILK_GROUND_PLUS_ZARI_MICRO_TENSION',
          dynamic_ppi_control_mode: 'AUTOMATED_MULTI_DENSITY_BODY_PALLU_SWITCH',
          on_loom_defect_category: 'NONE_ZERO_DEFECTS',
          zari_catch_selvage_status: 'PERFECT_CATCH_SMOOTH_EDGE',
          saree_section_phase: 'PALLU_HIGH_DENSITY', saree_length_measured_meters: '',
          saree_piece_clearance_status: 'PASSED_GRADE_A_QUALITY',
          weaver_approval_state: 'WEAVING_IN_PROGRESS', saree_production_order_no: '',
          reed_width_inches: '', picks_per_inch_ppi: '',
          first_pick_sample_status: 'APPROVED_FLAWLESS', shuttle_setup_mode: 'SINGLE_SHUTTLE',
          allocated_weft_silk_lot_no: '', allocated_zari_lot_no: '', issued_zari_weight_gm: '',
          loom_rpm: '', target_ppi: '', efficiency_percent: '', actual_output_yards: '',
          petni_master_job_id: ''
        })
        fetchJobs()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create master weaver job', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCertify = async (jobId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/master-weaver/jobs/${jobId}/certify`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Master weaver certified: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchJobs()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify master weaver job', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'SAREE_COMPLETED_PENDING_CUT': return 'success'
      case 'SHIFT_HANDOVER_COMPLETE': return 'success'
      case 'WEAVING_IN_PROGRESS': return 'warning'
      case 'LOOM_STOPPED_MAINTENANCE_REQUIRED': return 'error'
      case 'IN_PROGRESS_WEAVING': return 'info'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'READY_FOR_CUT_ROUTE_INSPECTION': return 'success'
      case 'WEAVING_QC_HOLD': return 'error'
      case 'WEAVING_IN_PROGRESS': return 'warning'
      case 'LOW_EFFICIENCY_ALERT': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Master Weaver (Loom Operator & Saree Production Specialist)
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
          {MASTER_WEAVER_TABS.map((t) => (
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
          Master weaver production run {validationResult.data.production_run_id} created successfully
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Run ID:</strong> {certificateDetail.production_run_id}</Typography>
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
              <Typography variant="h6" gutterBottom>Petni Master Jobs (Pre-Process)</Typography>
              {petniJobs.length === 0 ? (
                <Typography color="text.secondary">No approved Petni master jobs found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job ID</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Transition Method</TableCell>
                        <TableCell>Reed Draft</TableCell>
                        <TableCell>Dropper Wire</TableCell>
                        <TableCell>Lease Order</TableCell>
                        <TableCell>Crossed Ends</TableCell>
                        <TableCell>Approval State</TableCell>
                        <TableCell>Routing</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {petniJobs.map((job) => (
                        <TableRow key={job.id}>
                          <TableCell>{job.petni_job_card_id}</TableCell>
                          <TableCell>{job.loom_number_id}</TableCell>
                          <TableCell>{job.petni_transition_method}</TableCell>
                          <TableCell>{job.reed_denting_draft_pattern}</TableCell>
                          <TableCell>{job.dropper_wire_specification}</TableCell>
                          <TableCell>{job.lease_order_verification}</TableCell>
                          <TableCell>{job.crossed_ends_count}</TableCell>
                          <TableCell><Chip label={job.petni_master_approval_state} color={getStatusColor(job.petni_master_approval_state)} size="small" /></TableCell>
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
                    <InputLabel>Link Petni Master Job</InputLabel>
                    <Select value={jobForm.petni_master_job_id} label="Link Petni Master Job"
                      onChange={(e) => setJobForm({ ...jobForm, petni_master_job_id: e.target.value })}>
                      <MenuItem value="">Select job</MenuItem>
                      {petniJobs.map((job) => (
                        <MenuItem key={job.id} value={job.id}>{job.petni_job_card_id} — {job.loom_number_id}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField fullWidth label="Production Run ID" value={jobForm.production_run_id}
                    onChange={(e) => setJobForm({ ...jobForm, production_run_id: e.target.value })}
                    placeholder="e.g., PROD-RUN-2024-001" required />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>2. Category A: Weaving Operation & Loom Kinematics</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Loom ID" value={jobForm.loom_id}
                    onChange={(e) => setJobForm({ ...jobForm, loom_id: e.target.value })}
                    placeholder="e.g., LOOM-2400-001" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Saree Production ID" value={jobForm.saree_production_id}
                    onChange={(e) => setJobForm({ ...jobForm, saree_production_id: e.target.value })}
                    placeholder="e.g., SARE-2024-0001" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Loom Operating Speed (PPM)" value={jobForm.loom_operating_speed_ppm}
                    onChange={(e) => setJobForm({ ...jobForm, loom_operating_speed_ppm: e.target.value })}
                    placeholder="140 - 160" type="number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Warp Let-Off Tension (cN)" value={jobForm.warp_let_off_tension_cn}
                    onChange={(e) => setJobForm({ ...jobForm, warp_let_off_tension_cn: e.target.value })}
                    placeholder="120.0 - 140.0" type="number" inputProps={{ step: '0.1' }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Weft Insertion Feeder Profile</InputLabel>
                    <Select value={jobForm.weft_insertion_feeder_profile} label="Weft Insertion Feeder Profile"
                      onChange={(e) => setJobForm({ ...jobForm, weft_insertion_feeder_profile: e.target.value })}>
                      {WEFT_INSERTION_FEEDER_PROFILE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Dynamic PPI Control Mode</InputLabel>
                    <Select value={jobForm.dynamic_ppi_control_mode} label="Dynamic PPI Control Mode"
                      onChange={(e) => setJobForm({ ...jobForm, dynamic_ppi_control_mode: e.target.value })}>
                      {DYNAMIC_PPI_CONTROL_MODE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Reed Width (inches)" value={jobForm.reed_width_inches}
                    onChange={(e) => setJobForm({ ...jobForm, reed_width_inches: e.target.value })}
                    placeholder="e.g., 48.5" type="number" inputProps={{ step: '0.1' }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Picks Per Inch (PPI)" value={jobForm.picks_per_inch_ppi}
                    onChange={(e) => setJobForm({ ...jobForm, picks_per_inch_ppi: e.target.value })}
                    placeholder="e.g., 88" type="number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Shuttle Setup Mode</InputLabel>
                    <Select value={jobForm.shuttle_setup_mode} label="Shuttle Setup Mode"
                      onChange={(e) => setJobForm({ ...jobForm, shuttle_setup_mode: e.target.value })}>
                      {SHUTTLE_SETUP_MODE_OPTIONS.map((opt) => (
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
              <Typography variant="h6" gutterBottom>3. Category B: Quality Checks & On-Loom Audit</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>On-Loom Defect Category</InputLabel>
                    <Select value={jobForm.on_loom_defect_category} label="On-Loom Defect Category"
                      onChange={(e) => setJobForm({ ...jobForm, on_loom_defect_category: e.target.value })}>
                      {ON_LOOM_DEFECT_CATEGORY_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Zari Catch Selvage Status</InputLabel>
                    <Select value={jobForm.zari_catch_selvage_status} label="Zari Catch Selvage Status"
                      onChange={(e) => setJobForm({ ...jobForm, zari_catch_selvage_status: e.target.value })}>
                      {ZARI_CATCH_SELVAGE_STATUS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Saree Section Phase</InputLabel>
                    <Select value={jobForm.saree_section_phase} label="Saree Section Phase"
                      onChange={(e) => setJobForm({ ...jobForm, saree_section_phase: e.target.value })}>
                      {SAREE_SECTION_PHASE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Saree Length Measured (meters)" value={jobForm.saree_length_measured_meters}
                    onChange={(e) => setJobForm({ ...jobForm, saree_length_measured_meters: e.target.value })}
                    placeholder="5.50 - 6.30" type="number" inputProps={{ step: '0.01' }} />
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
                    <InputLabel>Saree Piece Clearance Status</InputLabel>
                    <Select value={jobForm.saree_piece_clearance_status} label="Saree Piece Clearance Status"
                      onChange={(e) => setJobForm({ ...jobForm, saree_piece_clearance_status: e.target.value })}>
                      {SAREE_PIECE_CLEARANCE_STATUS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Weaver Approval State</InputLabel>
                    <Select value={jobForm.weaver_approval_state} label="Weaver Approval State"
                      onChange={(e) => setJobForm({ ...jobForm, weaver_approval_state: e.target.value })}>
                      {WEAVER_APPROVAL_STATE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>First Pick Sample Status</InputLabel>
                    <Select value={jobForm.first_pick_sample_status} label="First Pick Sample Status"
                      onChange={(e) => setJobForm({ ...jobForm, first_pick_sample_status: e.target.value })}>
                      {FIRST_PICK_SAMPLE_STATUS_OPTIONS.map((opt) => (
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
              <Typography variant="h6" gutterBottom>5. Raw Material Allocation Tracker</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Allocated Weft Silk Lot No." value={jobForm.allocated_weft_silk_lot_no}
                    onChange={(e) => setJobForm({ ...jobForm, allocated_weft_silk_lot_no: e.target.value })}
                    placeholder="Scan or enter lot number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Allocated Zari Lot No." value={jobForm.allocated_zari_lot_no}
                    onChange={(e) => setJobForm({ ...jobForm, allocated_zari_lot_no: e.target.value })}
                    placeholder="Scan or enter lot number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Issued Zari Weight (gm)" value={jobForm.issued_zari_weight_gm}
                    onChange={(e) => setJobForm({ ...jobForm, issued_zari_weight_gm: e.target.value })}
                    placeholder="e.g., 250.0" type="number" inputProps={{ step: '0.1' }} />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Box display="flex" justifyContent="flex-end">
              <Button variant="contained" size="large" onClick={handleJobSubmit} disabled={submitting}>
                {submitting ? 'Submitting...' : 'Submit Production Run'}
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
                All fields from Category A, B, and C are stored with the master weaver job.
                The system automatically enforces validation rules, calculates yield, and tracks efficiency.
              </Typography>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Daily Production Yield Calculation</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Loom RPM" value={jobForm.loom_rpm}
                    onChange={(e) => setJobForm({ ...jobForm, loom_rpm: e.target.value })}
                    placeholder="e.g., 150" type="number" />
                </Grid>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Target PPI" value={jobForm.target_ppi}
                    onChange={(e) => setJobForm({ ...jobForm, target_ppi: e.target.value })}
                    placeholder="e.g., 120" type="number" />
                </Grid>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Efficiency (%)" value={jobForm.efficiency_percent}
                    onChange={(e) => setJobForm({ ...jobForm, efficiency_percent: e.target.value })}
                    placeholder="e.g., 85" type="number" inputProps={{ step: '0.1' }} />
                </Grid>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Actual Output Yards" value={jobForm.actual_output_yards}
                    onChange={(e) => setJobForm({ ...jobForm, actual_output_yards: e.target.value })}
                    placeholder="e.g., 120.0" type="number" inputProps={{ step: '0.1' }} />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Automated Routing & System Guardrails</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 1 (2400 Hook Speed Protection):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF saree_section_phase = PALLU_HIGH_DENSITY AND loom_operating_speed_ppm &gt; 165
                    → BLOCK: OVERSPEED_WARNING
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 2 (Warp Tension Variance Gate):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF warp_let_off_tension_cn &gt; 150.0 OR &lt; 110.0
                    → BLOCK: AUTO_LOOM_PAUSE
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 3 (On-Loom Defect Escalation):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF on_loom_defect_category IN (WEFT_PICK_GAP_LOOM_STOP, BORDER_MISALIGNMENT)
                    → BLOCK: MANDATORY_INSPECTION_STOP
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 4 (Saree Cut-Off Clearance):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF saree_piece_clearance_status NOT IN (PASSED_GRADE_A_QUALITY, GRADE_B_MINOR_DEFECT_LOGGED)
                    → BLOCK: DENY_ELECTRONIC_CUT_MARK_APPROVAL
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Daily Production Yield Calculation:</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    Target Output Yards/Hour = (Loom RPM * 60) / (Target PPI * 36) * Efficiency %
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Low Efficiency Alert:</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF actual_output_yards &lt; 0.8 * target_output_yards_per_hour
                    → SET low_efficiency_alert = TRUE
                  </Typography>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Recent Master Weaver Jobs</Typography>
              {jobs.length === 0 ? (
                <Typography color="text.secondary">No master weaver jobs found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Run ID</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Speed (PPM)</TableCell>
                        <TableCell>Tension (cN)</TableCell>
                        <TableCell>PPI</TableCell>
                        <TableCell>Defect Category</TableCell>
                        <TableCell>Clearance Status</TableCell>
                        <TableCell>Approval State</TableCell>
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {jobs.map((job) => (
                        <TableRow key={job.id}>
                          <TableCell>{job.production_run_id}</TableCell>
                          <TableCell>{job.loom_id}</TableCell>
                          <TableCell>{job.loom_operating_speed_ppm}</TableCell>
                          <TableCell>{job.warp_let_off_tension_cn}</TableCell>
                          <TableCell>{job.picks_per_inch_ppi}</TableCell>
                          <TableCell>{job.on_loom_defect_category}</TableCell>
                          <TableCell><Chip label={job.saree_piece_clearance_status} color={getStatusColor(job.saree_piece_clearance_status)} size="small" /></TableCell>
                          <TableCell><Chip label={job.weaver_approval_state} color={getStatusColor(job.weaver_approval_state)} size="small" /></TableCell>
                          <TableCell>
                            <Button size="small" variant="outlined" onClick={() => handleCertify(job.id)} disabled={job.status === 'SAREE_COMPLETED_PENDING_CUT'}>
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
              <Typography variant="h6" gutterBottom>Master Weaver Certificates</Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No master weaver certificates found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Run ID</TableCell>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Speed (PPM)</TableCell>
                        <TableCell>Tension (cN)</TableCell>
                        <TableCell>PPI</TableCell>
                        <TableCell>Efficiency</TableCell>
                        <TableCell>Low Eff. Alert</TableCell>
                        <TableCell>Certified At</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {certificates.map((cert) => (
                        <TableRow key={cert.id}>
                          <TableCell>{cert.production_run_id}</TableCell>
                          <TableCell>{cert.certificate_hash}</TableCell>
                          <TableCell>{cert.qr_tag_id}</TableCell>
                          <TableCell>{cert.loom_id}</TableCell>
                          <TableCell>{cert.loom_operating_speed_ppm}</TableCell>
                          <TableCell>{cert.warp_let_off_tension_cn}</TableCell>
                          <TableCell>{cert.picks_per_inch_ppi}</TableCell>
                          <TableCell>{cert.efficiency_percent ? `${cert.efficiency_percent.toFixed(1)}%` : '-'}</TableCell>
                          <TableCell>
                            <Chip label={cert.low_efficiency_alert ? 'LOW EFFICIENCY' : 'OK'} color={cert.low_efficiency_alert ? 'error' : 'success'} size="small" />
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
              <Typography variant="h6" gutterBottom>Sales Forecast — Master Weaver Material Plan</Typography>
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
                              <TableCell>Shuttle Mode</TableCell>
                              <TableCell>Est. Runs</TableCell>
                              <TableCell>Speed (PPM)</TableCell>
                              <TableCell>Priority</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.material_requirements.map((item, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{item.saree_category}</TableCell>
                                <TableCell>{item.design_code}</TableCell>
                                <TableCell>{item.shuttle_setup_mode}</TableCell>
                                <TableCell>{item.estimated_production_runs}</TableCell>
                                <TableCell>{item.loom_operating_speed_ppm}</TableCell>
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
                              <TableCell>Est. Runs</TableCell>
                              <TableCell>Loom ID</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.upcoming_lots.map((lot, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{lot.lot_number}</TableCell>
                                <TableCell>{lot.saree_category}</TableCell>
                                <TableCell>{lot.design_code}</TableCell>
                                <TableCell>{lot.estimated_production_runs}</TableCell>
                                <TableCell>{lot.loom_id}</TableCell>
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
      <EnterprisePanel userId={user?.id} factoryNodeId={user?.factory_node_id} />
    </Container>
  )
}
