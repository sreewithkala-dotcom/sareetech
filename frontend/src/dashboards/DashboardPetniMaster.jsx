import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'
import EnterprisePanel from '../components/EnterprisePanel'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const PETNI_MASTER_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Warp Joiner' },
  { id: 'job-creation', label: 'Petni Job & Denting Setup' },
  { id: 'verification', label: 'Quality Inspection & Verification' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const PETNI_TRANSITION_METHOD_OPTIONS = [
  { value: 'COMB_TENSIONED_PETNI_KNOTTING', label: 'Comb-Tensioned Petni Knotting [Default]' },
  { value: 'STANDARD_MANUAL_HAND_PETNI', label: 'Standard Manual Hand-Petni' },
  { value: 'ADHESIVE_TAPE_FUSION_JOINT', label: 'Adhesive Tape Fusion Joint' },
  { value: 'DIRECT_LOOM_DRAW_IN', label: 'Direct Loom Draw-In' },
]

const REED_DENTING_DRAFT_PATTERN_OPTIONS = [
  { value: '4_ENDS_DENT_144_EPI', label: '4 Ends / Dent (144 EPI High-Density) [Default]' },
  { value: '3_ENDS_DENT_120_EPI', label: '3 Ends / Dent (120 EPI Fine)' },
  { value: '2_ENDS_DENT_STANDARD', label: '2 Ends / Dent (Standard)' },
  { value: 'VARIABLE_BORDER_BODY_DRAFT', label: 'Variable Border/Body Draft' },
]

const DROPPER_WIRE_SPECIFICATION_OPTIONS = [
  { value: '0_3G_ULTRA_LIGHTWEIGHT_CLOSED_O_WIRE', label: '0.3g Ultra-Lightweight Closed-O Wire [Default]' },
  { value: '0_5G_OPEN_U_WIRE', label: '0.5g Open-U Wire' },
  { value: '0_7G_STANDARD_HEAVY_WIRE', label: '0.7g Standard Heavy Wire' },
]

const LEASE_ORDER_VERIFICATION_OPTIONS = [
  { value: '1X1_STRICT_LEASE_LOCK', label: '1x1 Strict Lease Lock [Default]' },
  { value: '2X2_GROUP_LEASE_LOCK', label: '2x2 Group Lease Lock' },
  { value: 'UNVERIFIED_LEASE', label: 'Unverified Lease' },
]

const CONTRAST_TYPE_OPTIONS = [
  { value: 'SIDE_BORDERS_ONLY', label: 'Side Borders Only [Default]' },
  { value: 'PALLU_END_PIECE_ONLY', label: 'Pallu (End-piece) Only' },
  { value: 'FULL_BODY_BORDER_PALLU_THREE_SHUTTLE_KORVAI', label: 'Full Body-Border-Pallu (Three-Shuttle Korvai)' },
]

const REED_MARK_LASER_INSPECTION_OPTIONS = [
  { value: 'PASSED_UNIFORM_DENTING', label: 'PASSED_UNIFORM_DENTING [Default]' },
  { value: 'WARNING_MINOR_DENT_SPACING_VAR', label: 'WARNING_MINOR_DENT_SPACING_VAR' },
  { value: 'FAILED_MISDENTED_REED', label: 'FAILED_MISDENTED_REED' },
]

const DROPPER_PINNING_COMPLETION_STATUS_OPTIONS = [
  { value: '100_PERCENT_DROPPERS_PINNED_AND_TESTED', label: '100%_DROPPERS_PINNED_AND_TESTED [Default]' },
  { value: 'PARTIAL_DROPPERS_MISSING', label: 'PARTIAL_DROPPERS_MISSING' },
  { value: 'ELECTRICAL_SHORT_DETECTED', label: 'ELECTRICAL_SHORT_DETECTED' },
]

const PETNI_MASTER_APPROVAL_STATE_OPTIONS = [
  { value: 'PETNI_IN_PROGRESS', label: 'PETNI_IN_PROGRESS [Default]' },
  { value: 'PASSED_APPROVED_FOR_FIRST_PICK', label: 'PASSED_APPROVED_FOR_FIRST_PICK' },
  { value: 'REJECTED_CROSSED_ENDS', label: 'REJECTED_CROSSED_ENDS' },
  { value: 'REJECTED_REED_MISDENT', label: 'REJECTED_REED_MISDENT' },
]

const JOINT_CLEARANCE_STATUS_OPTIONS = [
  { value: 'PASSED_TENSION_TEST', label: 'Passed Tension Test [Default]' },
  { value: 'REQUIRED_RE_SPLICING', label: 'Required Re-Splicing' },
  { value: 'FAILED_WARP_REALIGNMENT_NEEDED', label: 'Failed (Warp Realignment Needed)' },
]

export default function DashboardPetniMaster() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [warpJoinerJobs, setWarpJoinerJobs] = useState([])
  const [jobs, setJobs] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [jobForm, setJobForm] = useState({
    petni_job_card_id: '',
    loom_number_id: '',
    warp_set_id: '',
    petni_transition_method: 'COMB_TENSIONED_PETNI_KNOTTING',
    reed_denting_draft_pattern: '4_ENDS_DENT_144_EPI',
    dropper_wire_specification: '0_3G_ULTRA_LIGHTWEIGHT_CLOSED_O_WIRE',
    lease_order_verification: '1X1_STRICT_LEASE_LOCK',
    contrast_type: 'SIDE_BORDERS_ONLY',
    body_silk_lot_no: '',
    contrast_silk_lot_no: '',
    crossed_ends_count: '0',
    reed_mark_laser_inspection: 'PASSED_UNIFORM_DENTING',
    petni_joint_tension_variance_grams: '',
    border_channel_offset_mm: '',
    dropper_pinning_completion_status: '100_PERCENT_DROPPERS_PINNED_AND_TESTED',
    petni_master_approval_state: 'PETNI_IN_PROGRESS',
    saree_production_order_ref: '',
    total_threads_spliced_count: '',
    joint_clearance_status: 'PASSED_TENSION_TEST',
    contrast_yarn_weight_consumed_kg: '',
    warp_joining_job_id: ''
  })

  useEffect(() => {
    fetchWarpJoinerJobs()
    fetchJobs()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchWarpJoinerJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/warp-joiner/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setWarpJoinerJobs(data.jobs || [])
    } catch (error) {
      console.error('Failed to fetch warp joiner jobs:', error)
    }
  }

  const fetchJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/petni-master/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setJobs(data.jobs || [])
    } catch (error) {
      console.error('Failed to fetch petni master jobs:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/petni-master/certificates`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setCertificates(data.certificates || [])
    } catch (error) {
      console.error('Failed to fetch petni master certificates:', error)
    }
  }

  const fetchSalesForecast = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/sales/forecast/petni-master`, {
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
        crossed_ends_count: jobForm.crossed_ends_count ? parseInt(jobForm.crossed_ends_count) : 0,
        petni_joint_tension_variance_grams: jobForm.petni_joint_tension_variance_grams ? parseFloat(jobForm.petni_joint_tension_variance_grams) : null,
        border_channel_offset_mm: jobForm.border_channel_offset_mm ? parseFloat(jobForm.border_channel_offset_mm) : null,
        total_threads_spliced_count: jobForm.total_threads_spliced_count ? parseInt(jobForm.total_threads_spliced_count) : null,
        contrast_yarn_weight_consumed_kg: jobForm.contrast_yarn_weight_consumed_kg ? parseFloat(jobForm.contrast_yarn_weight_consumed_kg) : null,
        warp_joining_job_id: jobForm.warp_joining_job_id || null
      }

      const response = await fetch(`${API_URL}/petni-master/jobs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Petni master job ${data.petni_job_card_id} created`, 'success')
        setValidationResult({ type: 'success', data })
        setJobForm({
          petni_job_card_id: '', loom_number_id: '', warp_set_id: '',
          petni_transition_method: 'COMB_TENSIONED_PETNI_KNOTTING',
          reed_denting_draft_pattern: '4_ENDS_DENT_144_EPI',
          dropper_wire_specification: '0_3G_ULTRA_LIGHTWEIGHT_CLOSED_O_WIRE',
          lease_order_verification: '1X1_STRICT_LEASE_LOCK',
          contrast_type: 'SIDE_BORDERS_ONLY', body_silk_lot_no: '', contrast_silk_lot_no: '',
          crossed_ends_count: '0', reed_mark_laser_inspection: 'PASSED_UNIFORM_DENTING',
          petni_joint_tension_variance_grams: '', border_channel_offset_mm: '',
          dropper_pinning_completion_status: '100_PERCENT_DROPPERS_PINNED_AND_TESTED',
          petni_master_approval_state: 'PETNI_IN_PROGRESS', saree_production_order_ref: '',
          total_threads_spliced_count: '', joint_clearance_status: 'PASSED_TENSION_TEST',
          contrast_yarn_weight_consumed_kg: '', warp_joining_job_id: ''
        })
        fetchJobs()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create petni master job', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCertify = async (jobId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/petni-master/jobs/${jobId}/certify`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Petni master certified: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchJobs()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify petni master job', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'LOOM_ACTIVE_PRODUCTION': return 'success'
      case 'PASSED_APPROVED_FOR_FIRST_PICK': return 'success'
      case 'PETNI_IN_PROGRESS': return 'warning'
      case 'REJECTED_CROSSED_ENDS': return 'error'
      case 'REJECTED_REED_MISDENT': return 'error'
      case 'PETNI_SETUP_HOLD': return 'error'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'READY_FOR_WEAVING': return 'success'
      case 'LOOM_ACTIVE_PRODUCTION': return 'success'
      case 'PETNI_QC_HOLD': return 'error'
      case 'PETNI_IN_PROGRESS': return 'warning'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Petni Master (Warp Pulling & Reed Denting Specialist)
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
          {PETNI_MASTER_TABS.map((t) => (
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
          Petni master job {validationResult.data.petni_job_card_id} created successfully
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Job:</strong> {certificateDetail.petni_job_card_id}</Typography>
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
              <Typography variant="h6" gutterBottom>Warp Joiner Jobs (Pre-Process)</Typography>
              {warpJoinerJobs.length === 0 ? (
                <Typography color="text.secondary">No approved warp joiner jobs found</Typography>
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
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {warpJoinerJobs.map((job) => (
                        <TableRow key={job.id}>
                          <TableCell>{job.joining_job_card_id}</TableCell>
                          <TableCell>{job.loom_number_id}</TableCell>
                          <TableCell>{job.tying_machine_model}</TableCell>
                          <TableCell>{job.separation_needle_type}</TableCell>
                          <TableCell>{job.target_tying_speed_kpm}</TableCell>
                          <TableCell>{job.knot_type_selection}</TableCell>
                          <TableCell><Chip label={job.warp_joiner_approval_state} color={getStatusColor(job.warp_joiner_approval_state)} size="small" /></TableCell>
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
                    <InputLabel>Link Warp Joiner Job</InputLabel>
                    <Select value={jobForm.warp_joining_job_id} label="Link Warp Joiner Job"
                      onChange={(e) => setJobForm({ ...jobForm, warp_joining_job_id: e.target.value })}>
                      <MenuItem value="">Select job</MenuItem>
                      {warpJoinerJobs.map((job) => (
                        <MenuItem key={job.id} value={job.id}>{job.joining_job_card_id} — {job.loom_number_id}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField fullWidth label="Petni Job Card ID" value={jobForm.petni_job_card_id}
                    onChange={(e) => setJobForm({ ...jobForm, petni_job_card_id: e.target.value })}
                    placeholder="e.g., PETNI-2024-001" required />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>2. Category A: Petni & Denting Setup Configuration</Typography>
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
                    <InputLabel>Petni Transition Method</InputLabel>
                    <Select value={jobForm.petni_transition_method} label="Petni Transition Method"
                      onChange={(e) => setJobForm({ ...jobForm, petni_transition_method: e.target.value })}>
                      {PETNI_TRANSITION_METHOD_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Reed Denting Draft Pattern</InputLabel>
                    <Select value={jobForm.reed_denting_draft_pattern} label="Reed Denting Draft Pattern"
                      onChange={(e) => setJobForm({ ...jobForm, reed_denting_draft_pattern: e.target.value })}>
                      {REED_DENTING_DRAFT_PATTERN_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Dropper Wire Specification</InputLabel>
                    <Select value={jobForm.dropper_wire_specification} label="Dropper Wire Specification"
                      onChange={(e) => setJobForm({ ...jobForm, dropper_wire_specification: e.target.value })}>
                      {DROPPER_WIRE_SPECIFICATION_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Lease Order Verification</InputLabel>
                    <Select value={jobForm.lease_order_verification} label="Lease Order Verification"
                      onChange={(e) => setJobForm({ ...jobForm, lease_order_verification: e.target.value })}>
                      {LEASE_ORDER_VERIFICATION_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Contrast Type</InputLabel>
                    <Select value={jobForm.contrast_type} label="Contrast Type"
                      onChange={(e) => setJobForm({ ...jobForm, contrast_type: e.target.value })}>
                      {CONTRAST_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Body Silk Lot No." value={jobForm.body_silk_lot_no}
                    onChange={(e) => setJobForm({ ...jobForm, body_silk_lot_no: e.target.value })}
                    placeholder="Scan or enter lot number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Contrast Silk Lot No." value={jobForm.contrast_silk_lot_no}
                    onChange={(e) => setJobForm({ ...jobForm, contrast_silk_lot_no: e.target.value })}
                    placeholder="Scan or enter lot number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Total Threads Spliced Count" value={jobForm.total_threads_spliced_count}
                    onChange={(e) => setJobForm({ ...jobForm, total_threads_spliced_count: e.target.value })}
                    placeholder="e.g., 18000" type="number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Saree Production Order Ref" value={jobForm.saree_production_order_ref}
                    onChange={(e) => setJobForm({ ...jobForm, saree_production_order_ref: e.target.value })}
                    placeholder="Scan or enter production order" />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>3. Category B: Quality Inspection & Alignment Audit</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Crossed Ends Count" value={jobForm.crossed_ends_count}
                    onChange={(e) => setJobForm({ ...jobForm, crossed_ends_count: e.target.value })}
                    placeholder="0" type="number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Reed Mark Laser Inspection</InputLabel>
                    <Select value={jobForm.reed_mark_laser_inspection} label="Reed Mark Laser Inspection"
                      onChange={(e) => setJobForm({ ...jobForm, reed_mark_laser_inspection: e.target.value })}>
                      {REED_MARK_LASER_INSPECTION_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Petni Joint Tension Variance (grams)" value={jobForm.petni_joint_tension_variance_grams}
                    onChange={(e) => setJobForm({ ...jobForm, petni_joint_tension_variance_grams: e.target.value })}
                    placeholder="≤±0.3" type="number" inputProps={{ step: '0.1' }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Border Channel Offset (mm)" value={jobForm.border_channel_offset_mm}
                    onChange={(e) => setJobForm({ ...jobForm, border_channel_offset_mm: e.target.value })}
                    placeholder="≤0.2" type="number" inputProps={{ step: '0.1' }} />
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
                    <InputLabel>Dropper Pinning Completion Status</InputLabel>
                    <Select value={jobForm.dropper_pinning_completion_status} label="Dropper Pinning Completion Status"
                      onChange={(e) => setJobForm({ ...jobForm, dropper_pinning_completion_status: e.target.value })}>
                      {DROPPER_PINNING_COMPLETION_STATUS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Petni Master Approval State</InputLabel>
                    <Select value={jobForm.petni_master_approval_state} label="Petni Master Approval State"
                      onChange={(e) => setJobForm({ ...jobForm, petni_master_approval_state: e.target.value })}>
                      {PETNI_MASTER_APPROVAL_STATE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Joint Clearance Status</InputLabel>
                    <Select value={jobForm.joint_clearance_status} label="Joint Clearance Status"
                      onChange={(e) => setJobForm({ ...jobForm, joint_clearance_status: e.target.value })}>
                      {JOINT_CLEARANCE_STATUS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Box display="flex" justifyContent="flex-end">
              <Button variant="contained" size="large" onClick={handleJobSubmit} disabled={submitting}>
                {submitting ? 'Submitting...' : 'Submit Petni Master Job'}
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
                All fields from Category A, B, and C are stored with the Petni master job.
                The system automatically enforces validation rules and tracks contrast inventory depletion.
              </Typography>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Contrast Inventory Depletion</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Body Silk Lot No." value={jobForm.body_silk_lot_no}
                    onChange={(e) => setJobForm({ ...jobForm, body_silk_lot_no: e.target.value })}
                    placeholder="Scan or enter lot number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Contrast Silk Lot No." value={jobForm.contrast_silk_lot_no}
                    onChange={(e) => setJobForm({ ...jobForm, contrast_silk_lot_no: e.target.value })}
                    placeholder="Scan or enter lot number" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Contrast Yarn Weight Consumed (kg)" value={jobForm.contrast_yarn_weight_consumed_kg}
                    onChange={(e) => setJobForm({ ...jobForm, contrast_yarn_weight_consumed_kg: e.target.value })}
                    placeholder="e.g., 2.5" type="number" inputProps={{ step: '0.001' }} />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Automated Routing & System Guardrails</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 1 (Zero Crossed Ends Guardrail):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF crossed_ends_count &gt; 0
                    → BLOCK: CROSSED_ENDS_DETECTED
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 2 (Dropper Wire Weight Compliance):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF reed_denting_draft_pattern = 4 Ends / Dent (144 EPI) AND dropper_wire_specification = 0.7g Standard Heavy Wire
                    → BLOCK: INVALID_DROPPER_WEIGHT
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 3 (Reed Denting Quality Gate):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF reed_mark_laser_inspection = FAILED_MISDENTED_REED
                    → BLOCK: CANNOT_CLEAR_LOOM
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Rule 4 (First-Pick Production Authorization):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    IF petni_master_approval_state != PASSED_APPROVED_FOR_FIRST_PICK
                    → BLOCK: DENY_LOOM_WEAVER_START_SIGNAL
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Contrast Inventory Depletion (Multi-Lot Balancing):</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    On submit, deduct contrast yarn weight from Dyed Silk Stock Ledger and allocate to loom asset.
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" gutterBottom><strong>Saree Value-Add Multiplier:</strong></Typography>
                  <Typography variant="body2" color="text.secondary">
                    Automatically apply Labor Multiplier Code to child production order for Korvai/Petni sarees.
                  </Typography>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Recent Petni Master Jobs</Typography>
              {jobs.length === 0 ? (
                <Typography color="text.secondary">No petni master jobs found</Typography>
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
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {jobs.map((job) => (
                        <TableRow key={job.id}>
                          <TableCell>{job.petni_job_card_id}</TableCell>
                          <TableCell>{job.loom_number_id}</TableCell>
                          <TableCell>{job.petni_transition_method}</TableCell>
                          <TableCell>{job.reed_denting_draft_pattern}</TableCell>
                          <TableCell>{job.dropper_wire_specification}</TableCell>
                          <TableCell>{job.lease_order_verification}</TableCell>
                          <TableCell>{job.crossed_ends_count}</TableCell>
                          <TableCell><Chip label={job.petni_master_approval_state} color={getStatusColor(job.petni_master_approval_state)} size="small" /></TableCell>
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
              <Typography variant="h6" gutterBottom>Petni Master Certificates</Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No petni master certificates found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job ID</TableCell>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Transition Method</TableCell>
                        <TableCell>Reed Draft</TableCell>
                        <TableCell>Threads Spliced</TableCell>
                        <TableCell>Contrast Type</TableCell>
                        <TableCell>Routing</TableCell>
                        <TableCell>Certified At</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {certificates.map((cert) => (
                        <TableRow key={cert.id}>
                          <TableCell>{cert.petni_job_card_id}</TableCell>
                          <TableCell>{cert.certificate_hash}</TableCell>
                          <TableCell>{cert.qr_tag_id}</TableCell>
                          <TableCell>{cert.loom_number_id}</TableCell>
                          <TableCell>{cert.petni_transition_method}</TableCell>
                          <TableCell>{cert.reed_denting_draft_pattern}</TableCell>
                          <TableCell>{cert.total_threads_spliced_count}</TableCell>
                          <TableCell>{cert.contrast_type}</TableCell>
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
              <Typography variant="h6" gutterBottom>Sales Forecast — Petni Master Material Plan</Typography>
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
                              <TableCell>Transition Method</TableCell>
                              <TableCell>Est. Petni Jobs</TableCell>
                              <TableCell>Threads Spliced</TableCell>
                              <TableCell>Priority</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.material_requirements.map((item, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{item.saree_category}</TableCell>
                                <TableCell>{item.design_code}</TableCell>
                                <TableCell>{item.petni_transition_method}</TableCell>
                                <TableCell>{item.estimated_petni_jobs}</TableCell>
                                <TableCell>{item.total_threads_spliced_count}</TableCell>
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
                              <TableCell>Est. Petni Jobs</TableCell>
                              <TableCell>Loom ID</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.upcoming_lots.map((lot, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{lot.lot_number}</TableCell>
                                <TableCell>{lot.saree_category}</TableCell>
                                <TableCell>{lot.design_code}</TableCell>
                                <TableCell>{lot.estimated_petni_jobs}</TableCell>
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
      <EnterprisePanel userId={user?.id} factoryNodeId={user?.factory_node_id} />
    </Container>
  )
}
