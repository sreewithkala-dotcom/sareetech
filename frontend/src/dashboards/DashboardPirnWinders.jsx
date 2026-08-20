import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const PIRN_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Bobbin Winder' },
  { id: 'job-creation', label: 'Job Creation & Machine Setup' },
  { id: 'quality-audit', label: 'Post-Winding Quality Audit' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const PIRN_MACHINE_TYPE_OPTIONS = [
  { value: 'AUTOMATIC_HIGH_SPEED_PIRN_WINDER', label: 'Automatic High-Speed Pirn Winder' },
  { value: 'MANUAL_SPINDLE_WINDER', label: 'Manual Spindle Winder' },
  { value: 'PRECISION_FEEL_WHEEL_WINDER', label: 'Precision Feel-Wheel Winder' },
]

const WEFT_JOINT_METHOD_OPTIONS = [
  { value: 'AIR_SPLICING', label: 'Air Splicing' },
  { value: 'MICRO_MECHANICAL_JOIN', label: 'Micro Mechanical Join' },
  { value: 'STANDARD_WEAVERS_KNOT', label: "Standard Weaver's Knot" },
]

const SLOUGHING_RISK_OPTIONS = [
  { value: 'GRADE_5_ZERO_RISK_COMPACT', label: 'Grade 5 (Zero Risk / Compact)' },
  { value: 'GRADE_3_SLIGHT_LOOSE_COILS', label: 'Grade 3 (Slight Loose Coils)' },
  { value: 'GRADE_1_HIGH_SLOUGH_RISK', label: 'Grade 1 (High Slough Risk)' },
]

const PIRN_SURFACE_INSPECTION_OPTIONS = [
  { value: 'SMOOTH_FLAWLESS', label: 'Smooth / Flawless' },
  { value: 'ROUGH_CHAFED_FILAMENTS', label: 'Rough / Chafed Filaments' },
]

const TARGET_MACHINE_OPTIONS = [
  { value: '1536_HOOK_JACQUARD', label: '1536 Hook Jacquard (Standard Luxury Brocade)' },
  { value: '2400_HOOK_JACQUARD', label: '2400 Hook Jacquard (High-Density Fine Motif)' },
  { value: 'HANDLOOM', label: 'Handloom' },
  { value: 'POWERLOOM', label: 'Powerloom' },
  { value: 'RAPIER_LOOM', label: 'Rapier Loom' },
]

export default function DashboardPirnWinders() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [bobbinJobCards, setBobbinJobCards] = useState([])
  const [pirnJobs, setPirnJobs] = useState([])
  const [pirns, setPirns] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [jobForm, setJobForm] = useState({
    pirn_winding_job_no: '',
    bobbin_winder_job_card_id: '',
    bobbin_record_id: '',
    winding_machine_id: '',
    pirn_machine_type: 'AUTOMATIC_HIGH_SPEED_PIRN_WINDER',
    source_bobbin_lot_no: '',
    target_pirn_count_qty: 1000,
    input_yarn_weight_kg: '',
    output_pirn_net_weight_kg: '',
    pirn_scrap_waste_gm: 0,
    spindle_speed_rpm: 800,
    pirn_base_and_nose_taper_deg: '16.5',
    weft_joint_method: 'AIR_SPLICING',
    pirn_hardness_shore_d: '72.0',
    splices_per_pirn: 0,
    sloughing_risk_index: 'GRADE_5_ZERO_RISK_COMPACT',
    pirn_surface_inspection: 'SMOOTH_FLAWLESS',
    target_machine_type: '1536_HOOK_JACQUARD'
  })

  const [pirnForm, setPirnForm] = useState({
    pirn_id: '',
    carrier_type: 'Pirn',
    yarn_type: 'WEFT_TRAM_LOW_TWIST',
    silk_fiber_variety: 'PURE_MULBERRY_SILK',
    source_bobbin_lot_no: '',
    net_weight_kg: '',
    pirn_hardness_shore_d: '72.0',
    sloughing_risk_index: 'GRADE_5_ZERO_RISK_COMPACT',
    pirn_surface_inspection: 'SMOOTH_FLAWLESS',
    weft_joint_method: 'AIR_SPLICING',
    splices_per_pirn: 0
  })

  useEffect(() => {
    fetchBobbinJobCards()
    fetchPirnJobs()
    fetchPirns()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchBobbinJobCards = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/winding/job-cards`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setBobbinJobCards(data.job_cards || [])
    } catch (error) {
      console.error('Failed to fetch bobbin job cards:', error)
    }
  }

  const fetchPirnJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/pirn-winding/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setPirnJobs(data.jobs || [])
    } catch (error) {
      console.error('Failed to fetch pirn jobs:', error)
    }
  }

  const fetchPirns = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/pirn-winding/pirns`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setPirns(data.pirns || [])
    } catch (error) {
      console.error('Failed to fetch pirns:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/pirn-winding/certificates`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/pirn-winding`, {
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
        bobbin_winder_job_card_id: jobForm.bobbin_winder_job_card_id || null,
        bobbin_record_id: jobForm.bobbin_record_id || null,
        target_pirn_count_qty: jobForm.target_pirn_count_qty ? parseInt(jobForm.target_pirn_count_qty) : null,
        input_yarn_weight_kg: jobForm.input_yarn_weight_kg ? parseFloat(jobForm.input_yarn_weight_kg) : null,
        output_pirn_net_weight_kg: jobForm.output_pirn_net_weight_kg ? parseFloat(jobForm.output_pirn_net_weight_kg) : null,
        pirn_scrap_waste_gm: jobForm.pirn_scrap_waste_gm ? parseFloat(jobForm.pirn_scrap_waste_gm) : 0,
        spindle_speed_rpm: jobForm.spindle_speed_rpm ? parseInt(jobForm.spindle_speed_rpm) : null,
        pirn_base_and_nose_taper_deg: jobForm.pirn_base_and_nose_taper_deg ? parseFloat(jobForm.pirn_base_and_nose_taper_deg) : null,
        pirn_hardness_shore_d: jobForm.pirn_hardness_shore_d ? parseFloat(jobForm.pirn_hardness_shore_d) : null,
        splices_per_pirn: jobForm.splices_per_pirn || 0
      }

      const response = await fetch(`${API_URL}/pirn-winding/jobs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Pirn job ${data.pirn_winding_job_no} created`, 'success')
        setValidationResult({ type: 'success', data })
        setJobForm({
          pirn_winding_job_no: '', bobbin_winder_job_card_id: '', bobbin_record_id: '',
          winding_machine_id: '', pirn_machine_type: 'AUTOMATIC_HIGH_SPEED_PIRN_WINDER',
          source_bobbin_lot_no: '', target_pirn_count_qty: 1000,
          input_yarn_weight_kg: '', output_pirn_net_weight_kg: '', pirn_scrap_waste_gm: 0,
          spindle_speed_rpm: 800, pirn_base_and_nose_taper_deg: '16.5',
          weft_joint_method: 'AIR_SPLICING', pirn_hardness_shore_d: '72.0',
          splices_per_pirn: 0, sloughing_risk_index: 'GRADE_5_ZERO_RISK_COMPACT',
          pirn_surface_inspection: 'SMOOTH_FLAWLESS', target_machine_type: '1536_HOOK_JACQUARD'
        })
        fetchPirnJobs()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create pirn job', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCreatePirn = async () => {
    if (!jobForm.pirn_winding_job_no) {
      addNotification('Please create a job first', 'error')
      return
    }
    setSubmitting(true)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...pirnForm,
        job_id: pirnJobs.find(j => j.pirn_winding_job_no === jobForm.pirn_winding_job_no)?.id,
        net_weight_kg: pirnForm.net_weight_kg ? parseFloat(pirnForm.net_weight_kg) : 0
      }

      const response = await fetch(`${API_URL}/pirn-winding/pirns`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Pirn ${data.pirn_id} created`, 'success')
        setPirnForm({
          pirn_id: '', carrier_type: 'Pirn', yarn_type: 'WEFT_TRAM_LOW_TWIST',
          silk_fiber_variety: 'PURE_MULBERRY_SILK', source_bobbin_lot_no: '',
          net_weight_kg: '', pirn_hardness_shore_d: '72.0',
          sloughing_risk_index: 'GRADE_5_ZERO_RISK_COMPACT',
          pirn_surface_inspection: 'SMOOTH_FLAWLESS',
          weft_joint_method: 'AIR_SPLICING', splices_per_pirn: 0
        })
        fetchPirns()
      } else {
        addNotification(data.error || 'Failed to create pirn', 'error')
      }
    } catch (error) {
      addNotification('Failed to create pirn', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleComplete = async (jobId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/pirn-winding/jobs/${jobId}/complete`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Job completed', 'success')
        fetchPirnJobs()
      } else {
        addNotification(data.error || 'Completion failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to complete job', 'error')
    }
  }

  const handleCertify = async (jobId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/pirn-winding/jobs/${jobId}/certify`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Job certified: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchPirnJobs()
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
      case 'CERTIFIED': return 'success'
      case 'REJECTED': return 'error'
      case 'IN_PROGRESS': return 'info'
      case 'QC_HOLD': return 'warning'
      case 'COMPLETED': return 'info'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'WEFT_SHUTTLE_READY': return 'success'
      case 'REJECTED_RE_WINDING': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Pirn Winders — Weft Pirn Winding & Quality Audit
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
          {PIRN_TABS.map((t) => (
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
          {tab === 'job-creation' && `Pirn job ${validationResult.data.pirn_winding_job_no} created`}
          {tab === 'quality-audit' && 'Quality audit recorded'}
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Job No:</strong> {certificateDetail.pirn_winding_job_no}</Typography>
          <Typography variant="body2"><strong>Hash:</strong> {certificateDetail.certificate_hash}</Typography>
          <Typography variant="body2"><strong>QR Tag:</strong> {certificateDetail.qr_tag_id}</Typography>
        </Alert>
      )}

      {/* ===================== PRE-PROCESS TAB ===================== */}
      {tab === 'preprocess' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Bobbin Winder Job Cards (Pre-Process)</Typography>
              {bobbinJobCards.length === 0 ? (
                <Typography color="text.secondary">No bobbin job cards found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job Card ID</TableCell>
                        <TableCell>Machine</TableCell>
                        <TableCell>Dyed Lot</TableCell>
                        <TableCell>Yarn Type</TableCell>
                        <TableCell>Carrier</TableCell>
                        <TableCell>Input (kg)</TableCell>
                        <TableCell>Output (kg)</TableCell>
                        <TableCell>Waste %</TableCell>
                        <TableCell>Routing</TableCell>
                        <TableCell>Status</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {bobbinJobCards.map((card) => (
                        <TableRow key={card.id}>
                          <TableCell>{card.winding_job_card_id}</TableCell>
                          <TableCell>{card.spindle_machine_id}</TableCell>
                          <TableCell>{card.input_dyed_lot_no}</TableCell>
                          <TableCell>{card.yarn_type}</TableCell>
                          <TableCell>{card.carrier_destination_type}</TableCell>
                          <TableCell>{card.allocated_input_weight_kg}</TableCell>
                          <TableCell>{card.output_wound_weight_kg}</TableCell>
                          <TableCell>{card.waste_variance_percent}%</TableCell>
                          <TableCell><Chip label={card.auto_assigned_routing} color={getRoutingColor(card.auto_assigned_routing)} size="small" /></TableCell>
                          <TableCell><Chip label={card.status} color={getStatusColor(card.status)} size="small" /></TableCell>
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

      {/* ===================== JOB CREATION & MACHINE SETUP TAB ===================== */}
      {tab === 'job-creation' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>1. Pre-Process Linkage</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel>Select Bobbin Winder Job Card</InputLabel>
                    <Select value={jobForm.bobbin_winder_job_card_id} label="Select Bobbin Winder Job Card"
                      onChange={(e) => setJobForm({ ...jobForm, bobbin_winder_job_card_id: e.target.value })}>
                      <MenuItem value="">Select job card</MenuItem>
                      {bobbinJobCards.map((card) => (
                        <MenuItem key={card.id} value={card.id}>{card.winding_job_card_id} — {card.input_dyed_lot_no}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField fullWidth label="Pirn Winding Job No" value={jobForm.pirn_winding_job_no}
                    onChange={(e) => setJobForm({ ...jobForm, pirn_winding_job_no: e.target.value })}
                    placeholder="Auto-generated or scan" required />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>2. Operations & Resource Metadata</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Winding Machine ID" value={jobForm.winding_machine_id}
                    onChange={(e) => setJobForm({ ...jobForm, winding_machine_id: e.target.value })}
                    placeholder="Scan or enter machine ID" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Pirn Machine Type</InputLabel>
                    <Select value={jobForm.pirn_machine_type} label="Pirn Machine Type"
                      onChange={(e) => setJobForm({ ...jobForm, pirn_machine_type: e.target.value })}>
                      {PIRN_MACHINE_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Target Machine Type</InputLabel>
                    <Select value={jobForm.target_machine_type} label="Target Machine Type"
                      onChange={(e) => setJobForm({ ...jobForm, target_machine_type: e.target.value })}>
                      {TARGET_MACHINE_OPTIONS.map((opt) => (
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
              <Typography variant="h6" gutterBottom>3. Material & Output Intakes</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Source Bobbin Lot No" value={jobForm.source_bobbin_lot_no}
                    onChange={(e) => setJobForm({ ...jobForm, source_bobbin_lot_no: e.target.value })}
                    placeholder="Scan or enter bobbin lot no" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Target Pirn Count Qty" type="number"
                    value={jobForm.target_pirn_count_qty}
                    onChange={(e) => setJobForm({ ...jobForm, target_pirn_count_qty: e.target.value })}
                    inputProps={{ step: '1', min: 1 }} required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Input Yarn Weight (kg)" type="number"
                    value={jobForm.input_yarn_weight_kg}
                    onChange={(e) => setJobForm({ ...jobForm, input_yarn_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Output Pirn Net Weight (kg)" type="number"
                    value={jobForm.output_pirn_net_weight_kg}
                    onChange={(e) => setJobForm({ ...jobForm, output_pirn_net_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Pirn Scrap Waste (gm)" type="number"
                    value={jobForm.pirn_scrap_waste_gm}
                    onChange={(e) => setJobForm({ ...jobForm, pirn_scrap_waste_gm: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} required />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>4. Winding Setup & Machine Controls</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Spindle Speed (RPM)" type="number"
                    value={jobForm.spindle_speed_rpm}
                    onChange={(e) => setJobForm({ ...jobForm, spindle_speed_rpm: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="500-650 RPM for 2400-hook, 800-1000 RPM for 1536-hook" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Pirn Base and Nose Taper (deg)" type="number"
                    value={jobForm.pirn_base_and_nose_taper_deg}
                    onChange={(e) => setJobForm({ ...jobForm, pirn_base_and_nose_taper_deg: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} helperText="Target: 15-18°" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Weft Joint Method</InputLabel>
                    <Select value={jobForm.weft_joint_method} label="Weft Joint Method"
                      onChange={(e) => setJobForm({ ...jobForm, weft_joint_method: e.target.value })}>
                      {WEFT_JOINT_METHOD_OPTIONS.map((opt) => (
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
              <Box sx={{ display: 'flex', gap: 2 }}>
                <Button variant="contained" onClick={handleJobSubmit} disabled={submitting}>
                  {submitting ? 'Submitting...' : 'Save Pirn Winding Job'}
                </Button>
              </Box>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Pirn Winding Jobs</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Job No</TableCell>
                      <TableCell>Machine</TableCell>
                      <TableCell>Source Bobbin Lot</TableCell>
                      <TableCell>Pirn Count</TableCell>
                      <TableCell>Input (kg)</TableCell>
                      <TableCell>Output (kg)</TableCell>
                      <TableCell>Waste (gm)</TableCell>
                      <TableCell>Variance (kg)</TableCell>
                      <TableCell>Routing</TableCell>
                      <TableCell>Status</TableCell>
                      <TableCell>Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {pirnJobs.map((job) => (
                      <TableRow key={job.id}>
                        <TableCell>{job.pirn_winding_job_no}</TableCell>
                        <TableCell>{job.winding_machine_id}</TableCell>
                        <TableCell>{job.source_bobbin_lot_no}</TableCell>
                        <TableCell>{job.target_pirn_count_qty}</TableCell>
                        <TableCell>{job.input_yarn_weight_kg}</TableCell>
                        <TableCell>{job.output_pirn_net_weight_kg}</TableCell>
                        <TableCell>{job.pirn_scrap_waste_gm}</TableCell>
                        <TableCell>{job.material_variance_kg}</TableCell>
                        <TableCell><Chip label={job.auto_assigned_routing} color={getRoutingColor(job.auto_assigned_routing)} size="small" /></TableCell>
                        <TableCell><Chip label={job.status} color={getStatusColor(job.status)} size="small" /></TableCell>
                        <TableCell>
                          {job.status === 'IN_PROGRESS' || job.status === 'DRAFT' ? (
                            <>
                              <Button size="small" variant="outlined" color="success" onClick={() => handleComplete(job.id)}>Complete</Button>
                              <Button size="small" variant="outlined" color="primary" onClick={() => handleCertify(job.id)} sx={{ ml: 1 }}>Certify</Button>
                            </>
                          ) : (
                            <Chip label={job.certificate_hash ? 'Certified' : 'Processed'} size="small" />
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

      {/* ===================== POST-WINDING QUALITY AUDIT TAB ===================== */}
      {tab === 'quality-audit' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>1. Post-Winding Quality & Defect Checks</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Weft Joint Method</InputLabel>
                    <Select value={jobForm.weft_joint_method} label="Weft Joint Method"
                      onChange={(e) => setJobForm({ ...jobForm, weft_joint_method: e.target.value })}>
                      {WEFT_JOINT_METHOD_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Spindle Speed (RPM)" type="number"
                    value={jobForm.spindle_speed_rpm}
                    onChange={(e) => setJobForm({ ...jobForm, spindle_speed_rpm: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="500-650 RPM for 2400-hook, 800-1000 RPM for 1536-hook" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Pirn Base and Nose Taper (deg)" type="number"
                    value={jobForm.pirn_base_and_nose_taper_deg}
                    onChange={(e) => setJobForm({ ...jobForm, pirn_base_and_nose_taper_deg: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} helperText="Target: 15-18°" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Pirn Hardness Shore D" type="number"
                    value={jobForm.pirn_hardness_shore_d}
                    onChange={(e) => setJobForm({ ...jobForm, pirn_hardness_shore_d: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} helperText="Target: 70-75 Shore D for 2400-hook" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Splices per Pirn" type="number"
                    value={jobForm.splices_per_pirn}
                    onChange={(e) => setJobForm({ ...jobForm, splices_per_pirn: parseInt(e.target.value) || 0 })}
                    inputProps={{ step: '1', min: 0 }} helperText="Max 0 for 2400-hook" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Sloughing Risk Index</InputLabel>
                    <Select value={jobForm.sloughing_risk_index} label="Sloughing Risk Index"
                      onChange={(e) => setJobForm({ ...jobForm, sloughing_risk_index: e.target.value })}>
                      {SLOUGHING_RISK_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Pirn Surface Inspection</InputLabel>
                    <Select value={jobForm.pirn_surface_inspection} label="Pirn Surface Inspection"
                      onChange={(e) => setJobForm({ ...jobForm, pirn_surface_inspection: e.target.value })}>
                      {PIRN_SURFACE_INSPECTION_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
              <Alert severity="info" sx={{ mt: 3 }}>
                <Typography variant="subtitle2">Automated ERP Routing & Validation Rules</Typography>
                <Typography variant="body2">
                  • 2400 Hook + Splices per Pirn > 0 → REJECT_FOR_2400_WEFT (NO_KNOTS_ALLOWED_IN_HIGH_DENSITY_WEFT)<br/>
                  • 2400 Hook + Pirn Hardness < 70.0 Shore D → SOFT_PIRN_SLOUGHING_RISK (INCREASE_WINDING_TENSION)<br/>
                  • 2400 Hook + Spindle Speed > 650 RPM → HIGH_SPINDLE_SPEED_FRICTION_RISK<br/>
                  • Sloughing Risk Index Grade 3 or 1 → REJECT_NEEDS_RE_WINDING<br/>
                  • Material Variance > 0.3% → MATERIAL_VARIANCE_LEAKAGE
                </Typography>
              </Alert>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>2. Create Pirn Record</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Pirn ID" value={pirnForm.pirn_id}
                    onChange={(e) => setPirnForm({ ...pirnForm, pirn_id: e.target.value })}
                    placeholder="Auto-generated or scan" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Carrier Type</InputLabel>
                    <Select value={pirnForm.carrier_type} label="Carrier Type"
                      onChange={(e) => setPirnForm({ ...pirnForm, carrier_type: e.target.value })}>
                      <MenuItem value="Pirn">Pirn (Quill)</MenuItem>
                      <MenuItem value="Paper Cone">Paper Cone</MenuItem>
                      <MenuItem value="Plastic Spool">Plastic Spool</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Yarn Type</InputLabel>
                    <Select value={pirnForm.yarn_type} label="Yarn Type"
                      onChange={(e) => setPirnForm({ ...pirnForm, yarn_type: e.target.value })}>
                      <MenuItem value="WEFT_TRAM_LOW_TWIST">Weft Tram Low Twist</MenuItem>
                      <MenuItem value="WARP_ORGANZINE_HIGH_TWIST">Warp Organzine High Twist</MenuItem>
                      <MenuItem value="CREPE_ULTRA_TWIST">Crepe Ultra Twist</MenuItem>
                      <MenuItem value="DUPION_SLUB_YARN">Dupion Slub Yarn</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Net Weight (kg)" type="number"
                    value={pirnForm.net_weight_kg}
                    onChange={(e) => setPirnForm({ ...pirnForm, net_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Structural Verdict</InputLabel>
                    <Select value={pirnForm.sloughing_risk_index} label="Structural Verdict"
                      onChange={(e) => setPirnForm({ ...pirnForm, sloughing_risk_index: e.target.value })}>
                      {SLOUGHING_RISK_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Knot Method</InputLabel>
                    <Select value={pirnForm.weft_joint_method} label="Knot Method"
                      onChange={(e) => setPirnForm({ ...pirnForm, weft_joint_method: e.target.value })}>
                      {WEFT_JOINT_METHOD_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleCreatePirn} disabled={submitting}>
                    {submitting ? 'Creating...' : 'Create Pirn Record'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== CERTIFICATES & SALES FORECAST TAB ===================== */}
      {tab === 'certificates-forecast' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Pirn Winding Certificates — Post-Process Output</Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates issued yet</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag</TableCell>
                        <TableCell>Job No</TableCell>
                        <TableCell>Source Bobbin Lot</TableCell>
                        <TableCell>Pirn Count</TableCell>
                        <TableCell>Hardness (Shore D)</TableCell>
                        <TableCell>Splices</TableCell>
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
                          <TableCell>{cert.pirn_winding_job_no}</TableCell>
                          <TableCell>{cert.source_bobbin_lot_no}</TableCell>
                          <TableCell>{cert.target_pirn_count_qty}</TableCell>
                          <TableCell>{cert.pirn_hardness_shore_d}</TableCell>
                          <TableCell>{cert.splices_per_pirn}</TableCell>
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
              <Typography variant="h6" gutterBottom>Sales Forecast — Pirn Winding Material Processing Plan</Typography>
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
                          <Typography color="textSecondary" gutterBottom>Material Lines</Typography>
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
                          <TableCell>Shade Code</TableCell>
                          <TableCell>Yarn Type</TableCell>
                          <TableCell>Target Pirn Count</TableCell>
                          <TableCell>Est. Silk (kg)</TableCell>
                          <TableCell>Priority</TableCell>
                          <TableCell>Target Machine</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.material_requirements.map((item, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{item.saree_category}</TableCell>
                            <TableCell>{item.shade_code}</TableCell>
                            <TableCell>{item.yarn_type}</TableCell>
                            <TableCell>{item.target_pirn_count}</TableCell>
                            <TableCell>{item.estimated_silk_kg}</TableCell>
                            <TableCell><Chip label={item.priority} color={item.priority === 'HIGH' ? 'error' : 'warning'} size="small" /></TableCell>
                            <TableCell>{item.target_machine}</TableCell>
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
                          <TableCell>Shade Code</TableCell>
                          <TableCell>Target Pirn Count</TableCell>
                          <TableCell>Est. Sarees</TableCell>
                          <TableCell>Est. Silk (kg)</TableCell>
                          <TableCell>Yarn Type</TableCell>
                          <TableCell>Target Machine</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.upcoming_lots.map((lot, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{lot.lot_number}</TableCell>
                            <TableCell>{lot.saree_category}</TableCell>
                            <TableCell>{lot.shade_code}</TableCell>
                            <TableCell>{lot.target_pirn_count}</TableCell>
                            <TableCell>{lot.estimated_sarees}</TableCell>
                            <TableCell>{lot.estimated_silk_kg}</TableCell>
                            <TableCell>{lot.yarn_type}</TableCell>
                            <TableCell>{lot.target_machine}</TableCell>
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
