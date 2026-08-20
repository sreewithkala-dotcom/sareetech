import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider, Checkbox, FormControlLabel } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const WINDING_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Skein Dye' },
  { id: 'job-creation', label: 'Job Creation & Machine Setup' },
  { id: 'quality-audit', label: 'Post-Winding Quality Audit' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const WINDING_OPERATION_TYPE_OPTIONS = [
  { value: 'ROUTINE_PRODUCTION', label: 'Routine Production' },
  { value: 'SAMPLE_CONING', label: 'Sample Coning' },
  { value: 'RE_WINDING_CORRECTION', label: 'Re-Winding Correction' },
  { value: 'CLEANING_RUN', label: 'Cleaning Run' },
]

const WINDING_MACHINE_TYPE_OPTIONS = [
  { value: 'HIGH_SPEED_AUTOMATIC_CONER', label: 'High Speed Automatic Coner' },
  { value: 'SEMI_AUTOMATIC_BOBBIN_WINDER', label: 'Semi-Automatic Bobbin Winder' },
  { value: 'TRADITIONAL_HAND_WINDER', label: 'Traditional Hand Winder' },
]

const WORKER_SHIFT_OPTIONS = [
  { value: 'SHIFT_A_MORNING', label: 'Shift A Morning (06:00-14:00)' },
  { value: 'SHIFT_B_EVENING', label: 'Shift B Evening (14:00-22:00)' },
  { value: 'SHIFT_C_NIGHT', label: 'Shift C Night (22:00-06:00)' },
]

const YARN_PROCESSING_PROFILE_OPTIONS = [
  { value: 'WARP_ORGANZINE_HIGH_TWIST', label: 'Warp Organzine High Twist' },
  { value: 'WEFT_TRAM_LOW_TWIST', label: 'Weft Tram Low Twist' },
  { value: 'CREPE_ULTRA_TWIST', label: 'Crepe Ultra Twist' },
  { value: 'DUPION_SLUB_YARN', label: 'Dupion Slub Yarn' },
]

const SILK_FIBER_VARIETY_OPTIONS = [
  { value: 'PURE_MULBERRY_SILK', label: 'Pure Mulberry Silk' },
  { value: 'ORGANIC_TUSSAR_WILD', label: 'Organic Tussar Wild' },
  { value: 'MATTE_ERI_SPUN', label: 'Matte Eri Spun' },
  { value: 'SHIMMERING_MUGA', label: 'Shimmering Muga' },
]

const TARGET_CARRIER_TYPE_OPTIONS = [
  { value: 'FLANGED_PLASTIC_BOBBIN', label: 'Flanged Plastic Bobbin' },
  { value: 'TAPERED_PAPER_CONE', label: 'Tapered Paper Cone' },
  { value: 'CYLINDRICAL_PLASTIC_CHEESE', label: 'Cylindrical Plastic Cheese' },
  { value: 'WOODEN_HANK_SWIFT_SPOOL', label: 'Wooden Hank Swift Spool' },
]

const CARRIER_DESTINATION_TYPE_OPTIONS = [
  { value: 'Flanged Bobbin', label: 'Flanged Bobbin' },
  { value: 'Paper Cone', label: 'Paper Cone' },
  { value: 'Plastic Spool', label: 'Plastic Spool' },
  { value: 'Pirn (for Shuttle Filling)', label: 'Pirn (for Shuttle Filling)' },
]

const BOB_TRAVERSE_LENGTH_OPTIONS = [
  { value: 'TRAVERSE_4_INCH', label: 'Traverse 4 Inch' },
  { value: 'TRAVERSE_6_INCH', label: 'Traverse 6 Inch' },
  { value: 'TRAVERSE_8_INCH_JUMBO', label: 'Traverse 8 Inch Jumbo' },
]

const KNOT_METHOD_OPTIONS = [
  { value: 'STANDARD_WEAVERS_KNOT', label: 'Standard Weaver\'s Knot' },
  { value: 'AUTOMATED_AIR_SPLICED_JOIN', label: 'Automated Air Spliced Join' },
  { value: 'MICRO_MECHANICAL_KNOT', label: 'Micro Mechanical Knot' },
  { value: 'FISHERMANS_KNOT', label: 'Fisherman\'s Knot' },
  { value: 'ILLEGAL_OVERHAND_KNOT', label: 'Illegal Overhand Knot' },
]

const STRUCTURAL_VERDICT_OPTIONS = [
  { value: 'OPTIMAL_CROSS_WOUND', label: 'Optimal Cross-Wound' },
  { value: 'SOFT_BUILD_COLLAPSE', label: 'Soft Build Collapse' },
  { value: 'HARD_BUILD_STRETCHED', label: 'Hard Build Stretched' },
  { value: 'RIDGED_SHOULDER_TRAP', label: 'Ridged Shoulder Trap' },
]

const TARGET_MACHINE_OPTIONS = [
  { value: '1536_HOOK_JACQUARD', label: '1536 Hook Jacquard (Standard Luxury Brocade)' },
  { value: '2400_HOOK_JACQUARD', label: '2400 Hook Jacquard (High-Density Fine Motif)' },
  { value: 'HANDLOOM', label: 'Handloom' },
  { value: 'POWERLOOM', label: 'Powerloom' },
  { value: 'RAPIER_LOOM', label: 'Rapier Loom' },
]

export default function DashboardBobbinWinder() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [skeinDyeJobs, setSkeinDyeJobs] = useState([])
  const [jobCards, setJobCards] = useState([])
  const [bobbins, setBobbins] = useState([])
  const [certificates, setCertificates] = useState([])
  const [stockRouting, setStockRouting] = useState([])
  const [alarms, setAlarms] = useState([])
  const [forecast, setForecast] = useState(null)

  const [jobCardForm, setJobCardForm] = useState({
    winding_job_card_id: '',
    operator_employee_id: '',
    spindle_machine_id: '',
    input_dyed_lot_no: '',
    yarn_type: 'WARP_ORGANZINE_HIGH_TWIST',
    carrier_destination_type: 'Flanged Bobbin',
    allocated_input_weight_kg: '',
    output_wound_weight_kg: '',
    winding_scrap_waste_gm: '',
    winding_operation_type: 'ROUTINE_PRODUCTION',
    winding_machine_type: 'SEMI_AUTOMATIC_BOBBIN_WINDER',
    worker_attendance_shift_code: 'SHIFT_A_MORNING',
    yarn_processing_profile: 'WARP_ORGANZINE_HIGH_TWIST',
    silk_fiber_variety: 'PURE_MULBERRY_SILK',
    target_output_carrier_type: 'FLANGED_PLASTIC_BOBBIN',
    bobbin_traverse_length_config: 'TRAVERSE_6_INCH',
    knot_mechanical_join_profiling: 'STANDARD_WEAVERS_KNOT',
    bobbin_structural_build_verdict: 'OPTIMAL_CROSS_WOUND',
    bobbin_hardness_shore_d: '',
    splice_count_per_bobbin: 0,
    bobbin_flange_trapping_found: false,
    yarn_break_rate_per_1000m: '',
    inventory_output_routing_allocation: 'BOBBIN_CLEARED_FOR_WARPING',
    winding_speed_mpm: '',
    applied_tension_grams: '',
    target_machine_type: '1536_HOOK_JACQUARD',
    joint_method_used: 'STANDARD_WEAVERS_KNOT',
    skein_dye_job_id: '',
    skein_dye_certificate_id: '',
    master_colorist_recipe_id: '',
    master_colorist_certificate_id: '',
    throwster_record_id: '',
    throwster_batch_id: ''
  })

  const [bobbinForm, setBobbinForm] = useState({
    bobbin_id: '',
    carrier_type: 'Flanged Bobbin',
    yarn_type: 'WARP_ORGANZINE_HIGH_TWIST',
    silk_fiber_variety: 'PURE_MULBERRY_SILK',
    input_dyed_lot_no: '',
    net_weight_kg: '',
    traverse_length_config: 'TRAVERSE_6_INCH',
    knot_method: 'STANDARD_WEAVERS_KNOT',
    structural_verdict: 'OPTIMAL_CROSS_WOUND'
  })

  useEffect(() => {
    fetchSkeinDyeJobs()
    fetchJobCards()
    fetchBobbins()
    fetchCertificates()
    fetchStockRouting()
    fetchAlarms()
    fetchSalesForecast()
  }, [])

  const fetchSkeinDyeJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/skein-dye/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setSkeinDyeJobs(data.jobs || [])
    } catch (error) {
      console.error('Failed to fetch skein dye jobs:', error)
    }
  }

  const fetchJobCards = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/winding/job-cards`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setJobCards(data.job_cards || [])
    } catch (error) {
      console.error('Failed to fetch job cards:', error)
    }
  }

  const fetchBobbins = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/winding/bobbins`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setBobbins(data.bobbins || [])
    } catch (error) {
      console.error('Failed to fetch bobbins:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/winding/certificates`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setCertificates(data.certificates || [])
    } catch (error) {
      console.error('Failed to fetch certificates:', error)
    }
  }

  const fetchStockRouting = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/winding/stock-routing`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setStockRouting(data.routing || [])
    } catch (error) {
      console.error('Failed to fetch stock routing:', error)
    }
  }

  const fetchAlarms = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/winding/waste-alarms`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/winding`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setForecast(data)
    } catch (error) {
      console.error('Failed to fetch sales forecast:', error)
    }
  }

  const handleJobCardSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...jobCardForm,
        skein_dye_job_id: jobCardForm.skein_dye_job_id || null,
        skein_dye_certificate_id: jobCardForm.skein_dye_certificate_id || null,
        master_colorist_recipe_id: jobCardForm.master_colorist_recipe_id || null,
        master_colorist_certificate_id: jobCardForm.master_colorist_certificate_id || null,
        throwster_record_id: jobCardForm.throwster_record_id || null,
        throwster_batch_id: jobCardForm.throwster_batch_id || null,
        allocated_input_weight_kg: jobCardForm.allocated_input_weight_kg ? parseFloat(jobCardForm.allocated_input_weight_kg) : null,
        output_wound_weight_kg: jobCardForm.output_wound_weight_kg ? parseFloat(jobCardForm.output_wound_weight_kg) : null,
        winding_scrap_waste_gm: jobCardForm.winding_scrap_waste_gm ? parseFloat(jobCardForm.winding_scrap_waste_gm) : 0,
        winding_speed_mpm: jobCardForm.winding_speed_mpm ? parseInt(jobCardForm.winding_speed_mpm) : null,
        applied_tension_grams: jobCardForm.applied_tension_grams ? parseFloat(jobCardForm.applied_tension_grams) : null,
        splice_count_per_bobbin: jobCardForm.splice_count_per_bobbin || 0,
        bobbin_hardness_shore_d: jobCardForm.bobbin_hardness_shore_d ? parseFloat(jobCardForm.bobbin_hardness_shore_d) : null,
        yarn_break_rate_per_1000m: jobCardForm.yarn_break_rate_per_1000m ? parseFloat(jobCardForm.yarn_break_rate_per_1000m) : null
      }

      const response = await fetch(`${API_URL}/winding/job-cards`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Job card ${data.winding_job_card_id} created`, 'success')
        setValidationResult(data)
        setJobCardForm({
          winding_job_card_id: '', operator_employee_id: '', spindle_machine_id: '',
          input_dyed_lot_no: '', yarn_type: 'WARP_ORGANZINE_HIGH_TWIST',
          carrier_destination_type: 'Flanged Bobbin',
          allocated_input_weight_kg: '', output_wound_weight_kg: '', winding_scrap_waste_gm: '',
          winding_operation_type: 'ROUTINE_PRODUCTION', winding_machine_type: 'SEMI_AUTOMATIC_BOBBIN_WINDER',
          worker_attendance_shift_code: 'SHIFT_A_MORNING', yarn_processing_profile: 'WARP_ORGANZINE_HIGH_TWIST',
          silk_fiber_variety: 'PURE_MULBERRY_SILK', target_output_carrier_type: 'FLANGED_PLASTIC_BOBBIN',
          bobbin_traverse_length_config: 'TRAVERSE_6_INCH', knot_mechanical_join_profiling: 'STANDARD_WEAVERS_KNOT',
          bobbin_structural_build_verdict: 'OPTIMAL_CROSS_WOUND', bobbin_hardness_shore_d: '',
          splice_count_per_bobbin: 0, bobbin_flange_trapping_found: false, yarn_break_rate_per_1000m: '',
          inventory_output_routing_allocation: 'BOBBIN_CLEARED_FOR_WARPING', winding_speed_mpm: '',
          applied_tension_grams: '', target_machine_type: '1536_HOOK_JACQUARD',
          joint_method_used: 'STANDARD_WEAVERS_KNOT', skein_dye_job_id: '', skein_dye_certificate_id: '',
          master_colorist_recipe_id: '', master_colorist_certificate_id: '', throwster_record_id: '', throwster_batch_id: ''
        })
        fetchJobCards()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult(data)
      }
    } catch (error) {
      addNotification('Failed to create job card', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCreateBobbin = async () => {
    if (!jobCardForm.winding_job_card_id) {
      addNotification('Please create a job card first', 'error')
      return
    }
    setSubmitting(true)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...bobbinForm,
        job_card_id: jobCardForm.winding_job_card_id,
        net_weight_kg: bobbinForm.net_weight_kg ? parseFloat(bobbinForm.net_weight_kg) : 0
      }

      const response = await fetch(`${API_URL}/winding/bobbins`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Bobbin ${data.bobbin_id} created`, 'success')
        setBobbinForm({
          bobbin_id: '', carrier_type: 'Flanged Bobbin', yarn_type: 'WARP_ORGANZINE_HIGH_TWIST',
          silk_fiber_variety: 'PURE_MULBERRY_SILK', input_dyed_lot_no: '', net_weight_kg: '',
          traverse_length_config: 'TRAVERSE_6_INCH', knot_method: 'STANDARD_WEAVERS_KNOT',
          structural_verdict: 'OPTIMAL_CROSS_WOUND'
        })
        fetchBobbins()
      } else {
        addNotification(data.error || 'Failed to create bobbin', 'error')
      }
    } catch (error) {
      addNotification('Failed to create bobbin', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleApprove = async (jobCardId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/winding/job-cards/${jobCardId}/approve`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Job card certified: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchJobCards()
        fetchBobbins()
        fetchStockRouting()
      } else {
        addNotification(data.error || 'Approval failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to approve job card', 'error')
    }
  }

  const handleReject = async (jobCardId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/winding/job-cards/${jobCardId}/reject`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason: 'Rejected by Bobbin Winder' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Job card rejected', 'warning')
        fetchJobCards()
      } else {
        addNotification(data.error || 'Rejection failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to reject job card', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'CERTIFIED': return 'success'
      case 'REJECTED': return 'error'
      case 'ACTIVE': return 'warning'
      case 'COMPLETED': return 'info'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'BOBBIN_CLEARED_FOR_WARPING': return 'success'
      case 'BOBBIN_CLEARED_FOR_PIRN_WEFT': return 'warning'
      case 'WINDING_REJECT_RE_RUN': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Bobbin Winder — Yarn Winding & Bobbin Build
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
          {WINDING_TABS.map((t) => (
            <Tab key={t.id} label={t.label} value={t.id} />
          ))}
        </Tabs>
      </Paper>

      {validationResult && validationResult.errors && validationResult.errors.length > 0 && (
        <Alert severity="error" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Guardrail Violations</Typography>
          {validationResult.errors.map((err, idx) => (
            <Typography key={idx} variant="body2">• [{err.code}] {err.message}</Typography>
          ))}
        </Alert>
      )}

      {validationResult && validationResult.warnings && validationResult.warnings.length > 0 && (
        <Alert severity="warning" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Warnings</Typography>
          {validationResult.warnings.map((w, idx) => (
            <Typography key={idx} variant="body2">• [{w.code}] {w.message}</Typography>
          ))}
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Job Card:</strong> {certificateDetail.winding_job_card_id}</Typography>
          <Typography variant="body2"><strong>Certificate Hash:</strong> {certificateDetail.certificate_hash}</Typography>
        </Alert>
      )}

      {/* ===================== PRE-PROCESS TAB ===================== */}
      {tab === 'preprocess' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Skein Dye Jobs (Pre-Process)</Typography>
              {skeinDyeJobs.length === 0 ? (
                <Typography color="text.secondary">No skein dye jobs found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job ID</TableCell>
                        <TableCell>Machine</TableCell>
                        <TableCell>Vessel Type</TableCell>
                        <TableCell>Input Weight (kg)</TableCell>
                        <TableCell>Output Weight (kg)</TableCell>
                        <TableCell>Routing</TableCell>
                        <TableCell>Status</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {skeinDyeJobs.map((job) => (
                        <TableRow key={job.id}>
                          <TableCell>{job.job_id}</TableCell>
                          <TableCell>{job.allocated_machine_id}</TableCell>
                          <TableCell>{job.vessel_type_allocated}</TableCell>
                          <TableCell>{job.input_skein_dry_weight_kg}</TableCell>
                          <TableCell>{job.output_skein_dry_weight_kg}</TableCell>
                          <TableCell><Chip label={job.auto_assigned_routing} color={getRoutingColor(job.auto_assigned_routing)} size="small" /></TableCell>
                          <TableCell><Chip label={job.status} color={getStatusColor(job.status)} size="small" /></TableCell>
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
                    <InputLabel>Select Skein Dye Job</InputLabel>
                    <Select value={jobCardForm.skein_dye_job_id} label="Select Skein Dye Job"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, skein_dye_job_id: e.target.value })}>
                      <MenuItem value="">Select job</MenuItem>
                      {skeinDyeJobs.map((job) => (
                        <MenuItem key={job.id} value={job.id}>{job.job_id} — {job.allocated_machine_id}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField fullWidth label="Job Card ID" value={jobCardForm.winding_job_card_id}
                    onChange={(e) => setJobCardForm({ ...jobCardForm, winding_job_card_id: e.target.value })}
                    placeholder="Auto-generated or scan" />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>2. Operations & Personnel Metadata</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Operator Employee ID" value={jobCardForm.operator_employee_id}
                    onChange={(e) => setJobCardForm({ ...jobCardForm, operator_employee_id: e.target.value })}
                    placeholder="Scan or enter employee ID" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Spindle Machine ID" value={jobCardForm.spindle_machine_id}
                    onChange={(e) => setJobCardForm({ ...jobCardForm, spindle_machine_id: e.target.value })}
                    placeholder="Scan or enter machine ID" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Winding Operation Type</InputLabel>
                    <Select value={jobCardForm.winding_operation_type} label="Winding Operation Type"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, winding_operation_type: e.target.value })}>
                      {WINDING_OPERATION_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Winding Machine Type</InputLabel>
                    <Select value={jobCardForm.winding_machine_type} label="Winding Machine Type"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, winding_machine_type: e.target.value })}>
                      {WINDING_MACHINE_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Worker Attendance Shift</InputLabel>
                    <Select value={jobCardForm.worker_attendance_shift_code} label="Worker Attendance Shift"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, worker_attendance_shift_code: e.target.value })}>
                      {WORKER_SHIFT_OPTIONS.map((opt) => (
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
              <Typography variant="h6" gutterBottom>3. Textile Material & Structural Characteristics</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Yarn Processing Profile</InputLabel>
                    <Select value={jobCardForm.yarn_processing_profile} label="Yarn Processing Profile"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, yarn_processing_profile: e.target.value, yarn_type: e.target.value })}>
                      {YARN_PROCESSING_PROFILE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Silk Fiber Variety</InputLabel>
                    <Select value={jobCardForm.silk_fiber_variety} label="Silk Fiber Variety"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, silk_fiber_variety: e.target.value })}>
                      {SILK_FIBER_VARIETY_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Input Dyed Lot No</InputLabel>
                    <Select value={jobCardForm.input_dyed_lot_no} label="Input Dyed Lot No"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, input_dyed_lot_no: e.target.value })}>
                      <MenuItem value="">Select lot</MenuItem>
                      {skeinDyeJobs.map((job) => (
                        <MenuItem key={job.id} value={job.job_id}>{job.job_id} — {job.allocated_machine_id}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>4. Carrier Destination & Mechanical Dimensions</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Target Output Carrier Type</InputLabel>
                    <Select value={jobCardForm.target_output_carrier_type} label="Target Output Carrier Type"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, target_output_carrier_type: e.target.value, carrier_destination_type: e.target.value })}>
                      {TARGET_CARRIER_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Bobbin Traverse Length</InputLabel>
                    <Select value={jobCardForm.bobbin_traverse_length_config} label="Bobbin Traverse Length"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, bobbin_traverse_length_config: e.target.value })}>
                      {BOB_TRAVERSE_LENGTH_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Target Machine Type</InputLabel>
                    <Select value={jobCardForm.target_machine_type} label="Target Machine Type"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, target_machine_type: e.target.value })}>
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
              <Typography variant="h6" gutterBottom>5. Winding Setup & Machine Controls</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Winding Speed (m/min)" type="number"
                    value={jobCardForm.winding_speed_mpm}
                    onChange={(e) => setJobCardForm({ ...jobCardForm, winding_speed_mpm: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="Max 200 m/min for 2400-hook" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Applied Tension (grams)" type="number"
                    value={jobCardForm.applied_tension_grams}
                    onChange={(e) => setJobCardForm({ ...jobCardForm, applied_tension_grams: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} helperText="4-6g for 2400-hook, 8-12g for 1536-hook" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Joint Method Used</InputLabel>
                    <Select value={jobCardForm.joint_method_used} label="Joint Method Used"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, joint_method_used: e.target.value })}>
                      {KNOT_METHOD_OPTIONS.map((opt) => (
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
              <Typography variant="h6" gutterBottom>6. Material Balance & Scrap</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Allocated Input Weight (kg)" type="number"
                    value={jobCardForm.allocated_input_weight_kg}
                    onChange={(e) => setJobCardForm({ ...jobCardForm, allocated_input_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Output Wound Weight (kg)" type="number"
                    value={jobCardForm.output_wound_weight_kg}
                    onChange={(e) => setJobCardForm({ ...jobCardForm, output_wound_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Winding Scrap Waste (gm)" type="number"
                    value={jobCardForm.winding_scrap_waste_gm}
                    onChange={(e) => setJobCardForm({ ...jobCardForm, winding_scrap_waste_gm: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} required />
                </Grid>
              </Grid>
              <Box sx={{ mt: 3 }}>
                <Button variant="contained" onClick={handleJobCardSubmit} disabled={submitting}>
                  {submitting ? 'Submitting...' : 'Save Job Card'}
                </Button>
              </Box>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Winding Job Cards</Typography>
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
                      <TableCell>Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {jobCards.map((card) => (
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
                        <TableCell>
                          {card.status === 'ACTIVE' && (
                            <>
                              <Button size="small" variant="outlined" color="success" onClick={() => handleApprove(card.id)}>Certify</Button>
                              <Button size="small" variant="outlined" color="error" onClick={() => handleReject(card.id)} sx={{ ml: 1 }}>Reject</Button>
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

      {/* ===================== POST-WINDING QUALITY AUDIT TAB ===================== */}
      {tab === 'quality-audit' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>1. Real-Time Quality & Defect Checks</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Knot Mechanical Join Profiling</InputLabel>
                    <Select value={jobCardForm.knot_mechanical_join_profiling} label="Knot Mechanical Join Profiling"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, knot_mechanical_join_profiling: e.target.value, joint_method_used: e.target.value })}>
                      {KNOT_METHOD_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Bobbin Structural Build Verdict</InputLabel>
                    <Select value={jobCardForm.bobbin_structural_build_verdict} label="Bobbin Structural Build Verdict"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, bobbin_structural_build_verdict: e.target.value })}>
                      {STRUCTURAL_VERDICT_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Bobbin Hardness Shore D" type="number"
                    value={jobCardForm.bobbin_hardness_shore_d}
                    onChange={(e) => setJobCardForm({ ...jobCardForm, bobbin_hardness_shore_d: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} helperText="Target: 55-65" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Splice Count per Bobbin" type="number"
                    value={jobCardForm.splice_count_per_bobbin}
                    onChange={(e) => setJobCardForm({ ...jobCardForm, splice_count_per_bobbin: parseInt(e.target.value) || 0 })}
                    inputProps={{ step: '1', min: 0 }} helperText="Max 1 for 2400-hook" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Yarn Break Rate per 1000m" type="number"
                    value={jobCardForm.yarn_break_rate_per_1000m}
                    onChange={(e) => setJobCardForm({ ...jobCardForm, yarn_break_rate_per_1000m: e.target.value })}
                    inputProps={{ step: '0.01', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Bobbin Flange Trapping Found</InputLabel>
                    <Select value={jobCardForm.bobbin_flange_trapping_found ? 'Yes' : 'No'} label="Bobbin Flange Trapping Found"
                      onChange={(e) => setJobCardForm({ ...jobCardForm, bobbin_flange_trapping_found: e.target.value === 'Yes' })}>
                      <MenuItem value="No">No</MenuItem>
                      <MenuItem value="Yes">Yes</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
              <Alert severity="info" sx={{ mt: 3 }}>
                <Typography variant="subtitle2">Automated ERP Routing & Validation Rules</Typography>
                <Typography variant="body2">
                  • 2400 Hook + Standard Weaver's Knot → REJECT_FOR_2400_WARP (KNOTS_WILL_JAM_FINE_REED)<br/>
                  • 2400 Hook + Winding Speed > 200 m/min → HIGH_WINDING_SPEED_FRICTION_RISK<br/>
                  • 2400 Hook + Splice Count > 1 → DOWNGRADE_TO_1536_HOOK_OR_WEFT<br/>
                  • 2400 Hook + Tension > 6g → TENSION_TOO_HIGH_FOR_2400<br/>
                  • Waste Variance > 0.5% → HIGH_WASTE_VARIANCE
                </Typography>
              </Alert>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>2. Create Bobbin Record</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Bobbin ID" value={bobbinForm.bobbin_id}
                    onChange={(e) => setBobbinForm({ ...bobbinForm, bobbin_id: e.target.value })}
                    placeholder="Auto-generated or scan" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Carrier Type</InputLabel>
                    <Select value={bobbinForm.carrier_type} label="Carrier Type"
                      onChange={(e) => setBobbinForm({ ...bobbinForm, carrier_type: e.target.value })}>
                      {CARRIER_DESTINATION_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Yarn Type</InputLabel>
                    <Select value={bobbinForm.yarn_type} label="Yarn Type"
                      onChange={(e) => setBobbinForm({ ...bobbinForm, yarn_type: e.target.value })}>
                      {YARN_PROCESSING_PROFILE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Net Weight (kg)" type="number"
                    value={bobbinForm.net_weight_kg}
                    onChange={(e) => setBobbinForm({ ...bobbinForm, net_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Structural Verdict</InputLabel>
                    <Select value={bobbinForm.structural_verdict} label="Structural Verdict"
                      onChange={(e) => setBobbinForm({ ...bobbinForm, structural_verdict: e.target.value })}>
                      {STRUCTURAL_VERDICT_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Knot Method</InputLabel>
                    <Select value={bobbinForm.knot_method} label="Knot Method"
                      onChange={(e) => setBobbinForm({ ...bobbinForm, knot_method: e.target.value })}>
                      {KNOT_METHOD_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleCreateBobbin} disabled={submitting}>
                    {submitting ? 'Creating...' : 'Create Bobbin Record'}
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
              <Typography variant="h6" gutterBottom>Winding Certificates — Post-Process Output</Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates issued yet</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag</TableCell>
                        <TableCell>Job Card</TableCell>
                        <TableCell>Bobbin</TableCell>
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
                          <TableCell>{cert.winding_job_card_id}</TableCell>
                          <TableCell>{cert.bobbin_id}</TableCell>
                          <TableCell><Chip label={cert.inventory_output_routing_allocation} color={getRoutingColor(cert.inventory_output_routing_allocation)} size="small" /></TableCell>
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
              <Typography variant="h6" gutterBottom>Sales Forecast — Bobbin Winding Material Processing Plan</Typography>
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
                          <TableCell>Carrier Type</TableCell>
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
                            <TableCell>{item.carrier_type}</TableCell>
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
                          <TableCell>Yarn Type</TableCell>
                          <TableCell>Est. Sarees</TableCell>
                          <TableCell>Est. Silk (kg)</TableCell>
                          <TableCell>Carrier Type</TableCell>
                          <TableCell>Target Machine</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.upcoming_lots.map((lot, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{lot.lot_number}</TableCell>
                            <TableCell>{lot.saree_category}</TableCell>
                            <TableCell>{lot.shade_code}</TableCell>
                            <TableCell>{lot.yarn_type}</TableCell>
                            <TableCell>{lot.estimated_sarees}</TableCell>
                            <TableCell>{lot.estimated_silk_kg}</TableCell>
                            <TableCell>{lot.carrier_type}</TableCell>
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
