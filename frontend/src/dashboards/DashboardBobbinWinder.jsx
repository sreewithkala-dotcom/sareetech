import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Stepper, Step, StepLabel, StepContent } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const WINDING_STEPS = ['Pre-Process Setup', 'Winding Operation', 'Post-Process Audit & Certificate']

export default function DashboardBobbinWinder() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [activeStep, setActiveStep] = useState(0)
  const [submitting, setSubmitting] = useState(false)
  const [jobCards, setJobCards] = useState([])
  const [bobbins, setBobbins] = useState([])
  const [alarms, setAlarms] = useState([])
  const [stockRouting, setStockRouting] = useState([])
  const [selectedJobCard, setSelectedJobCard] = useState(null)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [preForm, setPreForm] = useState({
    spindle_machine_id: '',
    input_dyed_lot_no: '',
    yarn_type: 'WARP_ORGANZINE_HIGH_TWIST',
    carrier_destination_type: 'Flanged Bobbin',
    winding_operation_type: 'ROUTINE_PRODUCTION',
    winding_machine_type: 'SEMI_AUTOMATIC_BOBBIN_WINDER',
    worker_attendance_shift_code: 'SHIFT_A_MORNING',
    yarn_processing_profile: 'WARP_ORGANZINE_HIGH_TWIST',
    silk_fiber_variety: 'PURE_MULBERRY_SILK',
    target_output_carrier_type: 'FLANGED_PLASTIC_BOBBIN',
    bobbin_traverse_length_config: 'TRAVERSE_6_INCH'
  })

  const [processForm, setProcessForm] = useState({
    allocated_input_weight_kg: '',
    output_wound_weight_kg: '',
    winding_scrap_waste_gm: '',
    winding_speed_mpm: '',
    applied_tension_grams: '',
    joint_method_used: 'Standard Weaver\'s Knot',
    knot_mechanical_join_profiling: 'STANDARD_WEAVERS_KNOT',
    bobbin_structural_build_verdict: 'OPTIMAL_CROSS_WOUND',
    bobbin_hardness_shore_d: '',
    splice_count_per_bobbin: 0,
    bobbin_flange_trapping_found: false,
    yarn_break_rate_per_1000m: ''
  })

  const [postForm, setPostForm] = useState({
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
    fetchJobCards()
    fetchBobbins()
    fetchAlarms()
    fetchStockRouting()
  }, [])

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

  const handlePreNext = () => {
    setActiveStep(1)
  }

  const handleProcessSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...preForm,
        ...processForm,
        allocated_input_weight_kg: parseFloat(processForm.allocated_input_weight_kg) || 0,
        output_wound_weight_kg: parseFloat(processForm.output_wound_weight_kg) || 0,
        winding_scrap_waste_gm: parseFloat(processForm.winding_scrap_waste_gm) || 0,
        winding_speed_mpm: processForm.winding_speed_mpm ? parseInt(processForm.winding_speed_mpm) : null,
        applied_tension_grams: processForm.applied_tension_grams ? parseFloat(processForm.applied_tension_grams) : null,
        splice_count_per_bobbin: processForm.splice_count_per_bobbin || 0,
        bobbin_hardness_shore_d: processForm.bobbin_hardness_shore_d ? parseFloat(processForm.bobbin_hardness_shore_d) : null,
        yarn_break_rate_per_1000m: processForm.yarn_break_rate_per_1000m ? parseFloat(processForm.yarn_break_rate_per_1000m) : null
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
        setActiveStep(2)
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
    if (!selectedJobCard) {
      addNotification('Select a job card first', 'error')
      return
    }
    setSubmitting(true)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/winding/bobbins`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          ...postForm,
          job_card_id: selectedJobCard.id,
          net_weight_kg: parseFloat(postForm.net_weight_kg) || 0
        })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Bobbin ${data.bobbin_id} created`, 'success')
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

  const handleApproveJobCard = async (jobCardId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/winding/job-cards/${jobCardId}/approve`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Job card certified: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchJobCards()
        fetchStockRouting()
      } else {
        addNotification(data.error || 'Approval failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to approve job card', 'error')
    }
  }

  const handleRejectJobCard = async (jobCardId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/winding/job-cards/${jobCardId}/reject`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ reason: 'Rejected by supervisor' })
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
            Bobbin Winder — Floor Operations
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
      </Box>

      {validationResult && validationResult.errors && validationResult.errors.length > 0 && (
        <Alert severity="error" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            Guardrail Violations
          </Typography>
          {validationResult.errors.map((err, idx) => (
            <Typography key={idx} variant="body2">
              • [{err.code}] {err.message}
            </Typography>
          ))}
        </Alert>
      )}

      {validationResult && validationResult.warnings && validationResult.warnings.length > 0 && (
        <Alert severity="warning" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            Warnings
          </Typography>
          {validationResult.warnings.map((w, idx) => (
            <Typography key={idx} variant="body2">
              • [{w.code}] {w.message}
            </Typography>
          ))}
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            Certificate Generated
          </Typography>
          <Typography variant="body2">
            <strong>Job Card:</strong> {certificateDetail.winding_job_card_id}
          </Typography>
          <Typography variant="body2">
            <strong>Certificate Hash:</strong> {certificateDetail.certificate_hash}
          </Typography>
        </Alert>
      )}

      <Stepper activeStep={activeStep} sx={{ mb: 3 }}>
        {WINDING_STEPS.map((label) => (
          <Step key={label}>
            <StepLabel>{label}</StepLabel>
          </Step>
        ))}
      </Stepper>

      <Grid container spacing={3}>
        {/* Step 1: Pre-Process Setup */}
        {activeStep === 0 && (
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Pre-Process Setup & Machine Configuration
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Spindle Machine ID"
                    value={preForm.spindle_machine_id}
                    onChange={(e) => setPreForm({ ...preForm, spindle_machine_id: e.target.value })}
                    placeholder="Scan or enter machine ID"
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Input Dyed Lot No"
                    value={preForm.input_dyed_lot_no}
                    onChange={(e) => setPreForm({ ...preForm, input_dyed_lot_no: e.target.value })}
                    placeholder="QA-cleared dye lot code"
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Yarn Type</InputLabel>
                    <Select
                      value={preForm.yarn_type}
                      label="Yarn Type"
                      onChange={(e) => setPreForm({ ...preForm, yarn_type: e.target.value })}
                    >
                      <MenuItem value="WARP_ORGANZINE_HIGH_TWIST">Warp Yarn (High Twist / Organzine)</MenuItem>
                      <MenuItem value="WEFT_TRAM_LOW_TWIST">Weft Yarn (Low Twist / Tram)</MenuItem>
                      <MenuItem value="CREPE_ULTRA_TWIST">Crepe Ultra Twist</MenuItem>
                      <MenuItem value="DUPION_SLUB_YARN">Dupion Slub Yarn</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Carrier Destination Type</InputLabel>
                    <Select
                      value={preForm.carrier_destination_type}
                      label="Carrier Destination Type"
                      onChange={(e) => setPreForm({ ...preForm, carrier_destination_type: e.target.value })}
                    >
                      <MenuItem value="Flanged Bobbin">Flanged Bobbin</MenuItem>
                      <MenuItem value="Paper Cone">Paper Cone</MenuItem>
                      <MenuItem value="Plastic Spool">Plastic Spool</MenuItem>
                      <MenuItem value="Pirn (for Shuttle Filling)">Pirn (for Shuttle Filling)</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Winding Operation Type</InputLabel>
                    <Select
                      value={preForm.winding_operation_type}
                      label="Winding Operation Type"
                      onChange={(e) => setPreForm({ ...preForm, winding_operation_type: e.target.value })}
                    >
                      <MenuItem value="ROUTINE_PRODUCTION">Routine Production</MenuItem>
                      <MenuItem value="SAMPLE_CONING">Sample Coning</MenuItem>
                      <MenuItem value="RE_WINDING_CORRECTION">Re-Winding Correction</MenuItem>
                      <MenuItem value="CLEANING_RUN">Cleaning Run</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Winding Machine Type</InputLabel>
                    <Select
                      value={preForm.winding_machine_type}
                      label="Winding Machine Type"
                      onChange={(e) => setPreForm({ ...preForm, winding_machine_type: e.target.value })}
                    >
                      <MenuItem value="HIGH_SPEED_AUTOMATIC_CONER">High Speed Automatic Coner</MenuItem>
                      <MenuItem value="SEMI_AUTOMATIC_BOBBIN_WINDER">Semi-Automatic Bobbin Winder</MenuItem>
                      <MenuItem value="TRADITIONAL_HAND_WINDER">Traditional Hand Winder</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Worker Attendance Shift</InputLabel>
                    <Select
                      value={preForm.worker_attendance_shift_code}
                      label="Worker Attendance Shift"
                      onChange={(e) => setPreForm({ ...preForm, worker_attendance_shift_code: e.target.value })}
                    >
                      <MenuItem value="SHIFT_A_MORNING">Shift A Morning (06:00-14:00)</MenuItem>
                      <MenuItem value="SHIFT_B_EVENING">Shift B Evening (14:00-22:00)</MenuItem>
                      <MenuItem value="SHIFT_C_NIGHT">Shift C Night (22:00-06:00)</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Silk Fiber Variety</InputLabel>
                    <Select
                      value={preForm.silk_fiber_variety}
                      label="Silk Fiber Variety"
                      onChange={(e) => setPreForm({ ...preForm, silk_fiber_variety: e.target.value })}
                    >
                      <MenuItem value="PURE_MULBERRY_SILK">Pure Mulberry Silk</MenuItem>
                      <MenuItem value="ORGANIC_TUSSAR_WILD">Organic Tussar Wild</MenuItem>
                      <MenuItem value="MATTE_ERI_SPUN">Matte Eri Spun</MenuItem>
                      <MenuItem value="SHIMMERING_MUGA">Shimmering Muga</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Target Output Carrier Type</InputLabel>
                    <Select
                      value={preForm.target_output_carrier_type}
                      label="Target Output Carrier Type"
                      onChange={(e) => setPreForm({ ...preForm, target_output_carrier_type: e.target.value })}
                    >
                      <MenuItem value="FLANGED_PLASTIC_BOBBIN">Flanged Plastic Bobbin</MenuItem>
                      <MenuItem value="TAPERED_PAPER_CONE">Tapered Paper Cone</MenuItem>
                      <MenuItem value="CYLINDRICAL_PLASTIC_CHEESE">Cylindrical Plastic Cheese</MenuItem>
                      <MenuItem value="WOODEN_HANK_SWIFT_SPOOL">Wooden Hank Swift Spool</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Bobbin Traverse Length</InputLabel>
                    <Select
                      value={preForm.bobbin_traverse_length_config}
                      label="Bobbin Traverse Length"
                      onChange={(e) => setPreForm({ ...preForm, bobbin_traverse_length_config: e.target.value })}
                    >
                      <MenuItem value="TRAVERSE_4_INCH">Traverse 4 Inch</MenuItem>
                      <MenuItem value="TRAVERSE_6_INCH">Traverse 6 Inch</MenuItem>
                      <MenuItem value="TRAVERSE_8_INCH_JUMBO">Traverse 8 Inch Jumbo</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
              <Box sx={{ mt: 3 }}>
                <Button variant="contained" onClick={handlePreNext}>
                  Next: Winding Operation
                </Button>
              </Box>
            </Paper>
          </Grid>
        )}

        {/* Step 2: Winding Operation */}
        {activeStep === 1 && (
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Winding Operation & Material Balance
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Allocated Input Weight (kg)"
                    type="number"
                    value={processForm.allocated_input_weight_kg}
                    onChange={(e) => setProcessForm({ ...processForm, allocated_input_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }}
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Output Wound Weight (kg)"
                    type="number"
                    value={processForm.output_wound_weight_kg}
                    onChange={(e) => setProcessForm({ ...processForm, output_wound_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }}
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Winding Scrap Waste (gm)"
                    type="number"
                    value={processForm.winding_scrap_waste_gm}
                    onChange={(e) => setProcessForm({ ...processForm, winding_scrap_waste_gm: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }}
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Winding Speed (m/min)"
                    type="number"
                    value={processForm.winding_speed_mpm}
                    onChange={(e) => setProcessForm({ ...processForm, winding_speed_mpm: e.target.value })}
                    inputProps={{ min: 0 }}
                    helperText="Max 200 m/min for 2400-hook line"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Applied Tension (grams)"
                    type="number"
                    value={processForm.applied_tension_grams}
                    onChange={(e) => setProcessForm({ ...processForm, applied_tension_grams: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }}
                    helperText="4-6g for 2400-hook, 8-12g for 1536-hook"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Joint Method Used</InputLabel>
                    <Select
                      value={processForm.joint_method_used}
                      label="Joint Method Used"
                      onChange={(e) => setProcessForm({ ...processForm, joint_method_used: e.target.value, knot_mechanical_join_profiling: e.target.value.toUpperCase().replace(/ /g, '_') })}
                    >
                      <MenuItem value="Standard Weaver's Knot">Standard Weaver's Knot</MenuItem>
                      <MenuItem value="Air Splicing">Air Splicing</MenuItem>
                      <MenuItem value="Micro Mechanical Knot">Micro Mechanical Knot</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Bobbin Structural Build Verdict</InputLabel>
                    <Select
                      value={processForm.bobbin_structural_build_verdict}
                      label="Bobbin Structural Build Verdict"
                      onChange={(e) => setProcessForm({ ...processForm, bobbin_structural_build_verdict: e.target.value })}
                    >
                      <MenuItem value="OPTIMAL_CROSS_WOUND">Optimal Cross-Wound</MenuItem>
                      <MenuItem value="SOFT_BUILD_COLLAPSE">Soft Build Collapse</MenuItem>
                      <MenuItem value="HARD_BUILD_STRETCHED">Hard Build Stretched</MenuItem>
                      <MenuItem value="RIDGED_SHOULDER_TRAP">Ridged Shoulder Trap</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Bobbin Hardness Shore D"
                    type="number"
                    value={processForm.bobbin_hardness_shore_d}
                    onChange={(e) => setProcessForm({ ...processForm, bobbin_hardness_shore_d: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }}
                    helperText="Target: 55-65"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Splice Count per Bobbin"
                    type="number"
                    value={processForm.splice_count_per_bobbin}
                    onChange={(e) => setProcessForm({ ...processForm, splice_count_per_bobbin: parseInt(e.target.value) || 0 })}
                    inputProps={{ min: 0 }}
                    helperText="Max 1 for 2400-hook line"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Yarn Break Rate per 1000m"
                    type="number"
                    value={processForm.yarn_break_rate_per_1000m}
                    onChange={(e) => setProcessForm({ ...processForm, yarn_break_rate_per_1000m: e.target.value })}
                    inputProps={{ step: '0.01', min: 0 }}
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Bobbin Flange Trapping Found</InputLabel>
                    <Select
                      value={processForm.bobbin_flange_trapping_found ? 'Yes' : 'No'}
                      label="Bobbin Flange Trapping Found"
                      onChange={(e) => setProcessForm({ ...processForm, bobbin_flange_trapping_found: e.target.value === 'Yes' })}
                    >
                      <MenuItem value="No">No</MenuItem>
                      <MenuItem value="Yes">Yes</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
              <Box sx={{ mt: 3, display: 'flex', gap: 2 }}>
                <Button variant="outlined" onClick={() => setActiveStep(0)}>
                  Back
                </Button>
                <Button variant="contained" onClick={handleProcessSubmit} disabled={submitting}>
                  {submitting ? 'Submitting...' : 'Submit Job Card'}
                </Button>
              </Box>
            </Paper>
          </Grid>
        )}

        {/* Step 3: Post-Process Audit & Certificate */}
        {activeStep === 2 && (
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Post-Process Audit & Stock Routing
              </Typography>

              {selectedJobCard && (
                <Box sx={{ mb: 3 }}>
                  <Typography variant="subtitle1" gutterBottom>
                    Selected Job Card: {selectedJobCard.winding_job_card_id}
                  </Typography>
                  <Grid container spacing={2}>
                    <Grid item xs={12} md={4}>
                      <TextField
                        fullWidth
                        label="Bobbin ID"
                        value={postForm.bobbin_id}
                        onChange={(e) => setPostForm({ ...postForm, bobbin_id: e.target.value })}
                        placeholder="Auto-generated or scan"
                      />
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <FormControl fullWidth>
                        <InputLabel>Carrier Type</InputLabel>
                        <Select
                          value={postForm.carrier_type}
                          label="Carrier Type"
                          onChange={(e) => setPostForm({ ...postForm, carrier_type: e.target.value })}
                        >
                          <MenuItem value="Flanged Bobbin">Flanged Bobbin</MenuItem>
                          <MenuItem value="Paper Cone">Paper Cone</MenuItem>
                          <MenuItem value="Plastic Spool">Plastic Spool</MenuItem>
                          <MenuItem value="Pirn (for Shuttle Filling)">Pirn (for Shuttle Filling)</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <FormControl fullWidth>
                        <InputLabel>Yarn Type</InputLabel>
                        <Select
                          value={postForm.yarn_type}
                          label="Yarn Type"
                          onChange={(e) => setPostForm({ ...postForm, yarn_type: e.target.value })}
                        >
                          <MenuItem value="WARP_ORGANZINE_HIGH_TWIST">Warp Yarn (High Twist / Organzine)</MenuItem>
                          <MenuItem value="WEFT_TRAM_LOW_TWIST">Weft Yarn (Low Twist / Tram)</MenuItem>
                          <MenuItem value="CREPE_ULTRA_TWIST">Crepe Ultra Twist</MenuItem>
                          <MenuItem value="DUPION_SLUB_YARN">Dupion Slub Yarn</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <FormControl fullWidth>
                        <InputLabel>Silk Fiber Variety</InputLabel>
                        <Select
                          value={postForm.silk_fiber_variety}
                          label="Silk Fiber Variety"
                          onChange={(e) => setPostForm({ ...postForm, silk_fiber_variety: e.target.value })}
                        >
                          <MenuItem value="PURE_MULBERRY_SILK">Pure Mulberry Silk</MenuItem>
                          <MenuItem value="ORGANIC_TUSSAR_WILD">Organic Tussar Wild</MenuItem>
                          <MenuItem value="MATTE_ERI_SPUN">Matte Eri Spun</MenuItem>
                          <MenuItem value="SHIMMERING_MUGA">Shimmering Muga</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <TextField
                        fullWidth
                        label="Net Weight (kg)"
                        type="number"
                        value={postForm.net_weight_kg}
                        onChange={(e) => setPostForm({ ...postForm, net_weight_kg: e.target.value })}
                        inputProps={{ step: '0.001', min: 0 }}
                      />
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <FormControl fullWidth>
                        <InputLabel>Structural Verdict</InputLabel>
                        <Select
                          value={postForm.structural_verdict}
                          label="Structural Verdict"
                          onChange={(e) => setPostForm({ ...postForm, structural_verdict: e.target.value })}
                        >
                          <MenuItem value="OPTIMAL_CROSS_WOUND">Optimal Cross-Wound</MenuItem>
                          <MenuItem value="SOFT_BUILD_COLLAPSE">Soft Build Collapse</MenuItem>
                          <MenuItem value="HARD_BUILD_STRETCHED">Hard Build Stretched</MenuItem>
                          <MenuItem value="RIDGED_SHOULDER_TRAP">Ridged Shoulder Trap</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid item xs={12} md={4}>
                      <FormControl fullWidth>
                        <InputLabel>Knot Method</InputLabel>
                        <Select
                          value={postForm.knot_method}
                          label="Knot Method"
                          onChange={(e) => setPostForm({ ...postForm, knot_method: e.target.value })}
                        >
                          <MenuItem value="STANDARD_WEAVERS_KNOT">Standard Weaver's Knot</MenuItem>
                          <MenuItem value="AUTOMATED_AIR_SPLICED_JOIN">Automated Air Spliced Join</MenuItem>
                          <MenuItem value="FISHERMANS_KNOT">Fisherman's Knot</MenuItem>
                          <MenuItem value="ILLEGAL_OVERHAND_KNOT">Illegal Overhand Knot</MenuItem>
                        </Select>
                      </FormControl>
                    </Grid>
                    <Grid item xs={12}>
                      <Button
                        variant="contained"
                        onClick={handleCreateBobbin}
                        disabled={submitting || !selectedJobCard}
                      >
                        {submitting ? 'Creating...' : 'Create Bobbin Record'}
                      </Button>
                    </Grid>
                  </Grid>
                </Box>
              )}

              <Box sx={{ mt: 3 }}>
                <Typography variant="subtitle1" gutterBottom>
                  Certificate & Stock Routing Details
                </Typography>
                {certificateDetail ? (
                  <Alert severity="success">
                    <Typography variant="body2">
                      <strong>Certificate Hash:</strong> {certificateDetail.certificate_hash}
                    </Typography>
                  </Alert>
                ) : (
                  <Typography color="text.secondary">
                    Approve a job card to generate certificate details
                  </Typography>
                )}
              </Box>

              <Box sx={{ mt: 3, display: 'flex', gap: 2 }}>
                <Button variant="outlined" onClick={() => setActiveStep(1)}>
                  Back
                </Button>
              </Box>
            </Paper>
          </Grid>
        )}

        {/* Job Cards Table */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Winding Job Cards
            </Typography>
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
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {jobCards.map((card) => (
                    <TableRow key={card.id} hover selected={selectedJobCard?.id === card.id}>
                      <TableCell>{card.winding_job_card_id}</TableCell>
                      <TableCell>{card.spindle_machine_id}</TableCell>
                      <TableCell>{card.input_dyed_lot_no}</TableCell>
                      <TableCell>{card.yarn_type}</TableCell>
                      <TableCell>{card.carrier_destination_type}</TableCell>
                      <TableCell>{card.allocated_input_weight_kg}</TableCell>
                      <TableCell>{card.output_wound_weight_kg}</TableCell>
                      <TableCell>{card.waste_variance_percent}%</TableCell>
                      <TableCell>
                        <Chip label={card.status} color={getStatusColor(card.status)} size="small" />
                      </TableCell>
                      <TableCell>
                        <Button size="small" variant="outlined" onClick={() => { setSelectedJobCard(card); setActiveStep(2); }}>
                          Select
                        </Button>
                        {card.status === 'ACTIVE' && (
                          <>
                            <Button size="small" variant="outlined" color="success" onClick={() => handleApproveJobCard(card.id)} sx={{ ml: 1 }}>
                              Certify
                            </Button>
                            <Button size="small" variant="outlined" color="error" onClick={() => handleRejectJobCard(card.id)} sx={{ ml: 1 }}>
                              Reject
                            </Button>
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

        {/* Bobbin Records Table */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Bobbin Records
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Bobbin ID</TableCell>
                    <TableCell>Job Card</TableCell>
                    <TableCell>Carrier</TableCell>
                    <TableCell>Yarn</TableCell>
                    <TableCell>Fiber</TableCell>
                    <TableCell>Net Weight</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>QR Tag</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {bobbins.map((bobbin) => (
                    <TableRow key={bobbin.id}>
                      <TableCell>{bobbin.bobbin_id}</TableCell>
                      <TableCell>{bobbin.winding_job_card_id}</TableCell>
                      <TableCell>{bobbin.carrier_type}</TableCell>
                      <TableCell>{bobbin.yarn_type}</TableCell>
                      <TableCell>{bobbin.silk_fiber_variety}</TableCell>
                      <TableCell>{bobbin.net_weight_kg} kg</TableCell>
                      <TableCell>
                        <Chip label={bobbin.status} color={getStatusColor(bobbin.status)} size="small" />
                      </TableCell>
                      <TableCell>{bobbin.qr_tag_id}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>
        </Grid>

        {/* Stock Routing Table */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Stock Routing Recommendations
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Bobbin ID</TableCell>
                    <TableCell>Job Card</TableCell>
                    <TableCell>Carrier</TableCell>
                    <TableCell>Waste %</TableCell>
                    <TableCell>Build Verdict</TableCell>
                    <TableCell>Knot Method</TableCell>
                    <TableCell>Recommended Routing</TableCell>
                    <TableCell>Certificate QR</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {stockRouting.map((route) => (
                    <TableRow key={route.bobbin_id}>
                      <TableCell>{route.bobbin_id}</TableCell>
                      <TableCell>{route.winding_job_card_id}</TableCell>
                      <TableCell>{route.carrier_type}</TableCell>
                      <TableCell>{route.waste_variance_percent}%</TableCell>
                      <TableCell>{route.bobbin_structural_build_verdict}</TableCell>
                      <TableCell>{route.knot_mechanical_join_profiling}</TableCell>
                      <TableCell>
                        <Chip label={route.recommended_routing} color={getRoutingColor(route.recommended_routing)} size="small" />
                      </TableCell>
                      <TableCell>{route.winding_certificate_qr}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>
        </Grid>

        {/* Waste Alarms Table */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Waste Variance Alarms
            </Typography>
            {alarms.length === 0 ? (
              <Typography color="text.secondary">No waste alarms</Typography>
            ) : (
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Job Card</TableCell>
                      <TableCell>Waste %</TableCell>
                      <TableCell>Threshold</TableCell>
                      <TableCell>Severity</TableCell>
                      <TableCell>Message</TableCell>
                      <TableCell>Created</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {alarms.map((alarm) => (
                      <TableRow key={alarm.id}>
                        <TableCell>{alarm.winding_job_card_id}</TableCell>
                        <TableCell>{alarm.waste_variance_percent}%</TableCell>
                        <TableCell>{alarm.threshold_percent}%</TableCell>
                        <TableCell>
                          <Chip label={alarm.severity} color={alarm.severity === 'CRITICAL' ? 'error' : 'warning'} size="small" />
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
    </Container>
  )
}
