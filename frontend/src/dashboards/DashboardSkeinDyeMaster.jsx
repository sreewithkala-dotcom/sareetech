import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider, Checkbox, FormControlLabel } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'
import EnterprisePanel from '../components/EnterprisePanel'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const SKEIN_DYE_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Colorist' },
  { id: 'job-creation', label: 'Job Creation & Machine Setup' },
  { id: 'quality-audit', label: 'Post-Dye Quality Audit' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const VESSEL_TYPE_OPTIONS = [
  { value: 'AUTOMATED_SKEIN_CABINET_CLOSED', label: 'Automated Skein Cabinet (Closed)' },
  { value: 'HANK_DYE_MOVING_ARM_OPEN', label: 'Hank Dye Moving Arm (Open)' },
  { value: 'WINCH_DYEING_BECK', label: 'Winch Dyeing Beck' },
  { value: 'TRADITIONAL_WOODEN_VAT_MANUAL', label: 'Traditional Wooden Vat (Manual)' },
  { value: 'STAINLESS_STEEL_GAS_KETTLE', label: 'Stainless Steel Gas Kettle' },
]

const LEASE_TIE_TYPE_OPTIONS = [
  { value: 'STANDARD_COTTON_STRING', label: 'Standard Cotton String (4 ties)' },
  { value: 'FIGURE_8_POLY_SOFT_TIE', label: 'Figure-8 Poly-Soft Tie (6 ties)' },
  { value: 'ELASTICIZED_SOFT_BAND', label: 'Elasticized Soft Band' },
]

const MACHINE_TYPE_OPTIONS = [
  { value: 'MANUAL_OPEN_VAT', label: 'Manual Open Vat' },
  { value: 'ARM_TYPE_CABINET_HANK_MACHINE', label: 'Arm-Type Cabinet Hank Machine' },
  { value: 'PULSATOR_CABINET_MACHINE', label: 'Pulsator Cabinet Machine' },
]

const LIQUOR_RATIO_OPTIONS = [
  { value: '1_15_ULTRA_LOW_RATIO', label: '1:15 (Ultra Low Ratio)' },
  { value: '1_20_LOW_RATIO', label: '1:20 (Low Ratio)' },
  { value: '1_30_MEDIUM_RATIO', label: '1:30 (Medium Ratio)' },
  { value: '1_40_HIGH_RATIO', label: '1:40 (High Ratio)' },
  { value: '1_50_MAX_DILUTION', label: '1:50 (Max Dilution)' },
]

const WATER_TREATMENT_OPTIONS = [
  { value: 'RO_FILTERED_PURE', label: 'RO Filtered Pure' },
  { value: 'SOFTENED_ION_EXCHANGE', label: 'Softened Ion Exchange' },
  { value: 'RAW_BOREWELL_UNTREATED', label: 'Raw Borewell Untreated' },
  { value: 'MUNICIPAL_CHLORINATED', label: 'Municipal Chlorinated' },
]

const PEAK_TEMP_BRACKET_OPTIONS = [
  { value: '70C_75C_LOW_TEMP', label: '70-75°C (Low Temp)' },
  { value: '80C_85C_STANDARD', label: '80-85°C (Standard)' },
  { value: '86C_90C_HIGH_BOIL', label: '86-90°C (High Boil)' },
  { value: '91C_95C_CRITICAL', label: '91-95°C (Critical)' },
  { value: '96C_100C_OVER_BOIL', label: '96-100°C (Over Boil)' },
]

const SOFTENING_TYPE_OPTIONS = [
  { value: 'CATIONIC_FATTY_AMIDE_EMULSION', label: 'Cationic Fatty Amide Emulsion' },
  { value: 'AMINO_FUNCTIONAL_SILICONE', label: 'Amino Functional Silicone' },
  { value: 'HYDROPHILIC_SILICONE_SOFTENER', label: 'Hydrophilic Silicone Softener' },
  { value: 'NATURAL_COCONUT_OIL_STARCH_BLEND', label: 'Natural Coconut Oil Starch Blend' },
  { value: 'POLYURETHANE_ELASTOMERIC_FINISH', label: 'Polyurethane Elastomeric Finish' },
  { value: 'NONE', label: 'None' },
]

const SHADE_MATCH_OPTIONS = [
  { value: 'PASS_100_PENETRATION', label: 'Pass (100% Penetration)' },
  { value: 'FAIL_LIGHTER_CORE_SHADE', label: 'Fail (Lighter Core Shade)' },
]

const ENTANGLEMENT_RATING_OPTIONS = [
  { value: 'GRADE_5_FREE_FLOWING', label: 'Grade 5 (Free Flowing)' },
  { value: 'GRADE_3_SLIGHT_TANGLING', label: 'Grade 3 (Slight Tangling)' },
  { value: 'GRADE_1_SEVERELY_MATTED', label: 'Grade 1 (Severely Matted)' },
]

const TARGET_MACHINE_OPTIONS = [
  { value: '1536_HOOK_JACQUARD', label: '1536 Hook Jacquard (Standard Luxury Brocade)' },
  { value: '2400_HOOK_JACQUARD', label: '2400 Hook Jacquard (High-Density Fine Motif)' },
  { value: 'HANDLOOM', label: 'Handloom' },
  { value: 'POWERLOOM', label: 'Powerloom' },
  { value: 'RAPIER_LOOM', label: 'Rapier Loom' },
]

export default function DashboardSkeinDyeMaster() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [coloristRecipes, setColoristRecipes] = useState([])
  const [jobs, setJobs] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [jobForm, setJobForm] = useState({
    job_id: '',
    master_colorist_recipe_id: '',
    master_colorist_certificate_id: '',
    throwster_record_id: '',
    throwster_batch_id: '',
    allocated_machine_id: '',
    vessel_type_allocated: 'HANK_DYE_MOVING_ARM_OPEN',
    operator_name: '',
    actual_liquor_volume_liters: '',
    bath_start_time: '',
    bath_end_time: '',
    peak_boil_temperature_celsius: '90',
    fixation_duration_minutes: '',
    hank_unit_weight_g: 500,
    lease_tie_type: 'STANDARD_COTTON_STRING',
    machine_type: 'ARM_TYPE_CABINET_HANK_MACHINE',
    pump_flow_rate_lpm: '',
    liquor_ratio: '1_30_MEDIUM_RATIO',
    pre_boil_hardness_ppm: '',
    water_treatment_status: 'RO_FILTERED_PURE',
    peak_heating_temperature_bracket: '86C_90C_HIGH_BOIL',
    input_skein_dry_weight_kg: '',
    output_skein_dry_weight_kg: '',
    post_dye_softening_type: 'CATIONIC_FATTY_AMIDE_EMULSION',
    core_to_surface_shade_match: 'PASS_100_PENETRATION',
    tie_mark_spot_found: false,
    post_dye_winding_break_count: 0,
    hank_entanglement_rating: 'GRADE_5_FREE_FLOWING',
    target_machine_type: '1536_HOOK_JACQUARD',
    recipe_scaler_multiplier: 1.0
  })

  const [chemicalForm, setChemicalForm] = useState({
    chemical_name: '',
    quantity_grams: '',
    volume_ml: '',
    component_type: 'DYE_COMPONENT',
    sequence_order: 1,
    notes: ''
  })

  useEffect(() => {
    fetchColoristRecipes()
    fetchJobs()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchColoristRecipes = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/colorist/recipes`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setColoristRecipes(data.recipes || [])
    } catch (error) {
      console.error('Failed to fetch colorist recipes:', error)
    }
  }

  const fetchJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/skein-dye/jobs`, {
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
      const response = await fetch(`${API_URL}/skein-dye/certificates`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/skein-dye`, {
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
        master_colorist_recipe_id: jobForm.master_colorist_recipe_id || null,
        master_colorist_certificate_id: jobForm.master_colorist_certificate_id || null,
        throwster_record_id: jobForm.throwster_record_id || null,
        throwster_batch_id: jobForm.throwster_batch_id || null,
        peak_boil_temperature_celsius: jobForm.peak_boil_temperature_celsius ? parseInt(jobForm.peak_boil_temperature_celsius) : null,
        fixation_duration_minutes: jobForm.fixation_duration_minutes ? parseInt(jobForm.fixation_duration_minutes) : null,
        hank_unit_weight_g: jobForm.hank_unit_weight_g ? parseInt(jobForm.hank_unit_weight_g) : null,
        pump_flow_rate_lpm: jobForm.pump_flow_rate_lpm ? parseFloat(jobForm.pump_flow_rate_lpm) : null,
        actual_liquor_volume_liters: jobForm.actual_liquor_volume_liters ? parseFloat(jobForm.actual_liquor_volume_liters) : null,
        pre_boil_hardness_ppm: jobForm.pre_boil_hardness_ppm ? parseInt(jobForm.pre_boil_hardness_ppm) : null,
        input_skein_dry_weight_kg: jobForm.input_skein_dry_weight_kg ? parseFloat(jobForm.input_skein_dry_weight_kg) : null,
        output_skein_dry_weight_kg: jobForm.output_skein_dry_weight_kg ? parseFloat(jobForm.output_skein_dry_weight_kg) : null,
        post_dye_winding_break_count: jobForm.post_dye_winding_break_count ? parseInt(jobForm.post_dye_winding_break_count) : 0
      }

      const response = await fetch(`${API_URL}/skein-dye/jobs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Job ${data.job_id} created`, 'success')
        setValidationResult({ type: 'success', data })
        setJobForm({
          job_id: '', master_colorist_recipe_id: '', master_colorist_certificate_id: '',
          throwster_record_id: '', throwster_batch_id: '', allocated_machine_id: '',
          vessel_type_allocated: 'HANK_DYE_MOVING_ARM_OPEN', operator_name: '',
          actual_liquor_volume_liters: '', bath_start_time: '', bath_end_time: '',
          peak_boil_temperature_celsius: '90', fixation_duration_minutes: '',
          hank_unit_weight_g: 500, lease_tie_type: 'STANDARD_COTTON_STRING',
          machine_type: 'ARM_TYPE_CABINET_HANK_MACHINE', pump_flow_rate_lpm: '',
          liquor_ratio: '1_30_MEDIUM_RATIO', pre_boil_hardness_ppm: '',
          water_treatment_status: 'RO_FILTERED_PURE', peak_heating_temperature_bracket: '86C_90C_HIGH_BOIL',
          input_skein_dry_weight_kg: '', output_skein_dry_weight_kg: '',
          post_dye_softening_type: 'CATIONIC_FATTY_AMIDE_EMULSION',
          core_to_surface_shade_match: 'PASS_100_PENETRATION',
          tie_mark_spot_found: false, post_dye_winding_break_count: 0,
          hank_entanglement_rating: 'GRADE_5_FREE_FLOWING',
          target_machine_type: '1536_HOOK_JACQUARD', recipe_scaler_multiplier: 1.0
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

  const handleAddChemical = async () => {
    if (!jobForm.master_colorist_recipe_id) {
      addNotification('Please select a recipe first', 'error')
      return
    }
    setSubmitting(true)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...chemicalForm,
        quantity_grams: chemicalForm.quantity_grams ? parseFloat(chemicalForm.quantity_grams) : null,
        volume_ml: chemicalForm.volume_ml ? parseFloat(chemicalForm.volume_ml) : null
      }

      const response = await fetch(`${API_URL}/skein-dye/jobs/${jobForm.master_colorist_recipe_id}/chemicals`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Chemical added to job', 'success')
        setChemicalForm({
          chemical_name: '', quantity_grams: '', volume_ml: '',
          component_type: 'DYE_COMPONENT', sequence_order: chemicalForm.sequence_order + 1, notes: ''
        })
      } else {
        addNotification(data.error || 'Failed to add chemical', 'error')
      }
    } catch (error) {
      addNotification('Failed to add chemical', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleComplete = async (jobId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/skein-dye/jobs/${jobId}/complete`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ bath_end_time: new Date().toISOString() })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Job completed', 'success')
        fetchJobs()
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
      const response = await fetch(`${API_URL}/skein-dye/jobs/${jobId}/certify`, {
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
      case 'COMPLETED': return 'success'
      case 'CERTIFIED': return 'success'
      case 'IN_PROGRESS': return 'info'
      case 'QC_HOLD': return 'warning'
      case 'REJECTED': return 'error'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'WARP_PREMIUM_2400_HOOK_READY': return 'success'
      case 'WARP_PREMIUM_1536_HOOK_READY': return 'success'
      case 'WEFT_ONLY_APPROVED': return 'info'
      case 'QC_REJECT_HOLD': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Skein Dye Master — Physical Dye Bath Execution & Floor Management
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
          {SKEIN_DYE_TABS.map((t) => (
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
          {tab === 'job-creation' && `Job ${validationResult.data.job_id} created`}
          {tab === 'quality-audit' && 'Quality audit recorded'}
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
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
              <Typography variant="h6" gutterBottom>Master Colorist Recipes (Pre-Process)</Typography>
              {coloristRecipes.length === 0 ? (
                <Typography color="text.secondary">No approved recipes found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Recipe Code</TableCell>
                        <TableCell>Internal Shade</TableCell>
                        <TableCell>Pantone</TableCell>
                        <TableCell>Silk Origin</TableCell>
                        <TableCell>Dye Class</TableCell>
                        <TableCell>Delta-E</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell>Routing</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {coloristRecipes.map((recipe) => (
                        <TableRow key={recipe.id}>
                          <TableCell>{recipe.recipe_code}</TableCell>
                          <TableCell>{recipe.internal_shade_code}</TableCell>
                          <TableCell>{recipe.pantone_reference_id}</TableCell>
                          <TableCell>{recipe.silk_origin_type_suitability}</TableCell>
                          <TableCell>{recipe.dye_class_used}</TableCell>
                          <TableCell>{recipe.color_difference_delta_e}</TableCell>
                          <TableCell><Chip label={recipe.status} color={getStatusColor(recipe.status)} size="small" /></TableCell>
                          <TableCell><Chip label={recipe.auto_assigned_routing} color={getRoutingColor(recipe.auto_assigned_routing)} size="small" /></TableCell>
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
              <Typography variant="h6" gutterBottom>1. Master Recipe Linkage</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel>Select Master Colorist Recipe</InputLabel>
                    <Select value={jobForm.master_colorist_recipe_id} label="Select Master Colorist Recipe"
                      onChange={(e) => setJobForm({ ...jobForm, master_colorist_recipe_id: e.target.value })}>
                      <MenuItem value="">Select recipe</MenuItem>
                      {coloristRecipes.map((recipe) => (
                        <MenuItem key={recipe.id} value={recipe.id}>{recipe.recipe_code} — {recipe.internal_shade_code}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField fullWidth label="Job ID" value={jobForm.job_id}
                    onChange={(e) => setJobForm({ ...jobForm, job_id: e.target.value })}
                    placeholder="e.g., SKEIN-JOB-2024-001" required />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>2. Machine Allocation & Traceability</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Allocated Machine ID" value={jobForm.allocated_machine_id}
                    onChange={(e) => setJobForm({ ...jobForm, allocated_machine_id: e.target.value })}
                    placeholder="e.g., MACH-001" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Vessel Type Allocated</InputLabel>
                    <Select value={jobForm.vessel_type_allocated} label="Vessel Type Allocated"
                      onChange={(e) => setJobForm({ ...jobForm, vessel_type_allocated: e.target.value })}>
                      {VESSEL_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Operator Name" value={jobForm.operator_name}
                    onChange={(e) => setJobForm({ ...jobForm, operator_name: e.target.value })}
                    placeholder="e.g., John Doe" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Actual Liquor Volume (liters)" type="number"
                    value={jobForm.actual_liquor_volume_liters}
                    onChange={(e) => setJobForm({ ...jobForm, actual_liquor_volume_liters: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Hank Unit Weight (g)</InputLabel>
                    <Select value={jobForm.hank_unit_weight_g} label="Hank Unit Weight (g)"
                      onChange={(e) => setJobForm({ ...jobForm, hank_unit_weight_g: parseInt(e.target.value) })}>
                      <MenuItem value={250}>250g</MenuItem>
                      <MenuItem value={300}>300g</MenuItem>
                      <MenuItem value={500}>500g</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Lease Tie Type</InputLabel>
                    <Select value={jobForm.lease_tie_type} label="Lease Tie Type"
                      onChange={(e) => setJobForm({ ...jobForm, lease_tie_type: e.target.value })}>
                      {LEASE_TIE_TYPE_OPTIONS.map((opt) => (
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
              <Typography variant="h6" gutterBottom>3. Process Physics & Fluid Control</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Machine Type</InputLabel>
                    <Select value={jobForm.machine_type} label="Machine Type"
                      onChange={(e) => setJobForm({ ...jobForm, machine_type: e.target.value })}>
                      {MACHINE_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Pump Flow Rate (LPM)" type="number"
                    value={jobForm.pump_flow_rate_lpm}
                    onChange={(e) => setJobForm({ ...jobForm, pump_flow_rate_lpm: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Liquor Ratio</InputLabel>
                    <Select value={jobForm.liquor_ratio} label="Liquor Ratio"
                      onChange={(e) => setJobForm({ ...jobForm, liquor_ratio: e.target.value })}>
                      {LIQUOR_RATIO_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Pre-Boil Hardness (PPM)" type="number"
                    value={jobForm.pre_boil_hardness_ppm}
                    onChange={(e) => setJobForm({ ...jobForm, pre_boil_hardness_ppm: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="Must be < 50 PPM" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Water Treatment Status</InputLabel>
                    <Select value={jobForm.water_treatment_status} label="Water Treatment Status"
                      onChange={(e) => setJobForm({ ...jobForm, water_treatment_status: e.target.value })}>
                      {WATER_TREATMENT_OPTIONS.map((opt) => (
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
              <Typography variant="h6" gutterBottom>4. Thermal-Time Curve Tracking</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Bath Start Time" type="datetime-local"
                    value={jobForm.bath_start_time}
                    onChange={(e) => setJobForm({ ...jobForm, bath_start_time: e.target.value })}
                    InputLabelProps={{ shrink: true }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Bath End Time" type="datetime-local"
                    value={jobForm.bath_end_time}
                    onChange={(e) => setJobForm({ ...jobForm, bath_end_time: e.target.value })}
                    InputLabelProps={{ shrink: true }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Peak Heating Temperature Bracket</InputLabel>
                    <Select value={jobForm.peak_heating_temperature_bracket} label="Peak Heating Temperature Bracket"
                      onChange={(e) => setJobForm({ ...jobForm, peak_heating_temperature_bracket: e.target.value })}>
                      {PEAK_TEMP_BRACKET_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Peak Boil Temperature (°C)" type="number"
                    value={jobForm.peak_boil_temperature_celsius}
                    onChange={(e) => setJobForm({ ...jobForm, peak_boil_temperature_celsius: e.target.value })}
                    inputProps={{ step: '1', min: 0, max: 120 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Fixation Duration (minutes)" type="number"
                    value={jobForm.fixation_duration_minutes}
                    onChange={(e) => setJobForm({ ...jobForm, fixation_duration_minutes: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>5. Floor Mass Balance & Material Accounting</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Input Skein Dry Weight (kg)" type="number"
                    value={jobForm.input_skein_dry_weight_kg}
                    onChange={(e) => setJobForm({ ...jobForm, input_skein_dry_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Output Skein Dry Weight (kg)" type="number"
                    value={jobForm.output_skein_dry_weight_kg}
                    onChange={(e) => setJobForm({ ...jobForm, output_skein_dry_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Recipe Scaler Multiplier" type="number"
                    value={jobForm.recipe_scaler_multiplier}
                    onChange={(e) => setJobForm({ ...jobForm, recipe_scaler_multiplier: parseFloat(e.target.value) || 1 })}
                    inputProps={{ step: '0.1', min: 0.1 }} helperText="e.g., 50 for 50 KG batch" />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Grid container spacing={2}>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleJobSubmit} disabled={submitting}>
                    {submitting ? 'Submitting...' : 'Save Skein Dye Job'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Skein Dye Jobs</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Job ID</TableCell>
                      <TableCell>Machine</TableCell>
                      <TableCell>Vessel Type</TableCell>
                      <TableCell>Hank Weight (g)</TableCell>
                      <TableCell>Peak Temp (°C)</TableCell>
                      <TableCell>Fixation (min)</TableCell>
                      <TableCell>Yield Variance (kg)</TableCell>
                      <TableCell>Routing</TableCell>
                      <TableCell>Status</TableCell>
                      <TableCell>Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {jobs.map((job) => (
                      <TableRow key={job.id}>
                        <TableCell>{job.job_id}</TableCell>
                        <TableCell>{job.allocated_machine_id}</TableCell>
                        <TableCell>{job.vessel_type_allocated}</TableCell>
                        <TableCell>{job.hank_unit_weight_g}</TableCell>
                        <TableCell>{job.peak_boil_temperature_celsius}</TableCell>
                        <TableCell>{job.fixation_duration_minutes}</TableCell>
                        <TableCell>{job.dye_house_yield_variance}</TableCell>
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

      {/* ===================== POST-DYE QUALITY AUDIT TAB ===================== */}
      {tab === 'quality-audit' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Post-Dye Quality & Floor Audit</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Select Job</InputLabel>
                    <Select value={jobForm.master_colorist_recipe_id} label="Select Job"
                      onChange={(e) => setJobForm({ ...jobForm, master_colorist_recipe_id: e.target.value })}>
                      <MenuItem value="">Select job</MenuItem>
                      {jobs.map((job) => (
                        <MenuItem key={job.id} value={job.id}>{job.job_id} — {job.allocated_machine_id}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Core-to-Surface Shade Match</InputLabel>
                    <Select value={jobForm.core_to_surface_shade_match} label="Core-to-Surface Shade Match"
                      onChange={(e) => setJobForm({ ...jobForm, core_to_surface_shade_match: e.target.value })}>
                      {SHADE_MATCH_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Hank Entanglement Rating</InputLabel>
                    <Select value={jobForm.hank_entanglement_rating} label="Hank Entanglement Rating"
                      onChange={(e) => setJobForm({ ...jobForm, hank_entanglement_rating: e.target.value })}>
                      {ENTANGLEMENT_RATING_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Tie-Mark Spot Found" type="checkbox"
                    checked={jobForm.tie_mark_spot_found}
                    onChange={(e) => setJobForm({ ...jobForm, tie_mark_spot_found: e.target.checked })}
                    InputLabelProps={{ shrink: true }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Post-Dye Winding Break Count" type="number"
                    value={jobForm.post_dye_winding_break_count}
                    onChange={(e) => setJobForm({ ...jobForm, post_dye_winding_break_count: parseInt(e.target.value) || 0 })}
                    inputProps={{ step: '1', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Post-Dye Softening Type</InputLabel>
                    <Select value={jobForm.post_dye_softening_type} label="Post-Dye Softening Type"
                      onChange={(e) => setJobForm({ ...jobForm, post_dye_softening_type: e.target.value })}>
                      {SOFTENING_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
              <Alert severity="info" sx={{ mt: 3 }}>
                <Typography variant="subtitle2">Automated ERP Routing & Validation Rules</Typography>
                <Typography variant="body2">
                  • 2400 Hook + Winding Breaks > 1 → REJECT_FOR_2400_WARP (HIGH_KNOT_COUNT_WILL_JAM_REED)<br/>
                  • 2400 Hook + Hank Weight > 300g → RE_SKEIN_TO_SMALLER_HANKS (CORE_DYE_PENETRATION_RISK)<br/>
                  • Tie-Mark Spot Found → QC_REJECT_HOLD / DOWNGRADE_TO_WEFT_ONLY<br/>
                  • Fixation Duration > Recipe Target + 15 min → MATERIAL_STRESS_RISK<br/>
                  • Pre-Boil Hardness > 50 PPM → HARD_WATER_WARNING
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
              <Typography variant="h6" gutterBottom>Skein Dye Certificates — Post-Process Output</Typography>
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
                        <TableCell>Machine</TableCell>
                        <TableCell>Peak Temp (°C)</TableCell>
                        <TableCell>Fixation (min)</TableCell>
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
                          <TableCell>{cert.job_id_ref}</TableCell>
                          <TableCell>{cert.allocated_machine_id}</TableCell>
                          <TableCell>{cert.peak_boil_temperature_celsius}</TableCell>
                          <TableCell>{cert.fixation_duration_minutes}</TableCell>
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
              <Typography variant="h6" gutterBottom>Sales Forecast — Skein Dye Material Processing Plan</Typography>
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
                          <TableCell>Dye Class</TableCell>
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
                            <TableCell>{item.dye_class}</TableCell>
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
                          <TableCell>Est. Sarees</TableCell>
                          <TableCell>Est. Silk (kg)</TableCell>
                          <TableCell>Dye Class</TableCell>
                          <TableCell>Target Machine</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.upcoming_lots.map((lot, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{lot.lot_number}</TableCell>
                            <TableCell>{lot.saree_category}</TableCell>
                            <TableCell>{lot.shade_code}</TableCell>
                            <TableCell>{lot.estimated_sarees}</TableCell>
                            <TableCell>{lot.estimated_silk_kg}</TableCell>
                            <TableCell>{lot.dye_class}</TableCell>
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
      <EnterprisePanel userId={user?.id} factoryNodeId={user?.factory_node_id} />
    </Container>
  )
}
