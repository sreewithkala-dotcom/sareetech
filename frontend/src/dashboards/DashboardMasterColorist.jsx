import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider, Checkbox, FormControlLabel } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'
import EnterprisePanel from '../components/EnterprisePanel'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const COLORIST_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Throwster' },
  { id: 'recipe-creation', label: 'Master Recipe Creation' },
  { id: 'spectrophotometer', label: 'Spectrophotometer & Fastness Audit' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const SILK_ORIGIN_OPTIONS = [
  { value: 'BIVOLTINE_WHITE_ONLY', label: 'Bivoltine White Only' },
  { value: 'MULTIVOLTINE_YELLOW_ONLY', label: 'Multivoltine Yellow Only' },
  { value: 'UNIVERSAL_BLENDED', label: 'Universal Blended' },
  { value: 'DUPION_COARSE_YARN', label: 'Dupion Coarse Yarn' },
  { value: 'TUSSAR_WILD_SILK', label: 'Tussar Wild Silk' },
  { value: 'ERI_WOOLEN_SILK', label: 'Eri Woolen Silk' },
  { value: 'MUGA_GOLDEN_SILK', label: 'Muga Golden Silk' },
]

const LIQUOR_RATIO_OPTIONS = [
  { value: '1_30_STANDARD', label: '1:30 (Standard)' },
  { value: '1_40_GENTLE_HIGH_VOLUME', label: '1:40 (Gentle / High-Volume)' },
  { value: '1_50_ULTRA_FINE_YARN', label: '1:50 (Ultra-Fine Yarn)' },
]

const DYE_CLASS_OPTIONS = [
  { value: 'ACID_LEV_EQUALISING', label: 'Acid Lev Equalising' },
  { value: 'ACID_MILL_MILLING', label: 'Acid Mill Milling' },
  { value: 'ACID_SUPER_MIL_PRE_METALLISED_1_1', label: 'Acid Super Mil Pre-Metallised 1:1' },
  { value: 'ACID_PRE_METALLISED_1_2', label: 'Acid Pre-Metallised 1:2' },
  { value: 'REACTIVE_CIBACRON_F', label: 'Reactive Cibacron F' },
  { value: 'REACTIVE_REMAZOL_VINYLSULFONE', label: 'Reactive Remazol Vinylsulfone' },
  { value: 'NATURAL_INDIGO_PLANT', label: 'Natural Indigo Plant' },
  { value: 'NATURAL_MADDER_ROOT', label: 'Natural Madder Root' },
  { value: 'NATURAL_TURMERIC_MARIGOLD', label: 'Natural Turmeric Marigold' },
]

const ACID_FIXATIVE_OPTIONS = [
  { value: 'ACETIC_ACID_GLACIAL_99PCT', label: 'Acetic Acid Glacial 99%' },
  { value: 'ACETIC_ACID_DILUTE_50PCT', label: 'Acetic Acid Dilute 50%' },
  { value: 'FORMIC_ACID_85PCT', label: 'Formic Acid 85%' },
  { value: 'AMMONIUM_SULFATE_SALT', label: 'Ammonium Sulfate Salt' },
  { value: 'CITRIC_ACID_POWDER', label: 'Citric Acid Powder' },
  { value: 'SULFURIC_ACID_DILUTE', label: 'Sulfuric Acid Dilute' },
]

const LEVELING_AGENT_OPTIONS = [
  { value: 'GLAUBER_SALT_ANHYDROUS', label: 'Glauber Salt Anhydrous' },
  { value: 'COMMON_SALT_SODIUM_CHLORIDE', label: 'Common Salt Sodium Chloride' },
  { value: 'NON_IONIC_ETHOXYLATED_AMINE', label: 'Non-Ionic Ethoxylated Amine' },
  { value: 'ANIONIC_ALKYL_SULFATE', label: 'Anionic Alkyl Sulfate' },
  { value: 'CATIONIC_DYE_FIXING_POLYMER', label: 'Cationic Dye Fixing Polymer' },
  { value: 'NONE', label: 'None' },
]

const FASTNESS_OPTIONS = [
  { value: 'GRADE_5_NO_TRANSFER', label: 'Grade 5 (No Transfer)' },
  { value: 'GRADE_4', label: 'Grade 4' },
  { value: 'GRADE_3', label: 'Grade 3' },
  { value: 'GRADE_1_2_SEVERE_TRANSFER', label: 'Grade 1-2 (Severe Transfer)' },
]

const TARGET_MACHINE_OPTIONS = [
  { value: '1536_HOOK_JACQUARD', label: '1536 Hook Jacquard (Standard Luxury Brocade)' },
  { value: '2400_HOOK_JACQUARD', label: '2400 Hook Jacquard (High-Density Fine Motif)' },
  { value: 'HANDLOOM', label: 'Handloom' },
  { value: 'POWERLOOM', label: 'Powerloom' },
  { value: 'RAPIER_LOOM', label: 'Rapier Loom' },
]

export default function DashboardMasterColorist() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [throwsterRecords, setThrowsterRecords] = useState([])
  const [recipes, setRecipes] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [recipeForm, setRecipeForm] = useState({
    recipe_code: '',
    internal_shade_code: '',
    pantone_reference_id: '',
    silk_origin_type_suitability: 'BIVOLTINE_WHITE_ONLY',
    liquor_ratio: '1_30_STANDARD',
    throwster_record_id: '',
    throwster_batch_id: '',
    target_machine_type: '1536_HOOK_JACQUARD',
    dye_class_used: 'ACID_LEV_EQUALISING',
    dyebath_ph: '5.0',
    max_temperature_celsius: '95',
    leveling_agent_added: false,
    color_difference_delta_e: '',
    dry_crocking_fastness: 'GRADE_4',
    wet_crocking_fastness: 'GRADE_3',
    post_dye_tenacity_gd: '',
    antistatic_lubricant_applied: false,
    acid_fixative_type: 'ACETIC_ACID_GLACIAL_99PCT',
    leveling_exhausting_agent: 'GLAUBER_SALT_ANHYDROUS'
  })

  const [spectraForm, setSpectraForm] = useState({
    recipe_id: '',
    delta_e_value: '',
    light_source_profile: [],
    cie_lab_coordinates: {},
    dry_crocking_fastness: 'GRADE_4',
    wet_crocking_fastness: 'GRADE_3',
    post_dye_tenacity_gd: '',
    antistatic_lubricant_applied: false
  })

  const [components, setComponents] = useState([])
  const [componentForm, setComponentForm] = useState({
    component_type: 'DYE_COMPONENT',
    chemical_name: '',
    quantity_grams: '',
    volume_ml: '',
    sequence_order: 1,
    notes: ''
  })

  useEffect(() => {
    fetchThrowsterRecords()
    fetchRecipes()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchThrowsterRecords = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/throwster/records`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setThrowsterRecords(data.records || [])
    } catch (error) {
      console.error('Failed to fetch throwster records:', error)
    }
  }

  const fetchRecipes = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/colorist/recipes`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setRecipes(data.recipes || [])
    } catch (error) {
      console.error('Failed to fetch recipes:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/colorist/certificates`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/colorist`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setForecast(data)
    } catch (error) {
      console.error('Failed to fetch sales forecast:', error)
    }
  }

  const handleRecipeSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...recipeForm,
        throwster_record_id: recipeForm.throwster_record_id || null,
        throwster_batch_id: recipeForm.throwster_batch_id || null,
        dyebath_ph: recipeForm.dyebath_ph ? parseFloat(recipeForm.dyebath_ph) : null,
        max_temperature_celsius: recipeForm.max_temperature_celsius ? parseInt(recipeForm.max_temperature_celsius) : null,
        color_difference_delta_e: recipeForm.color_difference_delta_e ? parseFloat(recipeForm.color_difference_delta_e) : null,
        post_dye_tenacity_gd: recipeForm.post_dye_tenacity_gd ? parseFloat(recipeForm.post_dye_tenacity_gd) : null
      }

      const response = await fetch(`${API_URL}/colorist/recipes`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Recipe ${data.recipe_code} created`, 'success')
        setValidationResult({ type: 'success', data })
        setRecipeForm({
          recipe_code: '', internal_shade_code: '', pantone_reference_id: '',
          silk_origin_type_suitability: 'BIVOLTINE_WHITE_ONLY', liquor_ratio: '1_30_STANDARD',
          throwster_record_id: '', throwster_batch_id: '', target_machine_type: '1536_HOOK_JACQUARD',
          dye_class_used: 'ACID_LEV_EQUALISING', dyebath_ph: '5.0', max_temperature_celsius: '95',
          leveling_agent_added: false, color_difference_delta_e: '', dry_crocking_fastness: 'GRADE_4',
          wet_crocking_fastness: 'GRADE_3', post_dye_tenacity_gd: '', antistatic_lubricant_applied: false,
          acid_fixative_type: 'ACETIC_ACID_GLACIAL_99PCT', leveling_exhausting_agent: 'GLAUBER_SALT_ANHYDROUS'
        })
        fetchRecipes()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create recipe', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleAddComponent = async () => {
    if (!spectraForm.recipe_id) {
      addNotification('Please select a recipe first', 'error')
      return
    }
    setSubmitting(true)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...componentForm,
        quantity_grams: componentForm.quantity_grams ? parseFloat(componentForm.quantity_grams) : null,
        volume_ml: componentForm.volume_ml ? parseFloat(componentForm.volume_ml) : null
      }

      const response = await fetch(`${API_URL}/colorist/recipes/${spectraForm.recipe_id}/components`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Component added to recipe', 'success')
        setComponentForm({
          component_type: 'DYE_COMPONENT',
          chemical_name: '',
          quantity_grams: '',
          volume_ml: '',
          sequence_order: componentForm.sequence_order + 1,
          notes: ''
        })
      } else {
        addNotification(data.error || 'Failed to add component', 'error')
      }
    } catch (error) {
      addNotification('Failed to add component', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleApprove = async (recipeId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/colorist/recipes/${recipeId}/approve`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ notes: 'Approved by Master Colorist' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Recipe approved: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchRecipes()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Approval failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to approve recipe', 'error')
    }
  }

  const handleReject = async (recipeId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/colorist/recipes/${recipeId}/reject`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason: 'Rejected by Master Colorist' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Recipe rejected', 'warning')
        fetchRecipes()
      } else {
        addNotification(data.error || 'Rejection failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to reject recipe', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'APPROVED': return 'success'
      case 'CERTIFIED': return 'success'
      case 'SHADE_REJECTED': return 'error'
      case 'LAB_DIP_PENDING': return 'warning'
      case 'QC_HOLD': return 'warning'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'WARP_PREMIUM_2400_HOOK_READY': return 'success'
      case 'WARP_PREMIUM_1536_HOOK_READY': return 'success'
      case 'WEFT_ONLY_APPROVED': return 'info'
      case 'SHADE_REJECTED': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Master Colorist — Shade Matching & Chemical Recipe Formulation
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
          {COLORIST_TABS.map((t) => (
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
          {tab === 'recipe-creation' && `Recipe ${validationResult.data.recipe_code} created`}
          {tab === 'spectrophotometer' && 'Spectrophotometer audit updated'}
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Hash:</strong> {certificateDetail.certificate_hash}</Typography>
          <Typography variant="body2"><strong>QR Tag:</strong> {certificateDetail.qr_tag_id}</Typography>
          <Typography variant="body2"><strong>Routing:</strong> {certificateDetail.auto_assigned_routing}</Typography>
          <Typography variant="body2"><strong>Version:</strong> {certificateDetail.version}</Typography>
        </Alert>
      )}

      {/* ===================== PRE-PROCESS TAB ===================== */}
      {tab === 'preprocess' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Throwster/Twister Output (Pre-Process)</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Record ID</TableCell>
                      <TableCell>Batch No</TableCell>
                      <TableCell>Input Lot</TableCell>
                      <TableCell>Yarn Profile</TableCell>
                      <TableCell>Final TPM</TableCell>
                      <TableCell>Twist Var %</TableCell>
                      <TableCell>Routing</TableCell>
                      <TableCell>Status</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {throwsterRecords.map((record) => (
                      <TableRow key={record.id}>
                        <TableCell>{record.id}</TableCell>
                        <TableCell>{record.throwster_batch_no}</TableCell>
                        <TableCell>{record.input_raw_lot_no}</TableCell>
                        <TableCell>{record.engineered_yarn_profile}</TableCell>
                        <TableCell>{record.final_twist_tpm}</TableCell>
                        <TableCell>{record.twist_variation_pct}%</TableCell>
                        <TableCell><Chip label={record.auto_assigned_routing} color={getRoutingColor(record.auto_assigned_routing)} size="small" /></TableCell>
                        <TableCell><Chip label={record.status} color={getStatusColor(record.status)} size="small" /></TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== RECIPE CREATION TAB ===================== */}
      {tab === 'recipe-creation' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>1. Master Shade Profile & Recipe Mapping</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Recipe Code" value={recipeForm.recipe_code}
                    onChange={(e) => setRecipeForm({ ...recipeForm, recipe_code: e.target.value })}
                    placeholder="e.g., KNC-MRN-702" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Internal Shade Code" value={recipeForm.internal_shade_code}
                    onChange={(e) => setRecipeForm({ ...recipeForm, internal_shade_code: e.target.value })}
                    placeholder="e.g., KNC_MRN_702" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Pantone Reference ID" value={recipeForm.pantone_reference_id}
                    onChange={(e) => setRecipeForm({ ...recipeForm, pantone_reference_id: e.target.value })}
                    placeholder="e.g., PANTONE 19-1760 TCX" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Silk Origin Type Suitability</InputLabel>
                    <Select value={recipeForm.silk_origin_type_suitability} label="Silk Origin Type Suitability"
                      onChange={(e) => setRecipeForm({ ...recipeForm, silk_origin_type_suitability: e.target.value })}>
                      {SILK_ORIGIN_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Liquor Ratio</InputLabel>
                    <Select value={recipeForm.liquor_ratio} label="Liquor Ratio"
                      onChange={(e) => setRecipeForm({ ...recipeForm, liquor_ratio: e.target.value })}>
                      {LIQUOR_RATIO_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Target Machine Type</InputLabel>
                    <Select value={recipeForm.target_machine_type} label="Target Machine Type"
                      onChange={(e) => setRecipeForm({ ...recipeForm, target_machine_type: e.target.value })}>
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
              <Typography variant="h6" gutterBottom>2. Chemical Kitchen Composition (Per 1 KG Silk)</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Dye Chemical Class</InputLabel>
                    <Select value={recipeForm.dye_class_used} label="Dye Chemical Class"
                      onChange={(e) => setRecipeForm({ ...recipeForm, dye_class_used: e.target.value })}>
                      {DYE_CLASS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Acid Fixative Type</InputLabel>
                    <Select value={recipeForm.acid_fixative_type} label="Acid Fixative Type"
                      onChange={(e) => setRecipeForm({ ...recipeForm, acid_fixative_type: e.target.value })}>
                      {ACID_FIXATIVE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Leveling & Exhausting Agent</InputLabel>
                    <Select value={recipeForm.leveling_exhausting_agent} label="Leveling & Exhausting Agent"
                      onChange={(e) => setRecipeForm({ ...recipeForm, leveling_exhausting_agent: e.target.value })}>
                      {LEVELING_AGENT_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Dye Bath pH" type="number"
                    value={recipeForm.dyebath_ph}
                    onChange={(e) => setRecipeForm({ ...recipeForm, dyebath_ph: e.target.value })}
                    inputProps={{ step: '0.1', min: 0, max: 14 }} helperText="Target: 4.5 - 6.0" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Max Temperature (°C)" type="number"
                    value={recipeForm.max_temperature_celsius}
                    onChange={(e) => setRecipeForm({ ...recipeForm, max_temperature_celsius: e.target.value })}
                    inputProps={{ step: '1', min: 0, max: 120 }} helperText="Target: 85-98°C" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControlLabel
                    control={
                      <Checkbox
                        checked={recipeForm.leveling_agent_added}
                        onChange={(e) => setRecipeForm({ ...recipeForm, leveling_agent_added: e.target.checked })}
                      />
                    }
                    label="Leveling Agent Added"
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>3. Chemical Recipe Components Grid (Per 1 KG Silk)</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={3}>
                  <FormControl fullWidth>
                    <InputLabel>Component Type</InputLabel>
                    <Select value={componentForm.component_type} label="Component Type"
                      onChange={(e) => setComponentForm({ ...componentForm, component_type: e.target.value })}>
                      <MenuItem value="DYE_COMPONENT">Dye Component</MenuItem>
                      <MenuItem value="FIXING_AGENT">Fixing Agent</MenuItem>
                      <MenuItem value="ACID_BUFFER">Acid Buffer</MenuItem>
                      <MenuItem value="LEVELING_ADDITIVE">Leveling Additive</MenuItem>
                      <MenuItem value="SOFTENER">Softener</MenuItem>
                      <MenuItem value="ANTISTATIC_AGENT">Antistatic Agent</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Chemical Name" value={componentForm.chemical_name}
                    onChange={(e) => setComponentForm({ ...componentForm, chemical_name: e.target.value })}
                    placeholder="e.g., Acid Pre-Metallised 1:2" />
                </Grid>
                <Grid item xs={12} md={2}>
                  <TextField fullWidth label="Quantity (grams)" type="number"
                    value={componentForm.quantity_grams}
                    onChange={(e) => setComponentForm({ ...componentForm, quantity_grams: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={2}>
                  <TextField fullWidth label="Volume (ml)" type="number"
                    value={componentForm.volume_ml}
                    onChange={(e) => setComponentForm({ ...componentForm, volume_ml: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={2}>
                  <TextField fullWidth label="Sequence Order" type="number"
                    value={componentForm.sequence_order}
                    onChange={(e) => setComponentForm({ ...componentForm, sequence_order: parseInt(e.target.value) || 1 })}
                    inputProps={{ step: '1', min: 1 }} />
                </Grid>
                <Grid item xs={12} md={2}>
                  <Button variant="contained" onClick={handleAddComponent} disabled={submitting}>
                    Add Component
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Select Throwster Record</InputLabel>
                    <Select value={recipeForm.throwster_record_id} label="Select Throwster Record"
                      onChange={(e) => setRecipeForm({ ...recipeForm, throwster_record_id: e.target.value })}>
                      <MenuItem value="">Select record</MenuItem>
                      {throwsterRecords.map((record) => (
                        <MenuItem key={record.id} value={record.id}>{record.throwster_batch_no} — {record.input_raw_lot_no}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleRecipeSubmit} disabled={submitting}>
                    {submitting ? 'Submitting...' : 'Save Master Recipe'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Master Colorist Recipes</Typography>
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
                      <TableCell>Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {recipes.map((recipe) => (
                      <TableRow key={recipe.id}>
                        <TableCell>{recipe.recipe_code}</TableCell>
                        <TableCell>{recipe.internal_shade_code}</TableCell>
                        <TableCell>{recipe.pantone_reference_id}</TableCell>
                        <TableCell>{recipe.silk_origin_type_suitability}</TableCell>
                        <TableCell>{recipe.dye_class_used}</TableCell>
                        <TableCell>{recipe.color_difference_delta_e}</TableCell>
                        <TableCell><Chip label={recipe.status} color={getStatusColor(recipe.status)} size="small" /></TableCell>
                        <TableCell>
                          {recipe.status === 'LAB_DIP_PENDING' || recipe.status === 'DRAFT' ? (
                            <>
                              <Button size="small" variant="outlined" color="success" onClick={() => handleApprove(recipe.id)}>Approve</Button>
                              <Button size="small" variant="outlined" color="error" onClick={() => handleReject(recipe.id)} sx={{ ml: 1 }}>Reject</Button>
                            </>
                          ) : (
                            <Chip label={recipe.certificate_hash ? 'Certified' : 'Processed'} size="small" />
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

      {/* ===================== SPECTROPHOTOMETER & FASTNESS TAB ===================== */}
      {tab === 'spectrophotometer' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Spectrophotometer & Fastness Lab Audit</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Select Recipe</InputLabel>
                    <Select value={spectraForm.recipe_id} label="Select Recipe"
                      onChange={(e) => setSpectraForm({ ...spectraForm, recipe_id: e.target.value })}>
                      <MenuItem value="">Select recipe</MenuItem>
                      {recipes.map((recipe) => (
                        <MenuItem key={recipe.id} value={recipe.id}>{recipe.recipe_code} — {recipe.internal_shade_code}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Delta-E Value" type="number"
                    value={spectraForm.delta_e_value}
                    onChange={(e) => setSpectraForm({ ...spectraForm, delta_e_value: e.target.value })}
                    inputProps={{ step: '0.01', min: 0 }} helperText="Target: ≤1.0 (≤0.5 for 2400 Hook)" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Post-Dye Tenacity (g/d)" type="number"
                    value={spectraForm.post_dye_tenacity_gd}
                    onChange={(e) => setSpectraForm({ ...spectraForm, post_dye_tenacity_gd: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} helperText="Min 3.5 g/d (3.8+ for powerlooms)" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Dry Crocking Fastness</InputLabel>
                    <Select value={spectraForm.dry_crocking_fastness} label="Dry Crocking Fastness"
                      onChange={(e) => setSpectraForm({ ...spectraForm, dry_crocking_fastness: e.target.value })}>
                      {FASTNESS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Wet Crocking Fastness</InputLabel>
                    <Select value={spectraForm.wet_crocking_fastness} label="Wet Crocking Fastness"
                      onChange={(e) => setSpectraForm({ ...spectraForm, wet_crocking_fastness: e.target.value })}>
                      {FASTNESS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControlLabel
                    control={
                      <Checkbox
                        checked={spectraForm.antistatic_lubricant_applied}
                        onChange={(e) => setSpectraForm({ ...spectraForm, antistatic_lubricant_applied: e.target.checked })}
                      />
                    }
                    label="Antistatic Lubricant Applied (Mandatory for 2400 Hook)"
                  />
                </Grid>
                <Grid item xs={12}>
                  <Alert severity="info" sx={{ mb: 2 }}>
                    <Typography variant="subtitle2">Automated ERP Routing & Validation Rules</Typography>
                    <Typography variant="body2">
                      • 2400 Hook + Delta-E > 0.5 → DOWNGRADE_TO_WEFT_OR_RE_DYE (WARP_STREAK_RISK)<br/>
                      • 2400 Hook + Tenacity < 3.8 g/d → REJECT_FOR_2400_WARP (FIBER_WEAKENED_IN_DYE_BATH)<br/>
                      • 2400 Hook + No Antistatic Finish → CANNOT_DISPATCH_TO_WARPING (MISSING_FRICTION_SHIELD)
                    </Typography>
                  </Alert>
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={() => {
                    addNotification('Spectrophotometer audit recorded', 'success')
                    setValidationResult({ type: 'success', data: { status: 'SPECTRA_AUDIT_COMPLETE' } })
                  }}>
                    Save Spectrophotometer Audit
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== CERTIFICATES & FORECAST TAB ===================== */}
      {tab === 'certificates-forecast' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Master Colorist Certificates — Post-Process Output</Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates issued yet</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag</TableCell>
                        <TableCell>Recipe Code</TableCell>
                        <TableCell>Shade Code</TableCell>
                        <TableCell>Dye Class</TableCell>
                        <TableCell>Delta-E</TableCell>
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
                          <TableCell>{cert.recipe_code}</TableCell>
                          <TableCell>{cert.internal_shade_code}</TableCell>
                          <TableCell>{cert.dye_class_used}</TableCell>
                          <TableCell>{cert.delta_e_value}</TableCell>
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
              <Typography variant="h6" gutterBottom>Sales Forecast — Colorist Material Processing Plan</Typography>
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
                          <TableCell>Target Delta-E</TableCell>
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
                            <TableCell>{item.target_delta_e}</TableCell>
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
