import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider, Checkbox, FormControlLabel } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const DEGUMMING_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Zari Inspector' },
  { id: 'process-inputs', label: 'Process Inputs & Chemical Control' },
  { id: 'postprocess', label: 'Post-Process Quality & Weight Loss' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const BATH_LIQUOR_OPTIONS = [
  { value: '1:30_STANDARD_HANK', label: '1:30 (Standard Hank)' },
  { value: '1:40_GENTLE_HIGH_VOLUME', label: '1:40 (Gentle / High-Volume)' },
  { value: '1:50_ULTRA_FINE_YARN', label: '1:50 (Ultra-Fine Yarn)' },
]

const DEGUMMING_AGENT_OPTIONS = [
  { value: 'NEUTRAL_MARSEILLE_SOAP', label: 'Neutral Marseille Soap' },
  { value: 'SYNTHETIC_ANIONIC_DETERGENT', label: 'Synthetic Anionic Detergent' },
  { value: 'ENZYMATIC_PROTEASE_AGENT', label: 'Enzymatic Protease Agent' },
]

const ALKALI_OPTIONS = [
  { value: 'SODIUM_CARBONATE_SODA_ASH', label: 'Sodium Carbonate (Soda Ash)' },
  { value: 'SODIUM_BICARBONATE', label: 'Sodium Bicarbonate' },
  { value: 'TETRASODIUM_PYROPHOSPHATE', label: 'Trisodium Pyrophosphate' },
  { value: 'NONE', label: 'None' },
]

const WATER_SOFTENING_OPTIONS = [
  { value: 'EDTA_CHELATING_AGENT', label: 'EDTA Chelating Agent' },
  { value: 'ZEOLITE_POWDER', label: 'Zeolite Powder' },
  { value: 'REVERSE_OSMOSIS_PURE_WATER', label: 'Reverse Osmosis Pure Water' },
  { value: 'NONE', label: 'None' },
]

const VESSEL_OPTIONS = [
  { value: 'OPEN_BOILING_VAT', label: 'Open Boiling Vat' },
  { value: 'CLOSED_PRESSURE_KETTLE', label: 'Closed Pressure Kettle' },
  { value: 'CONTINUOUS_ROPE_WASHER', label: 'Continuous Rope Washer' },
]

const FIBRILLATION_OPTIONS = [
  { value: 'GRADE_5_FLAWLESS_GLASSY', label: 'Grade 5 (Flawless / Glassy)' },
  { value: 'GRADE_3_4_SLIGHT_FUZZ', label: 'Grade 3-4 (Slight Fuzz)' },
  { value: 'GRADE_1_2_SEVERE_CHAFING_SLUBS', label: 'Grade 1-2 (Severe Chafing / Slubs)' },
]

const TARGET_MACHINE_OPTIONS = [
  { value: '1536_HOOK_JACQUARD', label: '1536 Hook Jacquard (Standard Luxury Brocade)' },
  { value: '2400_HOOK_JACQUARD', label: '2400 Hook Jacquard (High-Density Fine Motif)' },
  { value: 'HANDLOOM', label: 'Handloom' },
  { value: 'POWERLOOM', label: 'Powerloom' },
  { value: 'RAPIER_LOOM', label: 'Rapier Loom' },
]

export default function DashboardSilkDegummingMaster() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [zariInspections, setZariInspections] = useState([])
  const [degummingBatches, setDegummingBatches] = useState([])
  const [degummingRecords, setDegummingRecords] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [batchForm, setBatchForm] = useState({
    degumming_batch_no: '',
    zari_inspection_id: '',
    zari_assay_id: '',
    zari_lot_batch_id: '',
    target_machine_type: '1536_HOOK_JACQUARD'
  })

  const [processForm, setProcessForm] = useState({
    degumming_batch_id: '',
    bath_liquor_ratio: '1:30_STANDARD_HANK',
    deaerating_agent_used: false,
    bath_ph_level: '9.5',
    boil_duration_minutes: '60',
    raw_dry_weight_kg: '',
    degummed_dry_weight_kg: '',
    post_degum_tenacity_gd: '',
    fibrillation_index: 'GRADE_5_FLAWLESS_GLASSY',
    degumming_agent_base: 'NEUTRAL_MARSEILLE_SOAP',
    alkali_buffer_additive: 'SODIUM_CARBONATE_SODA_ASH',
    water_softening_agent: 'EDTA_CHELATING_AGENT',
    vessel_type_allocated: 'OPEN_BOILING_VAT',
    boil_temperature_profile: '95'
  })

  useEffect(() => {
    fetchZariInspections()
    fetchDegummingBatches()
    fetchDegummingRecords()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchZariInspections = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/zari/inspection`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setZariInspections(data.inspections || [])
    } catch (error) {
      console.error('Failed to fetch Zari inspections:', error)
    }
  }

  const fetchDegummingBatches = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/degumming/batches`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setDegummingBatches(data.batches || [])
    } catch (error) {
      console.error('Failed to fetch degumming batches:', error)
    }
  }

  const fetchDegummingRecords = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/degumming/records`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setDegummingRecords(data.records || [])
    } catch (error) {
      console.error('Failed to fetch degumming records:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/degumming/certificates`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/degumming`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setForecast(data)
    } catch (error) {
      console.error('Failed to fetch sales forecast:', error)
    }
  }

  const handleBatchSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/degumming/batches`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(batchForm)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Degumming batch ${data.degumming_batch_no} created`, 'success')
        setValidationResult({ type: 'success', data })
        setBatchForm({ degumming_batch_no: '', zari_inspection_id: '', zari_assay_id: '', zari_lot_batch_id: '', target_machine_type: '1536_HOOK_JACQUARD' })
        fetchDegummingBatches()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create degumming batch', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleProcessSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...processForm,
        degumming_batch_id: processForm.degumming_batch_id || null,
        bath_ph_level: processForm.bath_ph_level ? parseFloat(processForm.bath_ph_level) : null,
        boil_duration_minutes: processForm.boil_duration_minutes ? parseInt(processForm.boil_duration_minutes) : null,
        raw_dry_weight_kg: processForm.raw_dry_weight_kg ? parseFloat(processForm.raw_dry_weight_kg) : null,
        degummed_dry_weight_kg: processForm.degummed_dry_weight_kg ? parseFloat(processForm.degummed_dry_weight_kg) : null,
        post_degum_tenacity_gd: processForm.post_degum_tenacity_gd ? parseFloat(processForm.post_degum_tenacity_gd) : null,
        boil_temperature_profile: processForm.boil_temperature_profile ? parseInt(processForm.boil_temperature_profile) : null
      }

      const response = await fetch(`${API_URL}/degumming/records`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Degumming record created. Sericin loss: ${data.sericin_loss_pct}%`, 'success')
        setValidationResult({ type: 'success', data })
        setProcessForm({
          degumming_batch_id: '', bath_liquor_ratio: '1:30_STANDARD_HANK', deaerating_agent_used: false,
          bath_ph_level: '9.5', boil_duration_minutes: '60', raw_dry_weight_kg: '', degummed_dry_weight_kg: '',
          post_degum_tenacity_gd: '', fibrillation_index: 'GRADE_5_FLAWLESS_GLASSY',
          degumming_agent_base: 'NEUTRAL_MARSEILLE_SOAP', alkali_buffer_additive: 'SODIUM_CARBONATE_SODA_ASH',
          water_softening_agent: 'EDTA_CHELATING_AGENT', vessel_type_allocated: 'OPEN_BOILING_VAT',
          boil_temperature_profile: '95'
        })
        fetchDegummingRecords()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create degumming record', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCertify = async (recordId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/degumming/records/${recordId}/certify`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ notes: 'Approved by Silk Degumming Master' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Degumming certified: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchDegummingRecords()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify degumming', 'error')
    }
  }

  const handleReject = async (recordId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/degumming/records/${recordId}/reject`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason: 'Rejected by Silk Degumming Master' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Degumming record rejected', 'warning')
        fetchDegummingRecords()
      } else {
        addNotification(data.error || 'Rejection failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to reject degumming', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'CERTIFIED': return 'success'
      case 'QC_HOLD': return 'warning'
      case 'REJECTED': return 'error'
      case 'IN_PROGRESS': return 'info'
      case 'QUEUED': return 'default'
      case 'COMPLETED': return 'success'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'LUXURY_2400_HOOK_JACQUARD_POOL': return 'success'
      case 'LUXURY_1536_HOOK_JACQUARD_POOL': return 'success'
      case 'COMMERCIAL_SEMI_PREMIUM': return 'info'
      case 'QC_REJECT_HOLD': return 'error'
      case 'DOWNGRADE_TO_1536_OR_WEFT': return 'warning'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Silk Degumming Master — Thermal-Chemical Process Control
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
      </Box>

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="fullWidth">
          {DEGUMMING_TABS.map((t) => (
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
          {tab === 'process-inputs' && 'Degumming record created'}
          {tab === 'postprocess' && 'Post-process quality updated'}
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Hash:</strong> {certificateDetail.certificate_hash}</Typography>
          <Typography variant="body2"><strong>QR Tag:</strong> {certificateDetail.qr_tag_id}</Typography>
          <Typography variant="body2"><strong>Routing:</strong> {certificateDetail.auto_assigned_routing}</Typography>
          <Typography variant="body2"><strong>Sericin Loss:</strong> {certificateDetail.sericin_loss_pct}%</Typography>
        </Alert>
      )}

      {/* ===================== PRE-PROCESS TAB ===================== */}
      {tab === 'preprocess' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Zari Inspector Output (Pre-Process)</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Inspection ID</TableCell>
                      <TableCell>Lot Batch</TableCell>
                      <TableCell>Assay Cert</TableCell>
                      <TableCell>Silver %</TableCell>
                      <TableCell>Gold %</TableCell>
                      <TableCell>Core Yarn</TableCell>
                      <TableCell>Routing</TableCell>
                      <TableCell>Status</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {zariInspections.map((insp) => (
                      <TableRow key={insp.id}>
                        <TableCell>{insp.id}</TableCell>
                        <TableCell>{insp.zari_lot_batch_no}</TableCell>
                        <TableCell>{insp.assay_certificate_no}</TableCell>
                        <TableCell>{insp.xrf_silver_purity_pct}%</TableCell>
                        <TableCell>{insp.xrf_gold_plating_pct}%</TableCell>
                        <TableCell>{insp.core_yarn_audit_result}</TableCell>
                        <TableCell><Chip label={insp.auto_assigned_routing} color={getRoutingColor(insp.auto_assigned_routing)} size="small" /></TableCell>
                        <TableCell><Chip label={insp.status} color={getStatusColor(insp.status)} size="small" /></TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Create Degumming Batch</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Degumming Batch No" value={batchForm.degumming_batch_no}
                    onChange={(e) => setBatchForm({ ...batchForm, degumming_batch_no: e.target.value })}
                    placeholder="e.g., DG-2024-001" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Zari Inspection</InputLabel>
                    <Select value={batchForm.zari_inspection_id} label="Zari Inspection"
                      onChange={(e) => setBatchForm({ ...batchForm, zari_inspection_id: e.target.value })}>
                      <MenuItem value="">Select inspection</MenuItem>
                      {zariInspections.map((insp) => (
                        <MenuItem key={insp.id} value={insp.id}>{insp.zari_lot_batch_no} — {insp.assay_certificate_no}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Target Machine Type</InputLabel>
                    <Select value={batchForm.target_machine_type} label="Target Machine Type"
                      onChange={(e) => setBatchForm({ ...batchForm, target_machine_type: e.target.value })}>
                      {TARGET_MACHINE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleBatchSubmit} disabled={submitting}>
                    {submitting ? 'Creating...' : 'Create Degumming Batch'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Degumming Batches</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Batch No</TableCell>
                      <TableCell>Lot Batch</TableCell>
                      <TableCell>Target Machine</TableCell>
                      <TableCell>Status</TableCell>
                      <TableCell>Created</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {degummingBatches.map((batch) => (
                      <TableRow key={batch.id}>
                        <TableCell>{batch.degumming_batch_no}</TableCell>
                        <TableCell>{batch.zari_lot_batch_no}</TableCell>
                        <TableCell>{batch.target_machine_type}</TableCell>
                        <TableCell><Chip label={batch.status} color={getStatusColor(batch.status)} size="small" /></TableCell>
                        <TableCell>{new Date(batch.created_at).toLocaleString()}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== PROCESS INPUTS TAB ===================== */}
      {tab === 'process-inputs' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Category A: Process Inputs & Chemical Control</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Degumming Batch</InputLabel>
                    <Select value={processForm.degumming_batch_id} label="Degumming Batch"
                      onChange={(e) => setProcessForm({ ...processForm, degumming_batch_id: e.target.value })}>
                      <MenuItem value="">Select batch</MenuItem>
                      {degummingBatches.map((batch) => (
                        <MenuItem key={batch.id} value={batch.id}>{batch.degumming_batch_no} — {batch.zari_lot_batch_no}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Bath Liquor Ratio</InputLabel>
                    <Select value={processForm.bath_liquor_ratio} label="Bath Liquor Ratio"
                      onChange={(e) => setProcessForm({ ...processForm, bath_liquor_ratio: e.target.value })}>
                      {BATH_LIQUOR_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Degumming Agent Base</InputLabel>
                    <Select value={processForm.degumming_agent_base} label="Degumming Agent Base"
                      onChange={(e) => setProcessForm({ ...processForm, degumming_agent_base: e.target.value })}>
                      {DEGUMMING_AGENT_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Alkali Buffer Additive</InputLabel>
                    <Select value={processForm.alkali_buffer_additive} label="Alkali Buffer Additive"
                      onChange={(e) => setProcessForm({ ...processForm, alkali_buffer_additive: e.target.value })}>
                      {ALKALI_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Water Softening Agent</InputLabel>
                    <Select value={processForm.water_softening_agent} label="Water Softening Agent"
                      onChange={(e) => setProcessForm({ ...processForm, water_softening_agent: e.target.value })}>
                      {WATER_SOFTENING_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Vessel Type Allocated</InputLabel>
                    <Select value={processForm.vessel_type_allocated} label="Vessel Type Allocated"
                      onChange={(e) => setProcessForm({ ...processForm, vessel_type_allocated: e.target.value })}>
                      {VESSEL_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Bath pH Level" type="number"
                    value={processForm.bath_ph_level}
                    onChange={(e) => setProcessForm({ ...processForm, bath_ph_level: e.target.value })}
                    inputProps={{ step: '0.1', min: 0, max: 14 }} helperText="Target: 9.0 - 10.2" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Boil Temperature (°C)" type="number"
                    value={processForm.boil_temperature_profile}
                    onChange={(e) => setProcessForm({ ...processForm, boil_temperature_profile: e.target.value })}
                    inputProps={{ step: '1', min: 0, max: 120 }} helperText="Target: 92°C - 98°C" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Boil Duration (minutes)" type="number"
                    value={processForm.boil_duration_minutes}
                    onChange={(e) => setProcessForm({ ...processForm, boil_duration_minutes: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="Typical: 45 - 90 minutes" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControlLabel
                    control={
                      <Checkbox
                        checked={processForm.deaerating_agent_used}
                        onChange={(e) => setProcessForm({ ...processForm, deaerating_agent_used: e.target.checked })}
                      />
                    }
                    label="Deaerating Agent Used"
                  />
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleProcessSubmit} disabled={submitting}>
                    {submitting ? 'Submitting...' : 'Save Process Inputs & Create Record'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== POST-PROCESS TAB ===================== */}
      {tab === 'postprocess' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Category B: Post-Process Quality & Weight Loss Metrics</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Raw Dry Weight (kg)" type="number"
                    value={processForm.raw_dry_weight_kg}
                    onChange={(e) => setProcessForm({ ...processForm, raw_dry_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} helperText="Pre-boil bone-dry mass" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Degummed Dry Weight (kg)" type="number"
                    value={processForm.degummed_dry_weight_kg}
                    onChange={(e) => setProcessForm({ ...processForm, degummed_dry_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} helperText="Post-boil bone-dry mass" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Post-Degum Tenacity (g/d)" type="number"
                    value={processForm.post_degum_tenacity_gd}
                    onChange={(e) => setProcessForm({ ...processForm, post_degum_tenacity_gd: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} helperText="Min 3.5 g/d (3.8+ for powerlooms)" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Fibrillation Index</InputLabel>
                    <Select value={processForm.fibrillation_index} label="Fibrillation Index"
                      onChange={(e) => setProcessForm({ ...processForm, fibrillation_index: e.target.value })}>
                      {FIBRILLATION_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Target Machine Type" select
                    value={batchForm.target_machine_type}
                    onChange={(e) => setBatchForm({ ...batchForm, target_machine_type: e.target.value })}>
                    {TARGET_MACHINE_OPTIONS.map((opt) => (
                      <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                    ))}
                  </TextField>
                </Grid>
                <Grid item xs={12}>
                  <Alert severity="info" sx={{ mb: 2 }}>
                    <Typography variant="subtitle2">Automated ERP Routing & Validation Rules</Typography>
                    <Typography variant="body2">
                      • 2400 Hook + Tenacity < 3.8 g/d → DOWNGRADE_TO_1536_OR_WEFT<br/>
                      • Sericin Loss > 24% → OVER_DEGUMMED warning<br/>
                      • 2400 Hook + Fibrillation Grade 3-4/1-2 → QC_REJECT_HOLD (SURFACE_FUZZ_WILL_JAM_FINE_REED)
                    </Typography>
                  </Alert>
                </Grid>
                <Grid item xs={12}>
                  <Box display="flex" gap={2}>
                    <Button variant="contained" onClick={handleProcessSubmit} disabled={submitting}>
                      {submitting ? 'Saving...' : 'Save Post-Process Record'}
                    </Button>
                  </Box>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Degumming Records</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Record ID</TableCell>
                      <TableCell>Batch No</TableCell>
                      <TableCell>Sericin Loss %</TableCell>
                      <TableCell>Tenacity (g/d)</TableCell>
                      <TableCell>Fibrillation</TableCell>
                      <TableCell>pH</TableCell>
                      <TableCell>Temp (°C)</TableCell>
                      <TableCell>Routing</TableCell>
                      <TableCell>Status</TableCell>
                      <TableCell>Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {degummingRecords.map((record) => (
                      <TableRow key={record.id}>
                        <TableCell>{record.id}</TableCell>
                        <TableCell>{record.degumming_batch_no}</TableCell>
                        <TableCell>{record.sericin_loss_pct}%</TableCell>
                        <TableCell>{record.post_degum_tenacity_gd}</TableCell>
                        <TableCell>{record.fibrillation_index}</TableCell>
                        <TableCell>{record.bath_ph_level}</TableCell>
                        <TableCell>{record.boil_temperature_profile}</TableCell>
                        <TableCell><Chip label={record.auto_assigned_routing} color={getRoutingColor(record.auto_assigned_routing)} size="small" /></TableCell>
                        <TableCell><Chip label={record.status} color={getStatusColor(record.status)} size="small" /></TableCell>
                        <TableCell>
                          {record.status === 'IN_PROGRESS' || record.status === 'DRAFT' ? (
                            <>
                              <Button size="small" variant="outlined" color="success" onClick={() => handleCertify(record.id)}>Certify</Button>
                              <Button size="small" variant="outlined" color="error" onClick={() => handleReject(record.id)} sx={{ ml: 1 }}>Reject</Button>
                            </>
                          ) : (
                            <Chip label={record.certificate_hash ? 'Certified' : 'Processed'} size="small" />
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

      {/* ===================== CERTIFICATES & FORECAST TAB ===================== */}
      {tab === 'certificates-forecast' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Degumming Certificates — Post-Process Output</Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates issued yet</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag</TableCell>
                        <TableCell>Batch No</TableCell>
                        <TableCell>Lot Batch</TableCell>
                        <TableCell>Sericin Loss %</TableCell>
                        <TableCell>Tenacity (g/d)</TableCell>
                        <TableCell>Fibrillation</TableCell>
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
                          <TableCell>{cert.degumming_batch_no}</TableCell>
                          <TableCell>{cert.zari_lot_batch_no}</TableCell>
                          <TableCell>{cert.sericin_loss_pct}%</TableCell>
                          <TableCell>{cert.post_degum_tenacity_gd}</TableCell>
                          <TableCell>{cert.fibrillation_index}</TableCell>
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
              <Typography variant="h6" gutterBottom>Sales Forecast — Degumming Material Processing Plan</Typography>
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
                          <TableCell>Silk Type</TableCell>
                          <TableCell>Est. Raw Silk (kg)</TableCell>
                          <TableCell>Target Sericin Loss</TableCell>
                          <TableCell>Target Tenacity</TableCell>
                          <TableCell>Priority</TableCell>
                          <TableCell>Target Machine</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.material_requirements.map((item, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{item.saree_category}</TableCell>
                            <TableCell>{item.silk_type}</TableCell>
                            <TableCell>{item.estimated_raw_silk_kg}</TableCell>
                            <TableCell>{item.target_sericin_loss_pct}%</TableCell>
                            <TableCell>{item.target_tenacity_gd} g/d</TableCell>
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
                          <TableCell>Est. Sarees</TableCell>
                          <TableCell>Est. Raw Silk (kg)</TableCell>
                          <TableCell>Target Sericin Loss</TableCell>
                          <TableCell>Target Machine</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.upcoming_lots.map((lot, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{lot.lot_number}</TableCell>
                            <TableCell>{lot.saree_category}</TableCell>
                            <TableCell>{lot.estimated_sarees}</TableCell>
                            <TableCell>{lot.estimated_raw_silk_kg}</TableCell>
                            <TableCell>{lot.target_sericin_loss_pct}%</TableCell>
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
