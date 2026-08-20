import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider, Checkbox, FormControlLabel } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'
import EnterprisePanel from '../components/EnterprisePanel'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const THROWSTER_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Degumming Master' },
  { id: 'machine-setup', label: 'Machine & Process Setup' },
  { id: 'post-twist', label: 'Post-Twist Quality & Lab Audit' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const PLY_COUNT_OPTIONS = [
  { value: 1, label: 'Single (1-Ply)' },
  { value: 2, label: '2-Ply (Organzine/Warp)' },
  { value: 3, label: '3-Ply (Tram/Weft)' },
  { value: 4, label: '4-Ply (Heavy Brocade Weft)' },
  { value: 6, label: '6-Ply (Heavy Structural Cords)' },
]

const INTENDED_USE_OPTIONS = [
  { value: 'WARP_YARN_HIGH_TWIST', label: 'Warp Yarn (High Twist)' },
  { value: 'WEFT_YARN_LOW_TWIST', label: 'Weft Yarn (Low Twist)' },
]

const YARN_TYPE_OPTIONS = [
  { value: 'BIVOLTINE_WHITE', label: 'Bivoltine White' },
  { value: 'MULTIVOLTINE_YELLOW', label: 'Multivoltine Yellow' },
  { value: 'DUPION_SILK', label: 'Dupion Silk' },
  { value: 'SPUN_SILK', label: 'Spun Silk' },
  { value: 'TUSSAR_WILD_SILK', label: 'Tussar Wild Silk' },
]

const TWIST_DIRECTION_OPTIONS = [
  { value: 'S_TWIST', label: 'S-Twist (Left-handed)' },
  { value: 'Z_TWIST', label: 'Z-Twist (Right-handed)' },
  { value: 'S_Z_CABLE_TWIST', label: 'S/Z Balanced Organzine' },
]

const YARN_PROFILE_OPTIONS = [
  { value: 'ORGANZINE', label: 'Organzine (High-Twist Warp)' },
  { value: 'TRAM', label: 'Tram (Low-Twist Weft)' },
  { value: 'CREPE', label: 'Crepe (Ultra-High Twist)' },
]

const STEAM_METHOD_OPTIONS = [
  { value: 'VACUUM_AUTOCLAVE', label: 'Vacuum Autoclave' },
  { value: 'MANUAL_STEAM_CHAMBER', label: 'Manual Steam Chamber' },
  { value: 'NATURAL_AGING_ROOM', label: 'Natural Aging Room' },
]

export default function DashboardThrowsterTwister() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [degummingRecords, setDegummingRecords] = useState([])
  const [throwsterBatches, setThrowsterBatches] = useState([])
  const [throwsterRecords, setThrowsterRecords] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [batchForm, setBatchForm] = useState({
    throwster_batch_no: '',
    degumming_record_id: '',
    degumming_batch_id: '',
    zari_lot_batch_id: ''
  })

  const [recordForm, setRecordForm] = useState({
    throwster_batch_id: '',
    input_raw_lot_no: '',
    input_weight_kg: '',
    target_ply_count: 2,
    intended_use: 'WARP_YARN_HIGH_TWIST',
    input_yarn_type: 'BIVOLTINE_WHITE',
    input_lot_purity_clearance: false,
    machinery_id: '',
    target_tpi: '',
    twist_direction: 'S_TWIST',
    steam_setting_duration_mins: '',
    ply_count: 2,
    first_twist_tpm: '',
    final_twist_tpm: '',
    engineered_yarn_profile: 'ORGANZINE',
    spindle_rotational_speed_rpm: '',
    steam_stabilization_method: 'VACUUM_AUTOCLAVE',
    steam_temperature_celsius: '',
    steaming_duration_minutes: '',
    output_twisted_weight_kg: '',
    process_scrap_waste_kg: '',
    tested_tpm_average: '',
    snarl_count_per_100m: 0,
    oil_lubrication_pick_up_pct: ''
  })

  useEffect(() => {
    fetchDegummingRecords()
    fetchThrowsterBatches()
    fetchThrowsterRecords()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

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

  const fetchThrowsterBatches = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/throwster/batches`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setThrowsterBatches(data.batches || [])
    } catch (error) {
      console.error('Failed to fetch throwster batches:', error)
    }
  }

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

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/throwster/certificates`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/throwster`, {
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
      const response = await fetch(`${API_URL}/throwster/batches`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(batchForm)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Throwster batch ${data.throwster_batch_no} created`, 'success')
        setValidationResult({ type: 'success', data })
        setBatchForm({ throwster_batch_no: '', degumming_record_id: '', degumming_batch_id: '', zari_lot_batch_id: '' })
        fetchThrowsterBatches()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create throwster batch', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleRecordSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...recordForm,
        throwster_batch_id: recordForm.throwster_batch_id || null,
        input_weight_kg: recordForm.input_weight_kg ? parseFloat(recordForm.input_weight_kg) : null,
        target_ply_count: recordForm.target_ply_count ? parseInt(recordForm.target_ply_count) : null,
        target_tpi: recordForm.target_tpi ? parseFloat(recordForm.target_tpi) : null,
        ply_count: recordForm.ply_count ? parseInt(recordForm.ply_count) : null,
        first_twist_tpm: recordForm.first_twist_tpm ? parseFloat(recordForm.first_twist_tpm) : null,
        final_twist_tpm: recordForm.final_twist_tpm ? parseFloat(recordForm.final_twist_tpm) : null,
        spindle_rotational_speed_rpm: recordForm.spindle_rotational_speed_rpm ? parseInt(recordForm.spindle_rotational_speed_rpm) : null,
        steam_temperature_celsius: recordForm.steam_temperature_celsius ? parseInt(recordForm.steam_temperature_celsius) : null,
        steaming_duration_minutes: recordForm.steaming_duration_minutes ? parseInt(recordForm.steaming_duration_minutes) : null,
        output_twisted_weight_kg: recordForm.output_twisted_weight_kg ? parseFloat(recordForm.output_twisted_weight_kg) : null,
        process_scrap_waste_kg: recordForm.process_scrap_waste_kg ? parseFloat(recordForm.process_scrap_waste_kg) : null,
        tested_tpm_average: recordForm.tested_tpm_average ? parseFloat(recordForm.tested_tpm_average) : null,
        oil_lubrication_pick_up_pct: recordForm.oil_lubrication_pick_up_pct ? parseFloat(recordForm.oil_lubrication_pick_up_pct) : null
      }

      const response = await fetch(`${API_URL}/throwster/records`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Throwster record created. Twist variation: ${data.twist_variation_pct}%`, 'success')
        setValidationResult({ type: 'success', data })
        setRecordForm({
          throwster_batch_id: '', input_raw_lot_no: '', input_weight_kg: '',
          target_ply_count: 2, intended_use: 'WARP_YARN_HIGH_TWIST',
          input_yarn_type: 'BIVOLTINE_WHITE', input_lot_purity_clearance: false,
          machinery_id: '', target_tpi: '', twist_direction: 'S_TWIST',
          steam_setting_duration_mins: '', ply_count: 2, first_twist_tpm: '',
          final_twist_tpm: '', engineered_yarn_profile: 'ORGANZINE',
          spindle_rotational_speed_rpm: '', steam_stabilization_method: 'VACUUM_AUTOCLAVE',
          steam_temperature_celsius: '', steaming_duration_minutes: '',
          output_twisted_weight_kg: '', process_scrap_waste_kg: '',
          tested_tpm_average: '', snarl_count_per_100m: 0,
          oil_lubrication_pick_up_pct: ''
        })
        fetchThrowsterRecords()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create throwster record', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCertify = async (recordId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/throwster/records/${recordId}/certify`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ notes: 'Approved by Throwster Master' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Throwster certified: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchThrowsterRecords()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify throwster', 'error')
    }
  }

  const handleReject = async (recordId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/throwster/records/${recordId}/reject`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason: 'Rejected by Throwster Master' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Throwster record rejected', 'warning')
        fetchThrowsterRecords()
      } else {
        addNotification(data.error || 'Rejection failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to reject throwster', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'CERTIFIED': return 'success'
      case 'QC_HOLD': return 'warning'
      case 'REJECTED': return 'error'
      case 'IN_PROGRESS': return 'info'
      case 'QUEUED': return 'default'
      case 'DOWNGRADE_TO_1536_HOOK': return 'warning'
      case 'RE_STEAMING_REQUIRED': return 'warning'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'TWISTED_WARP_READY': return 'success'
      case 'TWISTED_WEFT_READY': return 'info'
      case 'TWISTED_CREPE_READY': return 'info'
      case 'COMMERCIAL_SEMI_PREMIUM': return 'info'
      case 'QC_REJECT_HOLD': return 'error'
      case 'DOWNGRADE_TO_1536_HOOK': return 'warning'
      case 'RE_STEAMING_REQUIRED': return 'warning'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Throwster / Twister — Yarn Ply Doubling & Twist Control
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
          {THROWSTER_TABS.map((t) => (
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
          {tab === 'machine-setup' && 'Throwster record created'}
          {tab === 'post-twist' && 'Post-twist quality updated'}
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Hash:</strong> {certificateDetail.certificate_hash}</Typography>
          <Typography variant="body2"><strong>QR Tag:</strong> {certificateDetail.qr_tag_id}</Typography>
          <Typography variant="body2"><strong>Routing:</strong> {certificateDetail.auto_assigned_routing}</Typography>
          <Typography variant="body2"><strong>Twist Variation:</strong> {certificateDetail.twist_variation_pct}%</Typography>
        </Alert>
      )}

      {/* ===================== PRE-PROCESS TAB ===================== */}
      {tab === 'preprocess' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Degumming Master Output (Pre-Process)</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Record ID</TableCell>
                      <TableCell>Batch No</TableCell>
                      <TableCell>Sericin Loss %</TableCell>
                      <TableCell>Tenacity (g/d)</TableCell>
                      <TableCell>Fibrillation</TableCell>
                      <TableCell>Routing</TableCell>
                      <TableCell>Status</TableCell>
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
                        <TableCell><Chip label={record.auto_assigned_routing} color={getRoutingColor(record.auto_assigned_routing)} size="small" /></TableCell>
                        <TableCell><Chip label={record.status} color={getStatusColor(record.status)} size="small" /></TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Create Throwster Batch</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Throwster Batch No" value={batchForm.throwster_batch_no}
                    onChange={(e) => setBatchForm({ ...batchForm, throwster_batch_no: e.target.value })}
                    placeholder="e.g., TW-2024-001" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Degumming Record</InputLabel>
                    <Select value={batchForm.degumming_record_id} label="Degumming Record"
                      onChange={(e) => setBatchForm({ ...batchForm, degumming_record_id: e.target.value })}>
                      <MenuItem value="">Select record</MenuItem>
                      {degummingRecords.map((record) => (
                        <MenuItem key={record.id} value={record.id}>{record.degumming_batch_no} — Sericin: {record.sericin_loss_pct}%</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleBatchSubmit} disabled={submitting}>
                    {submitting ? 'Creating...' : 'Create Throwster Batch'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Throwster Batches</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Batch No</TableCell>
                      <TableCell>Degumming Batch</TableCell>
                      <TableCell>Status</TableCell>
                      <TableCell>Created</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {throwsterBatches.map((batch) => (
                      <TableRow key={batch.id}>
                        <TableCell>{batch.throwster_batch_no}</TableCell>
                        <TableCell>{batch.degumming_batch_no}</TableCell>
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

      {/* ===================== MACHINE & PROCESS SETUP TAB ===================== */}
      {tab === 'machine-setup' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Category A: Machine & Process Setup + Category B: Mechanical Structure</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Throwster Batch</InputLabel>
                    <Select value={recordForm.throwster_batch_id} label="Throwster Batch"
                      onChange={(e) => setRecordForm({ ...recordForm, throwster_batch_id: e.target.value })}>
                      <MenuItem value="">Select batch</MenuItem>
                      {throwsterBatches.map((batch) => (
                        <MenuItem key={batch.id} value={batch.id}>{batch.throwster_batch_no}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Input Raw Lot No" value={recordForm.input_raw_lot_no}
                    onChange={(e) => setRecordForm({ ...recordForm, input_raw_lot_no: e.target.value })}
                    placeholder="Filature lot number" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Input Weight (kg)" type="number"
                    value={recordForm.input_weight_kg}
                    onChange={(e) => setRecordForm({ ...recordForm, input_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Target Ply Count</InputLabel>
                    <Select value={recordForm.target_ply_count} label="Target Ply Count"
                      onChange={(e) => setRecordForm({ ...recordForm, target_ply_count: parseInt(e.target.value) })}>
                      {PLY_COUNT_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Intended Use</InputLabel>
                    <Select value={recordForm.intended_use} label="Intended Use"
                      onChange={(e) => setRecordForm({ ...recordForm, intended_use: e.target.value })}>
                      {INTENDED_USE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Input Yarn Type</InputLabel>
                    <Select value={recordForm.input_yarn_type} label="Input Yarn Type"
                      onChange={(e) => setRecordForm({ ...recordForm, input_yarn_type: e.target.value })}>
                      {YARN_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Ply Count</InputLabel>
                    <Select value={recordForm.ply_count} label="Ply Count"
                      onChange={(e) => setRecordForm({ ...recordForm, ply_count: parseInt(e.target.value) })}>
                      {PLY_COUNT_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Engineered Yarn Profile</InputLabel>
                    <Select value={recordForm.engineered_yarn_profile} label="Engineered Yarn Profile"
                      onChange={(e) => setRecordForm({ ...recordForm, engineered_yarn_profile: e.target.value })}>
                      {YARN_PROFILE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Machinery ID" value={recordForm.machinery_id}
                    onChange={(e) => setRecordForm({ ...recordForm, machinery_id: e.target.value })}
                    placeholder="Twisting machine ID" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Target TPI" type="number"
                    value={recordForm.target_tpi}
                    onChange={(e) => setRecordForm({ ...recordForm, target_tpi: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} helperText="Turns per inch" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Twist Direction</InputLabel>
                    <Select value={recordForm.twist_direction} label="Twist Direction"
                      onChange={(e) => setRecordForm({ ...recordForm, twist_direction: e.target.value })}>
                      {TWIST_DIRECTION_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="First Twist TPM" type="number"
                    value={recordForm.first_twist_tpm}
                    onChange={(e) => setRecordForm({ ...recordForm, first_twist_tpm: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="300-600 TPM" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Final Twist TPM" type="number"
                    value={recordForm.final_twist_tpm}
                    onChange={(e) => setRecordForm({ ...recordForm, final_twist_tpm: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="500-800 TPM" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Spindle Speed (RPM)" type="number"
                    value={recordForm.spindle_rotational_speed_rpm}
                    onChange={(e) => setRecordForm({ ...recordForm, spindle_rotational_speed_rpm: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="9,000-12,000 RPM" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Steam Stabilization Method</InputLabel>
                    <Select value={recordForm.steam_stabilization_method} label="Steam Stabilization Method"
                      onChange={(e) => setRecordForm({ ...recordForm, steam_stabilization_method: e.target.value })}>
                      {STEAM_METHOD_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Steam Temperature (°C)" type="number"
                    value={recordForm.steam_temperature_celsius}
                    onChange={(e) => setRecordForm({ ...recordForm, steam_temperature_celsius: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="Target: 70-85°C" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Steaming Duration (minutes)" type="number"
                    value={recordForm.steaming_duration_minutes}
                    onChange={(e) => setRecordForm({ ...recordForm, steaming_duration_minutes: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="e.g., 45 minutes" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Steam Setting Duration (mins)" type="number"
                    value={recordForm.steam_setting_duration_mins}
                    onChange={(e) => setRecordForm({ ...recordForm, steam_setting_duration_mins: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControlLabel
                    control={
                      <Checkbox
                        checked={recordForm.input_lot_purity_clearance}
                        onChange={(e) => setRecordForm({ ...recordForm, input_lot_purity_clearance: e.target.checked })}
                      />
                    }
                    label="Input Lot Purity Clearance (from Silk Grader)"
                  />
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleRecordSubmit} disabled={submitting}>
                    {submitting ? 'Submitting...' : 'Save Machine Setup & Create Record'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== POST-TWIST QUALITY TAB ===================== */}
      {tab === 'post-twist' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Category C: Output Materials & Waste + Category B: Post-Twist Quality</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Output Twisted Weight (kg)" type="number"
                    value={recordForm.output_twisted_weight_kg}
                    onChange={(e) => setRecordForm({ ...recordForm, output_twisted_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} helperText="Final twisted yarn weight" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Process Scrap Waste (kg)" type="number"
                    value={recordForm.process_scrap_waste_kg}
                    onChange={(e) => setRecordForm({ ...recordForm, process_scrap_waste_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} helperText="Broken ends, floor sweepings" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Tested TPM Average" type="number"
                    value={recordForm.tested_tpm_average}
                    onChange={(e) => setRecordForm({ ...recordForm, tested_tpm_average: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} helperText="Lab twist counter reading" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Snarl Count per 100m" type="number"
                    value={recordForm.snarl_count_per_100m}
                    onChange={(e) => setRecordForm({ ...recordForm, snarl_count_per_100m: parseInt(e.target.value) || 0 })}
                    inputProps={{ step: '1', min: 0 }} helperText="Target: 0" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Oil Lubrication Pick Up %" type="number"
                    value={recordForm.oil_lubrication_pick_up_pct}
                    onChange={(e) => setRecordForm({ ...recordForm, oil_lubrication_pick_up_pct: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} helperText="Target: 1.5-2.5%" />
                </Grid>
                <Grid item xs={12}>
                  <Alert severity="info" sx={{ mb: 2 }}>
                    <Typography variant="subtitle2">Automated ERP Routing & Validation Rules</Typography>
                    <Typography variant="body2">
                      • 2400 Hook + Final TPM < 700 → DOWNGRADE_TO_1536_HOOK (INSUFFICIENT_TWIST_FOR_HIGH_HARNESS_FRICTION)<br/>
                      • Twist Variation > 3.0% → WARP_BANDING_RISK warning<br/>
                      • Snarl Count > 0 → RE_STEAMING_REQUIRED (ACTIVE_TORQUE_WILL_CAUSE_LOOM_STOPS)<br/>
                      • Material Discrepancy > 1.5% → MATERIAL_VARIANCE_ALERT warning
                    </Typography>
                  </Alert>
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleRecordSubmit} disabled={submitting}>
                    {submitting ? 'Saving...' : 'Save Post-Twist Quality Record'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Throwster Production Records</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Record ID</TableCell>
                      <TableCell>Batch No</TableCell>
                      <TableCell>Input Lot</TableCell>
                      <TableCell>Ply</TableCell>
                      <TableCell>Final TPM</TableCell>
                      <TableCell>Twist Var %</TableCell>
                      <TableCell>Snarls</TableCell>
                      <TableCell>Routing</TableCell>
                      <TableCell>Status</TableCell>
                      <TableCell>Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {throwsterRecords.map((record) => (
                      <TableRow key={record.id}>
                        <TableCell>{record.id}</TableCell>
                        <TableCell>{record.throwster_batch_no}</TableCell>
                        <TableCell>{record.input_raw_lot_no}</TableCell>
                        <TableCell>{record.ply_count}</TableCell>
                        <TableCell>{record.final_twist_tpm}</TableCell>
                        <TableCell>{record.twist_variation_pct}%</TableCell>
                        <TableCell>{record.snarl_count_per_100m}</TableCell>
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
              <Typography variant="h6" gutterBottom>Throwster Certificates — Post-Process Output</Typography>
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
                        <TableCell>Input Lot</TableCell>
                        <TableCell>Ply</TableCell>
                        <TableCell>Final TPM</TableCell>
                        <TableCell>Twist Var %</TableCell>
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
                          <TableCell>{cert.throwster_batch_no}</TableCell>
                          <TableCell>{cert.input_raw_lot_no}</TableCell>
                          <TableCell>{cert.ply_count}</TableCell>
                          <TableCell>{cert.final_twist_tpm}</TableCell>
                          <TableCell>{cert.twist_variation_pct}%</TableCell>
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
              <Typography variant="h6" gutterBottom>Sales Forecast — Throwster Material Processing Plan</Typography>
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
                          <TableCell>Yarn Profile</TableCell>
                          <TableCell>Ply</TableCell>
                          <TableCell>Final TPM</TableCell>
                          <TableCell>Est. Raw Silk (kg)</TableCell>
                          <TableCell>Priority</TableCell>
                          <TableCell>Target Machine</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.material_requirements.map((item, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{item.saree_category}</TableCell>
                            <TableCell>{item.yarn_profile}</TableCell>
                            <TableCell>{item.ply_count}</TableCell>
                            <TableCell>{item.final_twist_tpm}</TableCell>
                            <TableCell>{item.estimated_raw_silk_kg}</TableCell>
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
                          <TableCell>Yarn Profile</TableCell>
                          <TableCell>Est. Sarees</TableCell>
                          <TableCell>Est. Raw Silk (kg)</TableCell>
                          <TableCell>Final TPM</TableCell>
                          <TableCell>Target Machine</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.upcoming_lots.map((lot, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{lot.lot_number}</TableCell>
                            <TableCell>{lot.saree_category}</TableCell>
                            <TableCell>{lot.yarn_profile}</TableCell>
                            <TableCell>{lot.estimated_sarees}</TableCell>
                            <TableCell>{lot.estimated_raw_silk_kg}</TableCell>
                            <TableCell>{lot.final_twist_tpm}</TableCell>
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
