import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider, Checkbox, FormControlLabel } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const ZARI_TABS = [
  { id: 'lot-batch', label: 'Zari Lot Batch — Pre-Process Entry' },
  { id: 'assay', label: 'Metallurgical & Assay — Post-Process' },
  { id: 'quality-gate', label: 'Quality Gate — Physical Toggle' },
  { id: 'certificates', label: 'Certificates — Post-Process Output' },
  { id: 'sales-forecast', label: 'Sales Forecast — Material Plan' },
]

const ZARI_TYPE_OPTIONS = [
  { value: 'PURE_REAL_ZARI_GOLD_SILVER', label: 'Pure / Real Zari (Gold & Silver)' },
  { value: 'TESTED_HALF_FINE_ZARI_COPPER_CORE', label: 'Tested / Half-Fine Zari (Copper-Core)' },
  { value: 'IMITATION_METALLIC_ZARI', label: 'Imitation / Metallic Zari (Polyester Film)' },
]

const ORIGIN_CLUSTER_OPTIONS = [
  { value: 'SURAT', label: 'Surat' },
  { value: 'KANCHIPURAM', label: 'Kanchipuram' },
  { value: 'DHARMAVARAM', label: 'Dharmavaram' },
  { value: 'BANARAS', label: 'Banaras' },
  { value: 'MYSORE', label: 'Mysore' },
  { value: 'COCHIN', label: 'Cochin' },
  { value: 'KOLKATA', label: 'Kolkata' },
  { value: 'AHMEDABAD', label: 'Ahmedabad' },
]

const CORE_YARN_OPTIONS = [
  { value: 'PURE_SILK_THREAD_RED_DYED', label: 'Pure Silk Thread (Red Dyed)' },
  { value: 'PURE_SILK_THREAD_YELLOW_DYED', label: 'Pure Silk Thread (Yellow Dyed)' },
  { value: 'PURE_COTTON_THREAD', label: 'Pure Cotton Thread' },
  { value: 'POLYESTER_FILAMENT', label: 'Polyester Filament' },
  { value: 'NYLON_FILAMENT', label: 'Nylon Filament' },
]

const DENIER_OPTIONS = [
  { value: '1200_YARDS_PER_OUNCE', label: '1200 Yards/Ounce' },
  { value: '1300_YARDS_PER_OUNCE', label: '1300 Yards/Ounce' },
  { value: '1400_YARDS_PER_OUNCE', label: '1400 Yards/Ounce' },
  { value: '1500_YARDS_PER_OUNCE', label: '1500 Yards/Ounce' },
]

const BOBBIN_TYPE_OPTIONS = [
  { value: 'FLANGED_BOBBIN', label: 'Flanged Bobbin' },
  { value: 'PAPER_CONE', label: 'Paper Cone' },
  { value: 'PLASTIC_SPOOL', label: 'Plastic Spool' },
]

export default function DashboardZariInspector() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [tab, setTab] = useState('lot-batch')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [lotBatches, setLotBatches] = useState([])
  const [assays, setAssays] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [lotForm, setLotForm] = useState({
    zari_lot_batch_no: '',
    zari_type: 'PURE_REAL_ZARI_GOLD_SILVER',
    zari_origin_cluster: 'KANCHIPURAM',
    saree_bundle_size: 80
  })

  const [assayForm, setAssayForm] = useState({
    zari_lot_batch_id: '',
    assay_certificate_no: '',
    silver_purity_pct: '',
    gold_plating_pct: '',
    copper_base_pct: '',
    core_yarn_material: 'PURE_SILK_THREAD_RED_DYED',
    zari_count_denier: '1400_YARDS_PER_OUNCE',
    zari_wire_diameter_microns: '',
    winding_bobbin_type: 'FLANGED_BOBBIN',
    invoice_declared_weight_gm: '',
    gross_scale_weight_gm: '',
    bobbin_tare_weight_gm: '',
    precious_metal_market_rate_per_gm: '',
    is_free_from_tarnishing: false,
    is_free_from_wire_cuts: false,
    luster_sheen_match: false
  })

  const [qualityForm, setQualityForm] = useState({
    assay_id: '',
    is_free_from_tarnishing: false,
    is_free_from_wire_cuts: false,
    luster_sheen_match: false,
    notes: ''
  })

  useEffect(() => {
    fetchLotBatches()
    fetchAssays()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchLotBatches = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/zari/lot-batches`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setLotBatches(data.batches || [])
    } catch (error) {
      console.error('Failed to fetch lot batches:', error)
    }
  }

  const fetchAssays = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/zari/assay`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setAssays(data.assays || [])
    } catch (error) {
      console.error('Failed to fetch assays:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/zari/certificates`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/zari`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setForecast(data)
    } catch (error) {
      console.error('Failed to fetch sales forecast:', error)
    }
  }

  const handleLotBatchSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/zari/lot-batches`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(lotForm)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Zari lot batch ${data.zari_lot_batch_no} created`, 'success')
        setValidationResult({ type: 'success', data })
        setLotForm({ zari_lot_batch_no: '', zari_type: 'PURE_REAL_ZARI_GOLD_SILVER', zari_origin_cluster: 'KANCHIPURAM', saree_bundle_size: 80 })
        fetchLotBatches()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create Zari lot batch', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleAssaySubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...assayForm,
        zari_lot_batch_id: assayForm.zari_lot_batch_id || null,
        silver_purity_pct: assayForm.silver_purity_pct ? parseFloat(assayForm.silver_purity_pct) : null,
        gold_plating_pct: assayForm.gold_plating_pct ? parseFloat(assayForm.gold_plating_pct) : null,
        copper_base_pct: assayForm.copper_base_pct ? parseFloat(assayForm.copper_base_pct) : null,
        zari_wire_diameter_microns: assayForm.zari_wire_diameter_microns ? parseFloat(assayForm.zari_wire_diameter_microns) : null,
        invoice_declared_weight_gm: assayForm.invoice_declared_weight_gm ? parseFloat(assayForm.invoice_declared_weight_gm) : null,
        gross_scale_weight_gm: assayForm.gross_scale_weight_gm ? parseFloat(assayForm.gross_scale_weight_gm) : null,
        bobbin_tare_weight_gm: assayForm.bobbin_tare_weight_gm ? parseFloat(assayForm.bobbin_tare_weight_gm) : null,
        precious_metal_market_rate_per_gm: assayForm.precious_metal_market_rate_per_gm ? parseFloat(assayForm.precious_metal_market_rate_per_gm) : null
      }

      const response = await fetch(`${API_URL}/zari/assay`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Zari assay ${data.assay_certificate_no} created`, 'success')
        setValidationResult({ type: 'success', data })
        setAssayForm({
          zari_lot_batch_id: '', assay_certificate_no: '', silver_purity_pct: '',
          gold_plating_pct: '', copper_base_pct: '', core_yarn_material: 'PURE_SILK_THREAD_RED_DYED',
          zari_count_denier: '1400_YARDS_PER_OUNCE', zari_wire_diameter_microns: '',
          winding_bobbin_type: 'FLANGED_BOBBIN', invoice_declared_weight_gm: '',
          gross_scale_weight_gm: '', bobbin_tare_weight_gm: '',
          precious_metal_market_rate_per_gm: '', is_free_from_tarnishing: false,
          is_free_from_wire_cuts: false, luster_sheen_match: false
        })
        fetchAssays()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create Zari assay', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCertifyAssay = async (assayId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/zari/assay/${assayId}/certify`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ notes: 'Approved by Zari Inspector' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Assay certified: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchAssays()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify assay', 'error')
    }
  }

  const handleRejectAssay = async (assayId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/zari/assay/${assayId}/reject`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason: 'Rejected by Zari Inspector' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Assay rejected', 'warning')
        fetchAssays()
      } else {
        addNotification(data.error || 'Rejection failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to reject assay', 'error')
    }
  }

  const handleQualityGateSave = async () => {
    if (!qualityForm.assay_id) {
      addNotification('Please select an assay', 'error')
      return
    }
    setSubmitting(true)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/zari/assay/${qualityForm.assay_id}/quality-gate`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          is_free_from_tarnishing: qualityForm.is_free_from_tarnishing,
          is_free_from_wire_cuts: qualityForm.is_free_from_wire_cuts,
          luster_sheen_match: qualityForm.luster_sheen_match
        })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Quality gate inspection saved', 'success')
        setValidationResult({ type: 'success', data })
        setQualityForm({ assay_id: '', is_free_from_tarnishing: false, is_free_from_wire_cuts: false, luster_sheen_match: false, notes: '' })
        fetchAssays()
      } else {
        addNotification(data.error || 'Failed to save quality gate', 'error')
      }
    } catch (error) {
      addNotification('Failed to save quality gate inspection', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'CERTIFIED': return 'success'
      case 'QC_HOLD': return 'warning'
      case 'REJECTED': return 'error'
      case 'ASSAY_IN_PROGRESS': return 'info'
      case 'SUBMITTED': return 'info'
      case 'OPEN': return 'default'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Zari Refinery — Inward & Quality Screen
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
      </Box>

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="fullWidth">
          {ZARI_TABS.map((t) => (
            <Tab key={t.id} label={t.label} value={t.id} />
          ))}
        </Tabs>
      </Paper>

      {validationResult && validationResult.type === 'error' && validationResult.data.validation_errors && (
        <Alert severity="error" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Metallurgical Guardrail Violations</Typography>
          {validationResult.data.validation_errors.map((err, idx) => (
            <Typography key={idx} variant="body2">• [{err.code}] {err.message}</Typography>
          ))}
        </Alert>
      )}

      {validationResult && validationResult.type === 'success' && (
        <Alert severity="success" sx={{ mb: 3 }}>
          {tab === 'lot-batch' && `Zari lot batch ${validationResult.data.zari_lot_batch_no} created`}
          {tab === 'assay' && `Zari assay ${validationResult.data.assay_certificate_no} created`}
          {tab === 'quality-gate' && `Quality gate action completed: ${validationResult.data.status}`}
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Hash:</strong> {certificateDetail.certificate_hash}</Typography>
          <Typography variant="body2"><strong>QR Tag:</strong> {certificateDetail.qr_tag_id}</Typography>
          <Typography variant="body2"><strong>Status:</strong> {certificateDetail.status}</Typography>
          {certificateDetail.precious_metal_value_estimate && (
            <Typography variant="body2"><strong>Precious Metal Value:</strong> ₹{certificateDetail.precious_metal_value_estimate}</Typography>
          )}
        </Alert>
      )}

      {/* ===================== LOT BATCH TAB ===================== */}
      {tab === 'lot-batch' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Category 1: Batch & Traceability Intakes (Pre-Process from Filature Supplier)</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth label="Zari Lot Batch No" value={lotForm.zari_lot_batch_no}
                    onChange={(e) => setLotForm({ ...lotForm, zari_lot_batch_no: e.target.value })}
                    placeholder="Unique tracking ID from refinery"
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Zari Type</InputLabel>
                    <Select value={lotForm.zari_type} label="Zari Type"
                      onChange={(e) => setLotForm({ ...lotForm, zari_type: e.target.value })}>
                      {ZARI_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Zari Origin Cluster</InputLabel>
                    <Select value={lotForm.zari_origin_cluster} label="Zari Origin Cluster"
                      onChange={(e) => setLotForm({ ...lotForm, zari_origin_cluster: e.target.value })}>
                      {ORIGIN_CLUSTER_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Saree Bundle Size" type="number"
                    value={lotForm.saree_bundle_size}
                    onChange={(e) => setLotForm({ ...lotForm, saree_bundle_size: parseInt(e.target.value) || 80 })}
                    inputProps={{ min: 1 }} helperText="Default: 80 sarees per lot" />
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleLotBatchSubmit} disabled={submitting}>
                    {submitting ? 'Creating...' : 'Create Zari Lot Batch'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Zari Lot Batches</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Lot Batch No</TableCell>
                      <TableCell>Zari Type</TableCell>
                      <TableCell>Origin Cluster</TableCell>
                      <TableCell>Bundle Size</TableCell>
                      <TableCell>Status</TableCell>
                      <TableCell>Created</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {lotBatches.map((batch) => (
                      <TableRow key={batch.id}>
                        <TableCell>{batch.zari_lot_batch_no}</TableCell>
                        <TableCell>{batch.zari_type}</TableCell>
                        <TableCell>{batch.zari_origin_cluster}</TableCell>
                        <TableCell>{batch.saree_bundle_size}</TableCell>
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

      {/* ===================== ASSAY TAB ===================== */}
      {tab === 'assay' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Category 2, 3, 4: Metallurgical & Assay Intakes</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Zari Lot Batch</InputLabel>
                    <Select value={assayForm.zari_lot_batch_id} label="Zari Lot Batch"
                      onChange={(e) => setAssayForm({ ...assayForm, zari_lot_batch_id: e.target.value })}>
                      <MenuItem value="">Select batch</MenuItem>
                      {lotBatches.map((batch) => (
                        <MenuItem key={batch.id} value={batch.id}>{batch.zari_lot_batch_no} — {batch.zari_type}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Assay Certificate No" value={assayForm.assay_certificate_no}
                    onChange={(e) => setAssayForm({ ...assayForm, assay_certificate_no: e.target.value })}
                    placeholder="XRF / Chemical assay certificate ID" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Silver Purity %" type="number"
                    value={assayForm.silver_purity_pct}
                    onChange={(e) => setAssayForm({ ...assayForm, silver_purity_pct: e.target.value })}
                    inputProps={{ step: '0.01', min: 0, max: 100 }} helperText="Luxury standard: 55% to 57%" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Gold Plating %" type="number"
                    value={assayForm.gold_plating_pct}
                    onChange={(e) => setAssayForm({ ...assayForm, gold_plating_pct: e.target.value })}
                    inputProps={{ step: '0.01', min: 0, max: 100 }} helperText="Luxury standard: 0.5% to 1.0%" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Copper Base %" type="number"
                    value={assayForm.copper_base_pct}
                    onChange={(e) => setAssayForm({ ...assayForm, copper_base_pct: e.target.value })}
                    inputProps={{ step: '0.01', min: 0, max: 100 }} helperText="For Tested Zari tracking" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Core Yarn Material</InputLabel>
                    <Select value={assayForm.core_yarn_material} label="Core Yarn Material"
                      onChange={(e) => setAssayForm({ ...assayForm, core_yarn_material: e.target.value })}>
                      {CORE_YARN_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Zari Count (Denier)</InputLabel>
                    <Select value={assayForm.zari_count_denier} label="Zari Count (Denier)"
                      onChange={(e) => setAssayForm({ ...assayForm, zari_count_denier: e.target.value })}>
                      {DENIER_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Wire Diameter (Microns)" type="number"
                    value={assayForm.zari_wire_diameter_microns}
                    onChange={(e) => setAssayForm({ ...assayForm, zari_wire_diameter_microns: e.target.value })}
                    inputProps={{ step: '0.1', min: 0, max: 100 }} helperText="Standard: 20 to 30 microns" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Winding Bobbin Type</InputLabel>
                    <Select value={assayForm.winding_bobbin_type} label="Winding Bobbin Type"
                      onChange={(e) => setAssayForm({ ...assayForm, winding_bobbin_type: e.target.value })}>
                      {BOBBIN_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Invoice Declared Weight (gm)" type="number"
                    value={assayForm.invoice_declared_weight_gm}
                    onChange={(e) => setAssayForm({ ...assayForm, invoice_declared_weight_gm: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Gross Scale Weight (gm)" type="number"
                    value={assayForm.gross_scale_weight_gm}
                    onChange={(e) => setAssayForm({ ...assayForm, gross_scale_weight_gm: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Bobbin Tare Weight (gm)" type="number"
                    value={assayForm.bobbin_tare_weight_gm}
                    onChange={(e) => setAssayForm({ ...assayForm, bobbin_tare_weight_gm: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }} helperText="Net weight = Gross - Tare" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Precious Metal Market Rate (₹/gm)" type="number"
                    value={assayForm.precious_metal_market_rate_per_gm}
                    onChange={(e) => setAssayForm({ ...assayForm, precious_metal_market_rate_per_gm: e.target.value })}
                    inputProps={{ step: '0.01', min: 0 }} helperText="Spot price on invoice day" />
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleAssaySubmit} disabled={submitting}>
                    {submitting ? 'Submitting...' : 'Submit Zari Assay'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Zari Assays</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Assay Cert No</TableCell>
                      <TableCell>Lot Batch</TableCell>
                      <TableCell>Zari Type</TableCell>
                      <TableCell>Silver %</TableCell>
                      <TableCell>Gold %</TableCell>
                      <TableCell>Net Wt (gm)</TableCell>
                      <TableCell>Status</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {assays.map((assay) => (
                      <TableRow key={assay.id}>
                        <TableCell>{assay.assay_certificate_no}</TableCell>
                        <TableCell>{assay.zari_lot_batch_no}</TableCell>
                        <TableCell>{assay.zari_type}</TableCell>
                        <TableCell>{assay.silver_purity_pct}%</TableCell>
                        <TableCell>{assay.gold_plating_pct}%</TableCell>
                        <TableCell>{assay.net_zari_weight_gm}</TableCell>
                        <TableCell><Chip label={assay.status} color={getStatusColor(assay.status)} size="small" /></TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== QUALITY GATE TAB ===================== */}
      {tab === 'quality-gate' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Category 5: Physical Gate-Keeper Quality Toggles</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Select Assay</InputLabel>
                    <Select value={qualityForm.assay_id} label="Select Assay"
                      onChange={(e) => setQualityForm({ ...qualityForm, assay_id: e.target.value })}>
                      <MenuItem value="">Select assay</MenuItem>
                      {assays.map((assay) => (
                        <MenuItem key={assay.id} value={assay.id}>{assay.assay_certificate_no} — {assay.zari_lot_batch_no}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12}>
                  <Typography variant="subtitle1" gutterBottom sx={{ mt: 2 }}>Binary Pass/Fail Inspections</Typography>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControlLabel
                    control={
                      <Checkbox
                        checked={qualityForm.is_free_from_tarnishing}
                        onChange={(e) => setQualityForm({ ...qualityForm, is_free_from_tarnishing: e.target.checked })}
                      />
                    }
                    label="Is Free From Tarnishing? (No dark/black oxidation under 5000K lamp)"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControlLabel
                    control={
                      <Checkbox
                        checked={qualityForm.is_free_from_wire_cuts}
                        onChange={(e) => setQualityForm({ ...qualityForm, is_free_from_wire_cuts: e.target.checked })}
                      />
                    }
                    label="Is Free From Wire Cuts? (Zero continuous knots per bobbin)"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControlLabel
                    control={
                      <Checkbox
                        checked={qualityForm.luster_sheen_match}
                        onChange={(e) => setQualityForm({ ...qualityForm, luster_sheen_match: e.target.checked })}
                      />
                    }
                    label="Luster Sheen Match? (Delta-E < 1.0 against master sample)"
                  />
                </Grid>
                <Grid item xs={12}>
                  <TextField
                    fullWidth label="Inspector Notes" multiline rows={3}
                    value={qualityForm.notes}
                    onChange={(e) => setQualityForm({ ...qualityForm, notes: e.target.value })}
                  />
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleQualityGateSave} disabled={submitting}>
                    {submitting ? 'Saving...' : 'Save Quality Gate Inspection'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== CERTIFICATES TAB ===================== */}
      {tab === 'certificates' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Zari Certificates — Post-Process Output</Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates issued yet</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag</TableCell>
                        <TableCell>Lot Batch</TableCell>
                        <TableCell>Zari Type</TableCell>
                        <TableCell>Origin</TableCell>
                        <TableCell>Silver %</TableCell>
                        <TableCell>Gold %</TableCell>
                        <TableCell>Net Wt (gm)</TableCell>
                        <TableCell>Precious Value (₹)</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell>Certified At</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {certificates.map((cert) => (
                        <TableRow key={cert.id}>
                          <TableCell>{cert.certificate_hash}</TableCell>
                          <TableCell>{cert.qr_tag_id}</TableCell>
                          <TableCell>{cert.zari_lot_batch_no}</TableCell>
                          <TableCell>{cert.zari_type}</TableCell>
                          <TableCell>{cert.zari_origin_cluster}</TableCell>
                          <TableCell>{cert.silver_purity_pct}%</TableCell>
                          <TableCell>{cert.gold_plating_pct}%</TableCell>
                          <TableCell>{cert.net_zari_weight_gm}</TableCell>
                          <TableCell>{cert.precious_metal_value_estimate ? parseFloat(cert.precious_metal_value_estimate).toFixed(2) : '-'}</TableCell>
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
        </Grid>
      )}

      {/* ===================== SALES FORECAST TAB ===================== */}
      {tab === 'sales-forecast' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Sales Forecast — Zari Material Processing Plan</Typography>
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
                          <TableCell>Zari Type</TableCell>
                          <TableCell>Grade</TableCell>
                          <TableCell>Est. Zari Wt (gm)</TableCell>
                          <TableCell>Est. Silver (gm)</TableCell>
                          <TableCell>Est. Gold (gm)</TableCell>
                          <TableCell>Priority</TableCell>
                          <TableCell>Origin</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.material_requirements.map((item, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{item.sari_category}</TableCell>
                            <TableCell>{item.zari_type}</TableCell>
                            <TableCell>{item.zari_grade}</TableCell>
                            <TableCell>{item.estimated_zari_weight_gm}</TableCell>
                            <TableCell>{item.estimated_silver_gm}</TableCell>
                            <TableCell>{item.estimated_gold_gm}</TableCell>
                            <TableCell><Chip label={item.priority} color={item.priority === 'HIGH' ? 'error' : 'warning'} size="small" /></TableCell>
                            <TableCell>{item.origin_cluster}</TableCell>
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
                          <TableCell>Est. Zari Wt (gm)</TableCell>
                          <TableCell>Target Grade</TableCell>
                          <TableCell>Target Origin</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.upcoming_lots.map((lot, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{lot.lot_number}</TableCell>
                            <TableCell>{lot.sari_category}</TableCell>
                            <TableCell>{lot.estimated_sarees}</TableCell>
                            <TableCell>{lot.estimated_zari_weight_gm}</TableCell>
                            <TableCell>{lot.target_grade}</TableCell>
                            <TableCell>{lot.target_origin}</TableCell>
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
