import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const QA_TABS = [
  { id: 'gate-clerk', label: 'Gate Clerk — Inward Entry' },
  { id: 'qc-inspector', label: 'QC Inspector — Lab Intake' },
  { id: 'quality-manager', label: 'Quality Manager — Approvals' },
  { id: 'sales-forecast', label: 'Sales Forecast — Material Plan' },
]

const SILK_TYPE_OPTIONS = [
  { value: 'BIVOLTINE_WHITE_SILK', label: 'Bivoltine (White Silk)' },
  { value: 'MULTIVOLTINE_YELLOW_SILK', label: 'Multivoltine (Yellow Silk)' },
  { value: 'TUSSAR_WILD_SILK', label: 'Tasar (Wild Silk)' },
  { value: 'MUGA_GOLDEN_SILK', label: 'Muga (Golden Silk)' },
  { value: 'ERI_SPUN_SILK', label: 'Eri (Spun Silk)' },
]

const MACHINERY_OPTIONS = [
  { value: 'ARM_AUTOMATIC_REELING', label: 'ARM (Automatic Reeling)' },
  { value: 'MRM_MULTI_END_REELING', label: 'MRM (Multi-End Reeling)' },
]

const GRADE_OPTIONS = ['6A', '5A', '4A', '3A', '2A', 'A', 'B', 'C', 'D', 'Reject']

const DENIER_OPTIONS = [
  { value: '13/15 Denier', label: '13/15 Denier — Ultra-fine' },
  { value: '16/18 Denier', label: '16/18 Denier — Fine' },
  { value: '20/22 Denier', label: '20/22 Denier — Industry Standard' },
  { value: '24/26 Denier', label: '24/26 Denier — Medium-heavy' },
  { value: '28/30 Denier', label: '28/30 Denier — Heavy' },
]

export default function DashboardFilatureSupplier() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [tab, setTab] = useState('gate-clerk')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [gateEntries, setGateEntries] = useState([])
  const [qualityIntakes, setQualityIntakes] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [gateForm, setGateForm] = useState({
    supplier_id: '',
    invoice_number: '',
    invoice_gross_weight_kg: '',
    actual_scale_weight_kg: '',
    filature_lot_number: ''
  })

  const [qcForm, setQcForm] = useState({
    inward_gate_entry_id: '',
    silk_type: 'BIVOLTINE_WHITE_SILK',
    machinery_source: 'ARM_AUTOMATIC_REELING',
    silk_mark_tag_id: '',
    lab_report_number: '',
    certified_grade: '4A',
    size_deviation_pct: '',
    evenness_pct: '',
    cleanness_pct: '',
    neatness_pct: '',
    tenacity_gd: '',
    cohesion_strokes: '',
    live_moisture_reading_pct: '',
    has_machine_oil_stains: false,
    has_mixed_dye_lots: false,
    sample_hank_weight_g: ''
  })

  useEffect(() => {
    fetchGateEntries()
    fetchQualityIntakes()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchGateEntries = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/inward-gate/entries`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setGateEntries(data.entries || [])
    } catch (error) {
      console.error('Failed to fetch gate entries:', error)
    }
  }

  const fetchQualityIntakes = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/quality/intake`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setQualityIntakes(data.intakes || [])
    } catch (error) {
      console.error('Failed to fetch quality intakes:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/quality/certificates`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/material`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setForecast(data)
    } catch (error) {
      console.error('Failed to fetch sales forecast:', error)
    }
  }

  const handleGateSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/inward-gate/entries`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          ...gateForm,
          supplier_id: gateForm.supplier_id || null,
          invoice_gross_weight_kg: parseFloat(gateForm.invoice_gross_weight_kg) || 0,
          actual_scale_weight_kg: parseFloat(gateForm.actual_scale_weight_kg) || 0
        })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Gate entry ${data.inward_gate_entry_no} created`, 'success')
        setValidationResult({ type: 'success', data })
        setGateForm({ supplier_id: '', invoice_number: '', invoice_gross_weight_kg: '', actual_scale_weight_kg: '', filature_lot_number: '' })
        fetchGateEntries()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create gate entry', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleQcSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...qcForm,
        inward_gate_entry_id: qcForm.inward_gate_entry_id || null,
        size_deviation_pct: qcForm.size_deviation_pct ? parseFloat(qcForm.size_deviation_pct) : null,
        evenness_pct: qcForm.evenness_pct ? parseFloat(qcForm.evenness_pct) : null,
        cleanness_pct: qcForm.cleanness_pct ? parseFloat(qcForm.cleanness_pct) : null,
        neatness_pct: qcForm.neatness_pct ? parseFloat(qcForm.neatness_pct) : null,
        tenacity_gd: qcForm.tenacity_gd ? parseFloat(qcForm.tenacity_gd) : null,
        cohesion_strokes: qcForm.cohesion_strokes ? parseInt(qcForm.cohesion_strokes) : null,
        live_moisture_reading_pct: qcForm.live_moisture_reading_pct ? parseFloat(qcForm.live_moisture_reading_pct) : null,
        sample_hank_weight_g: qcForm.sample_hank_weight_g ? parseInt(qcForm.sample_hank_weight_g) : null
      }

      const response = await fetch(`${API_URL}/quality/intake`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Quality intake ${data.quality_intake_no} created`, 'success')
        setValidationResult({ type: 'success', data })
        setQcForm({
          inward_gate_entry_id: '', silk_type: 'BIVOLTINE_WHITE_SILK', machinery_source: 'ARM_AUTOMATIC_REELING',
          silk_mark_tag_id: '', lab_report_number: '', certified_grade: '4A', size_deviation_pct: '',
          evenness_pct: '', cleanness_pct: '', neatness_pct: '', tenacity_gd: '', cohesion_strokes: '',
          live_moisture_reading_pct: '', has_machine_oil_stains: false, has_mixed_dye_lots: false, sample_hank_weight_g: ''
        })
        fetchQualityIntakes()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create quality intake', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleApproveIntake = async (intakeId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/quality/intake/${intakeId}/approve`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ notes: 'Approved by Quality Manager' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Intake approved: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchQualityIntakes()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Approval failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to approve intake', 'error')
    }
  }

  const handleRejectIntake = async (intakeId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/quality/intake/${intakeId}/reject`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason: 'Rejected by Quality Manager' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Intake rejected', 'warning')
        fetchQualityIntakes()
      } else {
        addNotification(data.error || 'Rejection failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to reject intake', 'error')
    }
  }

  const handleHoldIntake = async (intakeId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/quality/intake/${intakeId}/hold`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason: 'Placed on hold for further review' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Intake placed on hold', 'warning')
        fetchQualityIntakes()
      } else {
        addNotification(data.error || 'Hold failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to place intake on hold', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'WARP_PREMIUM': return 'success'
      case 'WEFT_ONLY': return 'info'
      case 'QC_HOLD': return 'warning'
      case 'REJECTED_VENDOR_RETURN': return 'error'
      case 'SUBMITTED': return 'info'
      case 'APPROVED': return 'success'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => getStatusColor(routing)

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Filature Supplier — Inward Quality Gate
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
      </Box>

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="fullWidth">
          {QA_TABS.map((t) => (
            <Tab key={t.id} label={t.label} value={t.id} />
          ))}
        </Tabs>
      </Paper>

      {validationResult && validationResult.type === 'error' && validationResult.data.errors && (
        <Alert severity="error" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Guardrail Violations</Typography>
          {validationResult.data.errors.map((err, idx) => (
            <Typography key={idx} variant="body2">• [{err.code}] {err.message}</Typography>
          ))}
        </Alert>
      )}

      {validationResult && validationResult.type === 'success' && (
        <Alert severity="success" sx={{ mb: 3 }}>
          {tab === 'gate-clerk' && `Gate entry ${validationResult.data.inward_gate_entry_no} created`}
          {tab === 'qc-inspector' && `Quality intake ${validationResult.data.quality_intake_no} created`}
          {tab === 'quality-manager' && `Action completed: ${validationResult.data.status}`}
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Hash:</strong> {certificateDetail.certificate_hash}</Typography>
          <Typography variant="body2"><strong>Status:</strong> {certificateDetail.status}</Typography>
        </Alert>
      )}

      {/* ===================== GATE CLERK TAB ===================== */}
      {tab === 'gate-clerk' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Category A: Logistics & Traceability Data</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth label="Supplier ID" value={gateForm.supplier_id}
                    onChange={(e) => setGateForm({ ...gateForm, supplier_id: e.target.value })}
                    placeholder="Linked to Vendor Master"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth label="Invoice Number" value={gateForm.invoice_number}
                    onChange={(e) => setGateForm({ ...gateForm, invoice_number: e.target.value })}
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth label="Invoice Gross Weight (kg)" type="number"
                    value={gateForm.invoice_gross_weight_kg}
                    onChange={(e) => setGateForm({ ...gateForm, invoice_gross_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }}
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth label="Actual Scale Weight (kg)" type="number"
                    value={gateForm.actual_scale_weight_kg}
                    onChange={(e) => setGateForm({ ...gateForm, actual_scale_weight_kg: e.target.value })}
                    inputProps={{ step: '0.001', min: 0 }}
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth label="Filature Lot Number" value={gateForm.filature_lot_number}
                    onChange={(e) => setGateForm({ ...gateForm, filature_lot_number: e.target.value })}
                    placeholder="Primary batch stamp"
                    required
                  />
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleGateSubmit} disabled={submitting}>
                    {submitting ? 'Creating...' : 'Create Gate Entry'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Inward Gate Entries</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Entry No</TableCell>
                      <TableCell>Supplier</TableCell>
                      <TableCell>Invoice</TableCell>
                      <TableCell>Invoice Wt</TableCell>
                      <TableCell>Scale Wt</TableCell>
                      <TableCell>Filature Lot</TableCell>
                      <TableCell>Status</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {gateEntries.map((entry) => (
                      <TableRow key={entry.id}>
                        <TableCell>{entry.inward_gate_entry_no}</TableCell>
                        <TableCell>{entry.supplier_name}</TableCell>
                        <TableCell>{entry.invoice_number}</TableCell>
                        <TableCell>{entry.invoice_gross_weight_kg} kg</TableCell>
                        <TableCell>{entry.actual_scale_weight_kg} kg</TableCell>
                        <TableCell>{entry.filature_lot_number}</TableCell>
                        <TableCell><Chip label={entry.status} color={getStatusColor(entry.status)} size="small" /></TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== QC INSPECTOR TAB ===================== */}
      {tab === 'qc-inspector' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Category B, C, D: Quality Intake Form</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Inward Gate Entry</InputLabel>
                    <Select value={qcForm.inward_gate_entry_id} label="Inward Gate Entry"
                      onChange={(e) => setQcForm({ ...qcForm, inward_gate_entry_id: e.target.value })}>
                      <MenuItem value="">Select entry</MenuItem>
                      {gateEntries.map((entry) => (
                        <MenuItem key={entry.id} value={entry.id}>{entry.inward_gate_entry_no} — {entry.supplier_name}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Silk Type</InputLabel>
                    <Select value={qcForm.silk_type} label="Silk Type"
                      onChange={(e) => setQcForm({ ...qcForm, silk_type: e.target.value })}>
                      {SILK_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Machinery Source</InputLabel>
                    <Select value={qcForm.machinery_source} label="Machinery Source"
                      onChange={(e) => setQcForm({ ...qcForm, machinery_source: e.target.value })}>
                      {MACHINERY_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Silk Mark Tag ID" value={qcForm.silk_mark_tag_id}
                    onChange={(e) => setQcForm({ ...qcForm, silk_mark_tag_id: e.target.value })} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Lab Report Number" value={qcForm.lab_report_number}
                    onChange={(e) => setQcForm({ ...qcForm, lab_report_number: e.target.value })} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Certified Grade</InputLabel>
                    <Select value={qcForm.certified_grade} label="Certified Grade"
                      onChange={(e) => setQcForm({ ...qcForm, certified_grade: e.target.value })}>
                      {GRADE_OPTIONS.map((grade) => (
                        <MenuItem key={grade} value={grade}>{grade}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Size Deviation %" type="number"
                    value={qcForm.size_deviation_pct}
                    onChange={(e) => setQcForm({ ...qcForm, size_deviation_pct: e.target.value })}
                    inputProps={{ step: '0.01', min: 0 }} helperText="Alert if > 4.0%" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Evenness %" type="number"
                    value={qcForm.evenness_pct}
                    onChange={(e) => setQcForm({ ...qcForm, evenness_pct: e.target.value })}
                    inputProps={{ step: '0.1', min: 0, max: 100 }} helperText="Target ≥ 95%" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Cleanness %" type="number"
                    value={qcForm.cleanness_pct}
                    onChange={(e) => setQcForm({ ...qcForm, cleanness_pct: e.target.value })}
                    inputProps={{ step: '0.1', min: 0, max: 100 }} helperText="Target ≥ 95%" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Neatness %" type="number"
                    value={qcForm.neatness_pct}
                    onChange={(e) => setQcForm({ ...qcForm, neatness_pct: e.target.value })}
                    inputProps={{ step: '0.1', min: 0, max: 100 }} helperText="Target ≥ 93%" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Tenacity (g/d)" type="number"
                    value={qcForm.tenacity_gd}
                    onChange={(e) => setQcForm({ ...qcForm, tenacity_gd: e.target.value })}
                    inputProps={{ step: '0.1', min: 0 }} helperText="Target 3.5–4.0 g/d" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Cohesion Strokes" type="number"
                    value={qcForm.cohesion_strokes}
                    onChange={(e) => setQcForm({ ...qcForm, cohesion_strokes: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="Min 60 strokes" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Live Moisture Reading %" type="number"
                    value={qcForm.live_moisture_reading_pct}
                    onChange={(e) => setQcForm({ ...qcForm, live_moisture_reading_pct: e.target.value })}
                    inputProps={{ step: '0.1', min: 0, max: 100 }} helperText="Max 11.0% standard" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Machine Oil Stains</InputLabel>
                    <Select value={qcForm.has_machine_oil_stains ? 'Yes' : 'No'} label="Machine Oil Stains"
                      onChange={(e) => setQcForm({ ...qcForm, has_machine_oil_stains: e.target.value === 'Yes' })}>
                      <MenuItem value="No">No</MenuItem>
                      <MenuItem value="Yes">Yes</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Mixed Dye Lots</InputLabel>
                    <Select value={qcForm.has_mixed_dye_lots ? 'Yes' : 'No'} label="Mixed Dye Lots"
                      onChange={(e) => setQcForm({ ...qcForm, has_mixed_dye_lots: e.target.value === 'Yes' })}>
                      <MenuItem value="No">No</MenuItem>
                      <MenuItem value="Yes">Yes</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Sample Hank Weight (g)" type="number"
                    value={qcForm.sample_hank_weight_g}
                    onChange={(e) => setQcForm({ ...qcForm, sample_hank_weight_g: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="Standard: 500g" />
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleQcSubmit} disabled={submitting}>
                    {submitting ? 'Submitting...' : 'Submit Quality Intake'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Quality Intakes</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Intake No</TableCell>
                      <TableCell>Grade</TableCell>
                      <TableCell>Silk Type</TableCell>
                      <TableCell>Moisture %</TableCell>
                      <TableCell>Evenness</TableCell>
                      <TableCell>Tenacity</TableCell>
                      <TableCell>Routing</TableCell>
                      <TableCell>Status</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {qualityIntakes.map((intake) => (
                      <TableRow key={intake.id}>
                        <TableCell>{intake.quality_intake_no}</TableCell>
                        <TableCell>{intake.certified_grade}</TableCell>
                        <TableCell>{intake.silk_type}</TableCell>
                        <TableCell>{intake.live_moisture_reading_pct}%</TableCell>
                        <TableCell>{intake.evenness_pct}%</TableCell>
                        <TableCell>{intake.tenacity_gd} g/d</TableCell>
                        <TableCell><Chip label={intake.auto_assigned_routing} color={getRoutingColor(intake.auto_assigned_routing)} size="small" /></TableCell>
                        <TableCell><Chip label={intake.status} color={getStatusColor(intake.status)} size="small" /></TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== QUALITY MANAGER TAB ===================== */}
      {tab === 'quality-manager' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Quality Intakes — Pending Approval</Typography>
              {qualityIntakes.length === 0 ? (
                <Typography color="text.secondary">No quality intakes yet</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Intake No</TableCell>
                        <TableCell>Grade</TableCell>
                        <TableCell>Moisture %</TableCell>
                        <TableCell>Size Dev %</TableCell>
                        <TableCell>Tenacity</TableCell>
                        <TableCell>Routing</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {qualityIntakes.map((intake) => (
                        <TableRow key={intake.id}>
                          <TableCell>{intake.quality_intake_no}</TableCell>
                          <TableCell>{intake.certified_grade}</TableCell>
                          <TableCell>{intake.live_moisture_reading_pct}%</TableCell>
                          <TableCell>{intake.size_deviation_pct}%</TableCell>
                          <TableCell>{intake.tenacity_gd} g/d</TableCell>
                          <TableCell><Chip label={intake.auto_assigned_routing} color={getRoutingColor(intake.auto_assigned_routing)} size="small" /></TableCell>
                          <TableCell><Chip label={intake.status} color={getStatusColor(intake.status)} size="small" /></TableCell>
                          <TableCell>
                            {intake.status === 'SUBMITTED' && (
                              <>
                                <Button size="small" variant="outlined" color="success" onClick={() => handleApproveIntake(intake.id)}>Approve</Button>
                                <Button size="small" variant="outlined" color="error" onClick={() => handleRejectIntake(intake.id)} sx={{ ml: 1 }}>Reject</Button>
                                <Button size="small" variant="outlined" onClick={() => handleHoldIntake(intake.id)} sx={{ ml: 1 }}>Hold</Button>
                              </>
                            )}
                          </TableCell>
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
              <Typography variant="h6" gutterBottom>Quality Certificates</Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates issued yet</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag</TableCell>
                        <TableCell>Grade</TableCell>
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
                          <TableCell>{cert.certified_grade}</TableCell>
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
        </Grid>
      )}

      {/* ===================== SALES FORECAST TAB ===================== */}
      {tab === 'sales-forecast' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Sales Forecast — Material Processing Plan</Typography>
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
                          <TableCell>Silk Type</TableCell>
                          <TableCell>Denier</TableCell>
                          <TableCell>Est. Qty (kg)</TableCell>
                          <TableCell>Priority</TableCell>
                          <TableCell>Destination</TableCell>
                          <TableCell>Recommended Grade</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.material_requirements.map((item, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{item.silk_type}</TableCell>
                            <TableCell>{item.denier}</TableCell>
                            <TableCell>{item.estimated_quantity_kg}</TableCell>
                            <TableCell><Chip label={item.priority} color={item.priority === 'HIGH' ? 'error' : 'warning'} size="small" /></TableCell>
                            <TableCell>{item.destination}</TableCell>
                            <TableCell>{item.recommended_grade}</TableCell>
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
                          <TableCell>Est. Sarees</TableCell>
                          <TableCell>Est. Yarn (kg)</TableCell>
                          <TableCell>Target Grade</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.upcoming_lots.map((lot, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{lot.lot_number}</TableCell>
                            <TableCell>{lot.estimated_sarees}</TableCell>
                            <TableCell>{lot.estimated_yarn_kg}</TableCell>
                            <TableCell>{lot.target_grade}</TableCell>
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
