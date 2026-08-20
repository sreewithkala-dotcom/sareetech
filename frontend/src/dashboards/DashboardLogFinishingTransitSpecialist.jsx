import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const LOG_FINISHING_TRANSIT_SPECIALIST_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Silk Mark Officer' },
  { id: 'finishing', label: 'Finishing & Packaging' },
  { id: 'dispatch', label: 'Dispatch Release & Transit Verification' },
  { id: 'certificates', label: 'Certificates & Product Details' },
  { id: 'forecast', label: 'Sales Forecast & Material Plan' },
]

const FINISHING_MACHINE_OPTIONS = [
  { value: 'Tensionless Felt-Belt Steam Calender', label: 'Tensionless Felt-Belt Steam Calender [Default]' },
  { value: 'Rotary Roller Calender', label: 'Rotary Roller Calender' },
  { value: 'Manual Steam Table', label: 'Manual Steam Table' },
  { value: 'Decatising Pressure Vessel', label: 'Decatising Pressure Vessel' },
]

const EDGE_FRINGING_OPTIONS = [
  { value: 'Hand-Twisted Micro-Fringe Knotting', label: 'Hand-Twisted Micro-Fringe Knotting [Default]' },
  { value: 'Satin Ribbon Hem Lock', label: 'Satin Ribbon Hem Lock' },
  { value: 'Ultra-Sonic Edge Cut', label: 'Ultra-Sonic Edge Cut' },
  { value: 'Unfinished Open Fringe', label: 'Unfinished Open Fringe' },
]

const PACKAGING_MATERIAL_OPTIONS = [
  { value: 'Acid-Free Tissue + Anti-Tarnish Vacuum Pack', label: 'Acid-Free Tissue + Anti-Tarnish Vacuum Pack [Default]' },
  { value: 'Standard 50-Micron Polybag', label: 'Standard 50-Micron Polybag' },
  { value: 'Breathable Cotton Muslin Bag', label: 'Breathable Cotton Muslin Bag' },
]

const DESICCANT_OPTIONS = [
  { value: 'Active Silica + Activated Carbon Pack', label: 'Active Silica + Activated Carbon Pack [Default]' },
  { value: 'Silica Gel Only', label: 'Silica Gel Only' },
  { value: 'None (Unsafe)', label: 'None (Unsafe)' },
]

const PALLU_INTERLEAVING_OPTIONS = [
  { value: 'Acid-Free Interleaved (Zero-Contact)', label: 'Acid-Free Interleaved (Zero-Contact) [Default]' },
  { value: 'Standard Paper Interleaved', label: 'Standard Paper Interleaved' },
  { value: 'No Interleaving', label: 'No Interleaving' },
]

const LOGISTICS_TRANSIT_OPTIONS = [
  { value: 'READY_FOR_DISPATCH_MANIFESTED', label: 'READY_FOR_DISPATCH_MANIFESTED [Default]' },
  { value: 'HOLD_PENDING_EXCISE_CUSTOMS', label: 'HOLD_PENDING_EXCISE_CUSTOMS' },
  { value: 'REJECTED_PACKAGING_DAMAGED', label: 'REJECTED_PACKAGING_DAMAGED' },
]

const LOGISTICS_CARRIER_OPTIONS = [
  { value: 'DHL Express', label: 'DHL Express' },
  { value: 'FedEx Cargo', label: 'FedEx Cargo' },
  { value: 'Bluedart Premium Air', label: 'Bluedart Premium Air' },
  { value: 'Local Dedicated Freight', label: 'Local Dedicated Freight' },
]

export default function DashboardLogFinishingTransitSpecialist() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [silkMarkOfficerLogs, setSilkMarkOfficerLogs] = useState([])
  const [finishingLogs, setFinishingLogs] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [form, setForm] = useState({
    finishing_job_id: '',
    saree_piece_serial_id: '',
    quality_inspector_log_id: '',
    qa_dyeing_inspector_log_id: '',
    silk_mark_officer_log_id: '',
    b2b_sales_order_ref: '',
    finishing_machine_type: 'Tensionless Felt-Belt Steam Calender',
    steam_temperature_celsius: '',
    edge_fringing_method: 'Hand-Twisted Micro-Fringe Knotting',
    scanned_saree_serial_no: '',
    verified_silk_mark_tag_id: '',
    packaging_material_spec: 'Acid-Free Tissue + Anti-Tarnish Vacuum Pack',
    anti_tarnish_desiccant_inserted: 'Active Silica + Activated Carbon Pack',
    pallu_interleaving_status: 'Acid-Free Interleaved (Zero-Contact)',
    final_packed_weight_grams: '',
    dispatch_manifest_id: '',
    tamper_seal_barcode_id: '',
    logistics_transit_status: 'READY_FOR_DISPATCH_MANIFESTED',
    logistics_carrier_name: '',
    consignment_airway_bill_no: '',
    gross_consignment_shipping_weight_kg: ''
  })

  useEffect(() => {
    fetchSilkMarkOfficerLogs()
    fetchFinishingLogs()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchSilkMarkOfficerLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/silk-mark-officer/logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setSilkMarkOfficerLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch silk mark officer logs:', error)
    }
  }

  const fetchFinishingLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/log-finishing-transit-specialist/logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setFinishingLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch finishing logs:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/log-finishing-transit-specialist/certificates`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/log-finishing-transit-specialist`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setForecast(data)
    } catch (error) {
      console.error('Failed to fetch sales forecast:', error)
    }
  }

  const handleSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...form,
        steam_temperature_celsius: form.steam_temperature_celsius ? parseFloat(form.steam_temperature_celsius) : null,
        final_packed_weight_grams: form.final_packed_weight_grams ? parseFloat(form.final_packed_weight_grams) : null,
        gross_consignment_shipping_weight_kg: form.gross_consignment_shipping_weight_kg ? parseFloat(form.gross_consignment_shipping_weight_kg) : null,
      }

      const response = await fetch(`${API_URL}/log-finishing-transit-specialist/logs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Finishing job ${data.finishing_job_id} submitted`, 'success')
        setValidationResult({ type: 'success', data })
        fetchFinishingLogs()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to submit finishing job', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCertify = async (logId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/log-finishing-transit-specialist/logs/${logId}/certify`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Certificate generated: ${data.qr_tag_id}`, 'success')
        setCertificateDetail(data)
        fetchFinishingLogs()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify finishing job', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'READY_FOR_DISPATCH_MANIFESTED':
      case 'CERTIFIED_GENUINE_SILK_MARK_RELEASED':
      case 'PASSED_CLEARED_FOR_PACKING':
        return 'success'
      case 'HOLD_PENDING_EXCISE_CUSTOMS':
      case 'HOLD_PURITY_RETEST_REQUIRED':
        return 'warning'
      case 'REJECTED_PACKAGING_DAMAGED':
      case 'REJECTED_COUNTERFEIT_OR_BLEND':
        return 'error'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    if (!routing) return 'default'
    if (routing.includes('HOLD') || routing.includes('REJECTED')) return 'error'
    if (routing.includes('PASSED') || routing.includes('READY')) return 'success'
    return 'info'
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            LOG Finishing & Transit Specialist — Finishing, Packaging & Dispatch
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
      </Box>

      {validationResult && validationResult.type === 'error' && validationResult.data.validation_errors && validationResult.data.validation_errors.length > 0 && (
        <Alert severity="error" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Guardrail Violations</Typography>
          {validationResult.data.validation_errors.map((err, idx) => (
            <Typography key={idx} variant="body2">
              • [{err.code}] {err.message}
            </Typography>
          ))}
        </Alert>
      )}

      {validationResult && validationResult.type === 'success' && (
        <Alert severity="success" sx={{ mb: 3 }}>
          Finishing job {validationResult.data.finishing_job_id} submitted successfully
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Finishing Job ID:</strong> {certificateDetail.finishing_job_id}</Typography>
          <Typography variant="body2"><strong>Hash:</strong> {certificateDetail.certificate_hash}</Typography>
          <Typography variant="body2"><strong>QR Tag:</strong> {certificateDetail.qr_tag_id}</Typography>
        </Alert>
      )}

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="fullWidth">
          {LOG_FINISHING_TRANSIT_SPECIALIST_TABS.map((t) => (
            <Tab key={t.id} label={t.label} value={t.id} />
          ))}
        </Tabs>
      </Paper>

      {/* ===================== PRE-PROCESS TAB ===================== */}
      {tab === 'preprocess' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Silk Mark Officer Approved Logs (Pre-Process)</Typography>
              {silkMarkOfficerLogs.length === 0 ? (
                <Typography color="text.secondary">No approved silk mark officer logs found. Silk Mark Officer must submit and clear compliance audit before Finishing & Transit Specialist can start processing.</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Audit Visit ID</TableCell>
                        <TableCell>Saree Serial</TableCell>
                        <TableCell>Burn Test</TableCell>
                        <TableCell>Solubility</TableCell>
                        <TableCell>Zari Purity</TableCell>
                        <TableCell>Tag Serial</TableCell>
                        <TableCell>Approval State</TableCell>
                        <TableCell>Routing</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {silkMarkOfficerLogs.map((log) => (
                        <TableRow key={log.id}>
                          <TableCell>{log.audit_visit_id}</TableCell>
                          <TableCell>{log.saree_piece_serial_id}</TableCell>
                          <TableCell><Chip label={log.burn_test_result_warp_weft} color={getStatusColor(log.burn_test_result_warp_weft)} size="small" /></TableCell>
                          <TableCell><Chip label={log.chemical_solubility_test_status} color={getStatusColor(log.chemical_solubility_test_status)} size="small" /></TableCell>
                          <TableCell><Chip label={log.zari_purity_classification} color={getStatusColor(log.zari_purity_classification)} size="small" /></TableCell>
                          <TableCell>{log.silk_mark_tag_serial_number}</TableCell>
                          <TableCell><Chip label={log.silk_mark_officer_approval_state} color={getStatusColor(log.silk_mark_officer_approval_state)} size="small" /></TableCell>
                          <TableCell><Chip label={log.auto_assigned_routing} color={getRoutingColor(log.auto_assigned_routing)} size="small" /></TableCell>
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

      {/* ===================== FINISHING & PACKAGING TAB ===================== */}
      {tab === 'finishing' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category A: Finishing Setup & Processing Configuration
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Finishing Job ID"
                    value={form.finishing_job_id}
                    onChange={(e) => setForm({ ...form, finishing_job_id: e.target.value })}
                    placeholder="AUTO-GENERATED"
                    disabled
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Saree Piece Serial ID"
                    value={form.saree_piece_serial_id}
                    onChange={(e) => setForm({ ...form, saree_piece_serial_id: e.target.value })}
                    placeholder="SAR-80W-001"
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Finishing Machine Type</InputLabel>
                    <Select
                      value={form.finishing_machine_type}
                      label="Finishing Machine Type"
                      onChange={(e) => setForm({ ...form, finishing_machine_type: e.target.value })}
                    >
                      {FINISHING_MACHINE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Steam Temperature (°C)"
                    type="number"
                    value={form.steam_temperature_celsius}
                    onChange={(e) => setForm({ ...form, steam_temperature_celsius: e.target.value })}
                    inputProps={{ min: 0, max: 200, step: 0.1 }}
                    helperText="Target: 110°C to 120°C"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Edge Fringing Method</InputLabel>
                    <Select
                      value={form.edge_fringing_method}
                      label="Edge Fringing Method"
                      onChange={(e) => setForm({ ...form, edge_fringing_method: e.target.value })}
                    >
                      {EDGE_FRINGING_OPTIONS.map((opt) => (
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
              <Typography variant="h6" gutterBottom>
                Category B: Packaging Integrity & Anti-Tarnish Audit
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Scanned Saree Serial No"
                    value={form.scanned_saree_serial_no}
                    onChange={(e) => setForm({ ...form, scanned_saree_serial_no: e.target.value })}
                    placeholder="Scan barcode"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Verified Silk Mark Tag ID"
                    value={form.verified_silk_mark_tag_id}
                    onChange={(e) => setForm({ ...form, verified_silk_mark_tag_id: e.target.value })}
                    placeholder="Scan Silk Mark hologram QR code"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Packaging Material Spec</InputLabel>
                    <Select
                      value={form.packaging_material_spec}
                      label="Packaging Material Spec"
                      onChange={(e) => setForm({ ...form, packaging_material_spec: e.target.value })}
                    >
                      {PACKAGING_MATERIAL_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Anti-Tarnish Desiccant Inserted</InputLabel>
                    <Select
                      value={form.anti_tarnish_desiccant_inserted}
                      label="Anti-Tarnish Desiccant Inserted"
                      onChange={(e) => setForm({ ...form, anti_tarnish_desiccant_inserted: e.target.value })}
                    >
                      {DESICCANT_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Pallu Interleaving Status</InputLabel>
                    <Select
                      value={form.pallu_interleaving_status}
                      label="Pallu Interleaving Status"
                      onChange={(e) => setForm({ ...form, pallu_interleaving_status: e.target.value })}
                    >
                      {PALLU_INTERLEAVING_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Final Packed Weight (grams)"
                    type="number"
                    value={form.final_packed_weight_grams}
                    onChange={(e) => setForm({ ...form, final_packed_weight_grams: e.target.value })}
                    inputProps={{ min: 0, step: 0.1 }}
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Pre-Process Linkages</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Quality Inspector Log ID"
                    value={form.quality_inspector_log_id}
                    onChange={(e) => setForm({ ...form, quality_inspector_log_id: e.target.value })}
                    placeholder="QI-20240820-1234"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="QA Dyeing Inspector Log ID"
                    value={form.qa_dyeing_inspector_log_id}
                    onChange={(e) => setForm({ ...form, qa_dyeing_inspector_log_id: e.target.value })}
                    placeholder="QDI-20240820-1234"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Silk Mark Officer Log ID"
                    value={form.silk_mark_officer_log_id}
                    onChange={(e) => setForm({ ...form, silk_mark_officer_log_id: e.target.value })}
                    placeholder="SMO-20240820-1234"
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="B2B Sales Order Ref"
                    value={form.b2b_sales_order_ref}
                    onChange={(e) => setForm({ ...form, b2b_sales_order_ref: e.target.value })}
                    placeholder="B2B-PO-2024-001"
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Button
              variant="contained"
              size="large"
              onClick={handleSubmit}
              disabled={submitting}
              fullWidth
            >
              {submitting ? 'Submitting...' : 'Submit Finishing & Packaging Job'}
            </Button>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Recent Finishing Jobs
              </Typography>
              {finishingLogs.length === 0 ? (
                <Typography color="text.secondary">No finishing jobs yet</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Finishing Job ID</TableCell>
                        <TableCell>Saree Serial</TableCell>
                        <TableCell>Machine Type</TableCell>
                        <TableCell>Temp (°C)</TableCell>
                        <TableCell>Packaging</TableCell>
                        <TableCell>Transit Status</TableCell>
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {finishingLogs.map((log) => (
                        <TableRow key={log.id}>
                          <TableCell>{log.finishing_job_id}</TableCell>
                          <TableCell>{log.saree_piece_serial_id}</TableCell>
                          <TableCell><Chip label={log.finishing_machine_type} color="info" size="small" /></TableCell>
                          <TableCell>{log.steam_temperature_celsius}</TableCell>
                          <TableCell><Chip label={log.packaging_material_spec} color={getStatusColor(log.packaging_material_spec)} size="small" /></TableCell>
                          <TableCell><Chip label={log.logistics_transit_status} color={getStatusColor(log.logistics_transit_status)} size="small" /></TableCell>
                          <TableCell>
                            <Button size="small" variant="contained" onClick={() => handleCertify(log.id)}>
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

      {/* ===================== DISPATCH TAB ===================== */}
      {tab === 'dispatch' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category C: Dispatch Release & Transit Verification
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Dispatch Manifest ID"
                    value={form.dispatch_manifest_id}
                    onChange={(e) => setForm({ ...form, dispatch_manifest_id: e.target.value })}
                    placeholder="Master shipping invoice / consignment identifier"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Tamper Seal Barcode ID"
                    value={form.tamper_seal_barcode_id}
                    onChange={(e) => setForm({ ...form, tamper_seal_barcode_id: e.target.value })}
                    placeholder="Security seal code"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Logistics Transit Status</InputLabel>
                    <Select
                      value={form.logistics_transit_status}
                      label="Logistics Transit Status"
                      onChange={(e) => setForm({ ...form, logistics_transit_status: e.target.value })}
                    >
                      {LOGISTICS_TRANSIT_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Logistics Carrier Name</InputLabel>
                    <Select
                      value={form.logistics_carrier_name}
                      label="Logistics Carrier Name"
                      onChange={(e) => setForm({ ...form, logistics_carrier_name: e.target.value })}
                    >
                      {LOGISTICS_CARRIER_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Consignment Airway Bill No / Tracking ID"
                    value={form.consignment_airway_bill_no}
                    onChange={(e) => setForm({ ...form, consignment_airway_bill_no: e.target.value })}
                    placeholder="Transport tracking code"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Gross Consignment Shipping Weight (kg)"
                    type="number"
                    value={form.gross_consignment_shipping_weight_kg}
                    onChange={(e) => setForm({ ...form, gross_consignment_shipping_weight_kg: e.target.value })}
                    inputProps={{ min: 0, step: 0.1 }}
                  />
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
              <Typography variant="h6" gutterBottom>
                Finishing & Transit Certificates & Product Details
              </Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates generated yet. Certificates are created when a finishing job is certified.</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag ID</TableCell>
                        <TableCell>Finishing Job ID</TableCell>
                        <TableCell>Saree Serial</TableCell>
                        <TableCell>Verified Silk Mark Tag</TableCell>
                        <TableCell>Dispatch Manifest</TableCell>
                        <TableCell>Airway Bill</TableCell>
                        <TableCell>Carrier</TableCell>
                        <TableCell>Transit Status</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell>Certified At</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {certificates.map((cert) => (
                        <TableRow key={cert.id}>
                          <TableCell><code>{cert.certificate_hash}</code></TableCell>
                          <TableCell><code>{cert.qr_tag_id}</code></TableCell>
                          <TableCell>{cert.finishing_job_id}</TableCell>
                          <TableCell>{cert.saree_piece_serial_id}</TableCell>
                          <TableCell>{cert.verified_silk_mark_tag_id}</TableCell>
                          <TableCell>{cert.dispatch_manifest_id}</TableCell>
                          <TableCell>{cert.consignment_airway_bill_no}</TableCell>
                          <TableCell>{cert.logistics_carrier_name}</TableCell>
                          <TableCell><Chip label={cert.logistics_transit_status} color={getStatusColor(cert.logistics_transit_status)} size="small" /></TableCell>
                          <TableCell><Chip label={cert.status} color={cert.status === 'ACTIVE' || cert.status === 'IN_TRANSIT' ? 'success' : 'default'} size="small" /></TableCell>
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

      {/* ===================== FORECAST TAB ===================== */}
      {tab === 'forecast' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Sales Forecast: Finishing & Transit Material Processing Plan
              </Typography>
              {!forecast ? (
                <Typography color="text.secondary">Loading forecast...</Typography>
              ) : (
                <Grid container spacing={3}>
                  <Grid item xs={12} md={6}>
                    <Card variant="outlined">
                      <CardContent>
                        <Typography variant="subtitle1" gutterBottom>Factory</Typography>
                        <Typography variant="h6">{forecast.factory_node_id || 'N/A'}</Typography>
                        <Typography variant="body2" color="text.secondary">Period: {forecast.forecast_period}</Typography>
                        <Typography variant="body2" color="text.secondary">Generated: {forecast.generated_at}</Typography>
                      </CardContent>
                    </Card>
                  </Grid>
                  <Grid item xs={12}>
                    <Typography variant="subtitle1" gutterBottom sx={{ mt: 2 }}>Material Requirements</Typography>
                    {forecast.material_requirements && forecast.material_requirements.length > 0 ? (
                      <TableContainer>
                        <Table>
                          <TableHead>
                            <TableRow>
                              <TableCell>Saree Category</TableCell>
                              <TableCell>Design Code</TableCell>
                              <TableCell>Machine Type</TableCell>
                              <TableCell>Temp (°C)</TableCell>
                              <TableCell>Fringing</TableCell>
                              <TableCell>Packaging</TableCell>
                              <TableCell>Desiccant</TableCell>
                              <TableCell>Interleaving</TableCell>
                              <TableCell>Est. Jobs</TableCell>
                              <TableCell>Priority</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.material_requirements.map((item, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{item.saree_category}</TableCell>
                                <TableCell><code>{item.design_code}</code></TableCell>
                                <TableCell>{item.finishing_machine_type}</TableCell>
                                <TableCell>{item.steam_temperature_celsius}</TableCell>
                                <TableCell>{item.edge_fringing_method}</TableCell>
                                <TableCell>{item.packaging_material_spec}</TableCell>
                                <TableCell>{item.anti_tarnish_desiccant_inserted}</TableCell>
                                <TableCell>{item.pallu_interleaving_status}</TableCell>
                                <TableCell>{item.estimated_finishing_jobs}</TableCell>
                                <TableCell>
                                  <Chip label={item.priority} color={item.priority === 'HIGH' ? 'error' : item.priority === 'MEDIUM' ? 'warning' : 'info'} size="small" />
                                </TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </TableContainer>
                    ) : (
                      <Typography color="text.secondary">No material requirements available</Typography>
                    )}
                  </Grid>
                  <Grid item xs={12}>
                    <Typography variant="subtitle1" gutterBottom sx={{ mt: 2 }}>Upcoming Lots</Typography>
                    {forecast.upcoming_lots && forecast.upcoming_lots.length > 0 ? (
                      <TableContainer>
                        <Table>
                          <TableHead>
                            <TableRow>
                              <TableCell>Lot Number</TableCell>
                              <TableCell>Saree Category</TableCell>
                              <TableCell>Design Code</TableCell>
                              <TableCell>Est. Jobs</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.upcoming_lots.map((lot, idx) => (
                              <TableRow key={idx}>
                                <TableCell><code>{lot.lot_number}</code></TableCell>
                                <TableCell>{lot.saree_category}</TableCell>
                                <TableCell><code>{lot.design_code}</code></TableCell>
                                <TableCell>{lot.estimated_finishing_jobs}</TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </TableContainer>
                    ) : (
                      <Typography color="text.secondary">No upcoming lots available</Typography>
                    )}
                  </Grid>
                </Grid>
              )}
            </Paper>
          </Grid>
        </Grid>
      )}
    </Container>
  )
}
