import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const SILK_MARK_OFFICER_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Quality Inspector' },
  { id: 'compliance', label: 'Silk Mark Compliance & Certification' },
  { id: 'certificates', label: 'Certificates & Product Details' },
  { id: 'forecast', label: 'Sales Forecast & Material Plan' },
]

const SILK_YARN_DENIER_OPTIONS = [
  { value: 'PASSED_16_18D_MULBERRY_SILK', label: 'PASSED_16_18D_MULBERRY_SILK [Default]' },
  { value: 'PASSED_20_22D_RAW_SILK', label: 'PASSED_20_22D_RAW_SILK' },
  { value: 'FAILED_DENIER_VARIANCE_OUT_OF_SPEC', label: 'FAILED_DENIER_VARIANCE_OUT_OF_SPEC' },
]

const BURN_TEST_OPTIONS = [
  { value: 'PASSED_CHAR_ASH_BURNT_HAIR_ODOR', label: 'PASSED_CHAR_ASH_BURNT_HAIR_ODOR [Default]' },
  { value: 'FAILED_MELTED_BEAD_SYNTHETIC_DETECTED', label: 'FAILED_MELTED_BEAD_SYNTHETIC_DETECTED' },
  { value: 'INCONCLUSIVE_RETEST_REQUIRED', label: 'INCONCLUSIVE_RETEST_REQUIRED' },
]

const CHEMICAL_SOLUBILITY_OPTIONS = [
  { value: '100%_DISSOLVED_PURE_PROTEIN', label: '100%_DISSOLVED_PURE_PROTEIN [Default]' },
  { value: 'PARTIAL_RESIDUE_POLYESTER_BLEND', label: 'PARTIAL_RESIDUE_POLYESTER_BLEND' },
  { value: 'FAILED_CELLULOSE_DETECTED', label: 'FAILED_CELLULOSE_DETECTED' },
]

const ZARI_PURITY_OPTIONS = [
  { value: 'PURE_GOLD_SILVER_TESTED_ZARI', label: 'PURE_GOLD_SILVER_TESTED_ZARI [Default]' },
  { value: 'HALF_FINE_ZARI', label: 'HALF_FINE_ZARI' },
  { value: 'METALLIC_PLASTIC_IMITATION_ZARI', label: 'METALLIC_PLASTIC_IMITATION_ZARI' },
]

const SILK_CORE_ZARI_OPTIONS = [
  { value: 'PASSED_PURE_SILK_CORE', label: 'PASSED_PURE_SILK_CORE [Default]' },
  { value: 'FAILED_COTTON_CORE_DETECTED', label: 'FAILED_COTTON_CORE_DETECTED' },
  { value: 'FAILED_VISCOSE_CORE_DETECTED', label: 'FAILED_VISCOSE_CORE_DETECTED' },
]

const TAG_APPLICATION_OPTIONS = [
  { value: 'HOLOGRAPHIC_TAG_AFFIXED_AND_SCANNED', label: 'HOLOGRAPHIC_TAG_AFFIXED_AND_SCANNED [Default]' },
  { value: 'TAG_PENDING_APPLICATON', label: 'TAG_PENDING_APPLICATON' },
  { value: 'DAMAGED_TAG_VOIDED', label: 'DAMAGED_TAG_VOIDED' },
]

const SILK_MARK_APPROVAL_OPTIONS = [
  { value: 'CERTIFIED_GENUINE_SILK_MARK_RELEASED', label: 'CERTIFIED_GENUINE_SILK_MARK_RELEASED [Default]' },
  { value: 'HOLD_PURITY_RETEST_REQUIRED', label: 'HOLD_PURITY_RETEST_REQUIRED' },
  { value: 'REJECTED_COUNTERFEIT_OR_BLEND', label: 'REJECTED_COUNTERFEIT_OR_BLEND' },
]

const LAB_REPORT_OPTIONS = [
  { value: 'PENDING_LAB_ANALYSIS', label: 'PENDING_LAB_ANALYSIS [Default]' },
  { value: 'PASSED_100_PURE_SILK', label: 'PASSED_100_PURE_SILK' },
  { value: 'FAILED_ADULTERATION_ALERT', label: 'FAILED_ADULTERATION_ALERT' },
]

export default function DashboardSilkMarkOfficer() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [qualityInspectorLogs, setQualityInspectorLogs] = useState([])
  const [silkMarkLogs, setSilkMarkLogs] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [form, setForm] = useState({
    audit_visit_id: '',
    smoii_authorised_user_id: '',
    license_validity_start_date: '',
    license_expiry_date: '',
    assigned_silk_mark_officer_id: '',
    quality_inspector_log_id: '',
    qa_dyeing_inspector_log_id: '',
    sup_supervisor_log_id: '',
    sampled_production_run_ref: '',
    sample_extraction_weight_gm: '',
    lab_report_status: 'PENDING_LAB_ANALYSIS',
    saree_piece_serial_id: '',
    silk_yarn_denier_testing_report: 'PASSED_16_18D_MULBERRY_SILK',
    burn_test_result_warp_weft: 'PASSED_CHAR_ASH_BURNT_HAIR_ODOR',
    chemical_solubility_test_status: '100%_DISSOLVED_PURE_PROTEIN',
    zari_purity_classification: 'PURE_GOLD_SILVER_TESTED_ZARI',
    xrf_silver_content_pct: '',
    xrf_gold_content_grams_per_kg: '',
    silk_core_zari_wrap_status: 'PASSED_PURE_SILK_CORE',
    silk_mark_tag_serial_number: '',
    tag_application_status: 'HOLOGRAPHIC_TAG_AFFIXED_AND_SCANNED',
    silk_mark_officer_approval_state: 'CERTIFIED_GENUINE_SILK_MARK_RELEASED',
    hologram_consignment_invoice_no: '',
    hologram_serial_range_start: '',
    hologram_serial_range_end: '',
    total_tags_received_qty: ''
  })

  useEffect(() => {
    fetchQualityInspectorLogs()
    fetchSilkMarkLogs()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchQualityInspectorLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/quality-inspector/logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setQualityInspectorLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch quality inspector logs:', error)
    }
  }

  const fetchSilkMarkLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/silk-mark-officer/logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setSilkMarkLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch silk mark logs:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/silk-mark-officer/certificates`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/silk-mark-officer`, {
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
        sample_extraction_weight_gm: form.sample_extraction_weight_gm ? parseFloat(form.sample_extraction_weight_gm) : null,
        xrf_silver_content_pct: form.xrf_silver_content_pct ? parseFloat(form.xrf_silver_content_pct) : null,
        xrf_gold_content_grams_per_kg: form.xrf_gold_content_grams_per_kg ? parseFloat(form.xrf_gold_content_grams_per_kg) : null,
        hologram_serial_range_start: form.hologram_serial_range_start ? parseInt(form.hologram_serial_range_start) : null,
        hologram_serial_range_end: form.hologram_serial_range_end ? parseInt(form.hologram_serial_range_end) : null,
        total_tags_received_qty: form.total_tags_received_qty ? parseInt(form.total_tags_received_qty) : null,
      }

      const response = await fetch(`${API_URL}/silk-mark-officer/logs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Audit visit ${data.audit_visit_id} submitted`, 'success')
        setValidationResult({ type: 'success', data })
        fetchSilkMarkLogs()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to submit audit', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCertify = async (logId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/silk-mark-officer/logs/${logId}/certify`, {
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
        fetchSilkMarkLogs()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify audit', 'error')
    }
  }

  const getApprovalColor = (state) => {
    switch (state) {
      case 'CERTIFIED_GENUINE_SILK_MARK_RELEASED':
      case 'PASSED_CHAR_ASH_BURNT_HAIR_ODOR':
      case 'PASSED_100_PURE_SILK':
      case 'PASSED_16_18D_MULBERRY_SILK':
        return 'success'
      case 'HOLD_PURITY_RETEST_REQUIRED':
      case 'REJECTED_COUNTERFEIT_OR_BLEND':
      case 'FAILED_MELTED_BEAD_SYNTHETIC_DETECTED':
      case 'PARTIAL_RESIDUE_POLYESTER_BLEND':
        return 'error'
      case 'PENDING_LAB_ANALYSIS':
      case 'INSPECTION_IN_PROGRESS':
        return 'warning'
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
            Silk Mark Officer — Compliance & Certification
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
          Audit visit {validationResult.data.audit_visit_id} submitted successfully
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Audit Visit ID:</strong> {certificateDetail.audit_visit_id}</Typography>
          <Typography variant="body2"><strong>Hash:</strong> {certificateDetail.certificate_hash}</Typography>
          <Typography variant="body2"><strong>QR Tag:</strong> {certificateDetail.qr_tag_id}</Typography>
        </Alert>
      )}

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="fullWidth">
          {SILK_MARK_OFFICER_TABS.map((t) => (
            <Tab key={t.id} label={t.label} value={t.id} />
          ))}
        </Tabs>
      </Paper>

      {/* ===================== PRE-PROCESS TAB ===================== */}
      {tab === 'preprocess' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Quality Inspector Approved Logs (Pre-Process)</Typography>
              {qualityInspectorLogs.length === 0 ? (
                <Typography color="text.secondary">No approved quality inspector logs found. Quality Inspector must submit and clear inspection before Silk Mark Officer can start compliance audit.</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Inspection ID</TableCell>
                        <TableCell>Saree Serial</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Defect Code</TableCell>
                        <TableCell>Length (m)</TableCell>
                        <TableCell>Grade</TableCell>
                        <TableCell>Approval State</TableCell>
                        <TableCell>Routing</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {qualityInspectorLogs.map((log) => (
                        <TableRow key={log.id}>
                          <TableCell>{log.inspection_id}</TableCell>
                          <TableCell>{log.saree_serial_barcode}</TableCell>
                          <TableCell>{log.loom_id_ref}</TableCell>
                          <TableCell><Chip label={log.primary_fabric_defect_code} color={log.primary_fabric_defect_code === 'DEFECT_NONE_CLEAN_PIECE' ? 'success' : 'error'} size="small" /></TableCell>
                          <TableCell>{log.total_saree_length_measured_meters}</TableCell>
                          <TableCell><Chip label={log.final_fabric_quality_grade} color={getApprovalColor(log.final_fabric_quality_grade)} size="small" /></TableCell>
                          <TableCell><Chip label={log.qa_inspector_approval_state} color={getApprovalColor(log.qa_inspector_approval_state)} size="small" /></TableCell>
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

      {/* ===================== COMPLIANCE TAB ===================== */}
      {tab === 'compliance' && (
        <Grid container spacing={3}>
          {/* Registration & Licensing Master Data */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Registration & Licensing Master Data
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="SMOII Authorised User ID"
                    value={form.smoii_authorised_user_id}
                    onChange={(e) => setForm({ ...form, smoii_authorised_user_id: e.target.value })}
                    placeholder="Factory license number"
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="License Validity Start Date"
                    type="date"
                    value={form.license_validity_start_date}
                    onChange={(e) => setForm({ ...form, license_validity_start_date: e.target.value })}
                    InputLabelProps={{ shrink: true }}
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="License Expiry Date"
                    type="date"
                    value={form.license_expiry_date}
                    onChange={(e) => setForm({ ...form, license_expiry_date: e.target.value })}
                    InputLabelProps={{ shrink: true }}
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Assigned Silk Mark Officer ID"
                    value={form.assigned_silk_mark_officer_id}
                    onChange={(e) => setForm({ ...form, assigned_silk_mark_officer_id: e.target.value })}
                    placeholder="Officer badge number"
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Audit & Sampling Log */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Audit & Sampling Log
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Audit Visit ID"
                    value={form.audit_visit_id}
                    onChange={(e) => setForm({ ...form, audit_visit_id: e.target.value })}
                    placeholder="AUTO-GENERATED"
                    disabled
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Sampled Production Run Ref"
                    value={form.sampled_production_run_ref}
                    onChange={(e) => setForm({ ...form, sampled_production_run_ref: e.target.value })}
                    placeholder="Warp beam lot / finished saree serial"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Sample Extraction Weight (gm)"
                    type="number"
                    value={form.sample_extraction_weight_gm}
                    onChange={(e) => setForm({ ...form, sample_extraction_weight_gm: e.target.value })}
                    inputProps={{ min: 0, step: 0.1 }}
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Lab Report Status</InputLabel>
                    <Select
                      value={form.lab_report_status}
                      label="Lab Report Status"
                      onChange={(e) => setForm({ ...form, lab_report_status: e.target.value })}
                    >
                      {LAB_REPORT_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Category A: Material Origin & Sampling Setup */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category A: Material Origin & Sampling Setup
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Saree Piece Serial ID"
                    value={form.saree_piece_serial_id}
                    onChange={(e) => setForm({ ...form, saree_piece_serial_id: e.target.value })}
                    placeholder="SAR-80W-001"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Silk Yarn Denier Testing Report</InputLabel>
                    <Select
                      value={form.silk_yarn_denier_testing_report}
                      label="Silk Yarn Denier Testing Report"
                      onChange={(e) => setForm({ ...form, silk_yarn_denier_testing_report: e.target.value })}
                    >
                      {SILK_YARN_DENIER_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Burn Test Result (Warp/Weft)</InputLabel>
                    <Select
                      value={form.burn_test_result_warp_weft}
                      label="Burn Test Result (Warp/Weft)"
                      onChange={(e) => setForm({ ...form, burn_test_result_warp_weft: e.target.value })}
                    >
                      {BURN_TEST_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Chemical Solubility Test Status</InputLabel>
                    <Select
                      value={form.chemical_solubility_test_status}
                      label="Chemical Solubility Test Status"
                      onChange={(e) => setForm({ ...form, chemical_solubility_test_status: e.target.value })}
                    >
                      {CHEMICAL_SOLUBILITY_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Category B: Zari Metallurgy & Purity Verification */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category B: Zari Metallurgy & Purity Verification
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Zari Purity Classification</InputLabel>
                    <Select
                      value={form.zari_purity_classification}
                      label="Zari Purity Classification"
                      onChange={(e) => setForm({ ...form, zari_purity_classification: e.target.value })}
                    >
                      {ZARI_PURITY_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="XRF Silver Content (%)"
                    type="number"
                    value={form.xrf_silver_content_pct}
                    onChange={(e) => setForm({ ...form, xrf_silver_content_pct: e.target.value })}
                    inputProps={{ min: 0, max: 100, step: 0.1 }}
                    helperText="45.0 - 52.0% for Pure Gold/Silver Tested Zari"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="XRF Gold Content (g/kg)"
                    type="number"
                    value={form.xrf_gold_content_grams_per_kg}
                    onChange={(e) => setForm({ ...form, xrf_gold_content_grams_per_kg: e.target.value })}
                    inputProps={{ min: 0, step: 0.1 }}
                    helperText="≥5.0 g/kg for Pure Gold/Silver Tested Zari"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Silk Core Zari Wrap Status</InputLabel>
                    <Select
                      value={form.silk_core_zari_wrap_status}
                      label="Silk Core Zari Wrap Status"
                      onChange={(e) => setForm({ ...form, silk_core_zari_wrap_status: e.target.value })}
                    >
                      {SILK_CORE_ZARI_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* High-Security Hologram Inventory Ledger */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                High-Security Hologram Inventory Ledger
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Hologram Consignment Invoice No"
                    value={form.hologram_consignment_invoice_no}
                    onChange={(e) => setForm({ ...form, hologram_consignment_invoice_no: e.target.value })}
                    placeholder="SMOI billing receipt number"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Hologram Serial Range Start"
                    type="number"
                    value={form.hologram_serial_range_start}
                    onChange={(e) => setForm({ ...form, hologram_serial_range_start: e.target.value })}
                    inputProps={{ min: 0 }}
                    helperText="e.g., 7001"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Hologram Serial Range End"
                    type="number"
                    value={form.hologram_serial_range_end}
                    onChange={(e) => setForm({ ...form, hologram_serial_range_end: e.target.value })}
                    inputProps={{ min: 0 }}
                    helperText="e.g., 8000"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Total Tags Received Qty"
                    type="number"
                    value={form.total_tags_received_qty}
                    onChange={(e) => setForm({ ...form, total_tags_received_qty: e.target.value })}
                    inputProps={{ min: 0 }}
                    helperText="Auto-calculated: (end - start) + 1"
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Category C: Certification Release & Serialization */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category C: Certification Release & Serialization
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Silk Mark Tag Serial Number"
                    value={form.silk_mark_tag_serial_number}
                    onChange={(e) => setForm({ ...form, silk_mark_tag_serial_number: e.target.value })}
                    placeholder="Encrypted QR serial number"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Tag Application Status</InputLabel>
                    <Select
                      value={form.tag_application_status}
                      label="Tag Application Status"
                      onChange={(e) => setForm({ ...form, tag_application_status: e.target.value })}
                    >
                      {TAG_APPLICATION_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Silk Mark Officer Approval State</InputLabel>
                    <Select
                      value={form.silk_mark_officer_approval_state}
                      label="Silk Mark Officer Approval State"
                      onChange={(e) => setForm({ ...form, silk_mark_officer_approval_state: e.target.value })}
                    >
                      {SILK_MARK_APPROVAL_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Quality Inspector Log ID"
                    value={form.quality_inspector_log_id}
                    onChange={(e) => setForm({ ...form, quality_inspector_log_id: e.target.value })}
                    placeholder="QI-20240820-1234"
                  />
                </Grid>
                <Grid item xs={12}>
                  <Button
                    variant="contained"
                    size="large"
                    onClick={handleSubmit}
                    disabled={submitting}
                    fullWidth
                  >
                    {submitting ? 'Submitting...' : 'Submit Compliance Audit'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Recent Silk Mark Logs */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Recent Compliance Audits
              </Typography>
              {silkMarkLogs.length === 0 ? (
                <Typography color="text.secondary">No compliance audits yet</Typography>
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
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {silkMarkLogs.map((log) => (
                        <TableRow key={log.id}>
                          <TableCell>{log.audit_visit_id}</TableCell>
                          <TableCell>{log.saree_piece_serial_id}</TableCell>
                          <TableCell><Chip label={log.burn_test_result_warp_weft} color={getApprovalColor(log.burn_test_result_warp_weft)} size="small" /></TableCell>
                          <TableCell><Chip label={log.chemical_solubility_test_status} color={getApprovalColor(log.chemical_solubility_test_status)} size="small" /></TableCell>
                          <TableCell><Chip label={log.zari_purity_classification} color={getApprovalColor(log.zari_purity_classification)} size="small" /></TableCell>
                          <TableCell>{log.silk_mark_tag_serial_number}</TableCell>
                          <TableCell><Chip label={log.silk_mark_officer_approval_state} color={getApprovalColor(log.silk_mark_officer_approval_state)} size="small" /></TableCell>
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

      {/* ===================== CERTIFICATES TAB ===================== */}
      {tab === 'certificates' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Silk Mark Officer Certificates & Product Details
              </Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates generated yet. Certificates are created when a compliance audit is certified.</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag ID</TableCell>
                        <TableCell>Audit Visit ID</TableCell>
                        <TableCell>Saree Serial</TableCell>
                        <TableCell>Tag Serial</TableCell>
                        <TableCell>Burn Test</TableCell>
                        <TableCell>Solubility</TableCell>
                        <TableCell>Zari Purity</TableCell>
                        <TableCell>Approval State</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell>Certified At</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {certificates.map((cert) => (
                        <TableRow key={cert.id}>
                          <TableCell><code>{cert.certificate_hash}</code></TableCell>
                          <TableCell><code>{cert.qr_tag_id}</code></TableCell>
                          <TableCell>{cert.audit_visit_id}</TableCell>
                          <TableCell>{cert.saree_piece_serial_id}</TableCell>
                          <TableCell>{cert.silk_mark_tag_serial_number}</TableCell>
                          <TableCell><Chip label={cert.burn_test_result_warp_weft} color={getApprovalColor(cert.burn_test_result_warp_weft)} size="small" /></TableCell>
                          <TableCell><Chip label={cert.chemical_solubility_test_status} color={getApprovalColor(cert.chemical_solubility_test_status)} size="small" /></TableCell>
                          <TableCell><Chip label={cert.zari_purity_classification} color={getApprovalColor(cert.zari_purity_classification)} size="small" /></TableCell>
                          <TableCell><Chip label={cert.silk_mark_officer_approval_state} color={getApprovalColor(cert.silk_mark_officer_approval_state)} size="small" /></TableCell>
                          <TableCell><Chip label={cert.status} color={cert.status === 'ACTIVE' ? 'success' : 'default'} size="small" /></TableCell>
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
                Sales Forecast: Silk Mark Officer Material Processing Plan
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
                              <TableCell>Denier Test</TableCell>
                              <TableCell>Burn Test</TableCell>
                              <TableCell>Solubility</TableCell>
                              <TableCell>Zari Purity</TableCell>
                              <TableCell>Ag % Min</TableCell>
                              <TableCell>Au g/kg Min</TableCell>
                              <TableCell>Est. Certs</TableCell>
                              <TableCell>Priority</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.material_requirements.map((item, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{item.saree_category}</TableCell>
                                <TableCell><code>{item.design_code}</code></TableCell>
                                <TableCell>{item.silk_yarn_denier_testing_report}</TableCell>
                                <TableCell>{item.burn_test_result_warp_weft}</TableCell>
                                <TableCell>{item.chemical_solubility_test_status}</TableCell>
                                <TableCell>{item.zari_purity_classification}</TableCell>
                                <TableCell>{item.xrf_silver_content_pct_min}</TableCell>
                                <TableCell>{item.xrf_gold_content_grams_per_kg_min}</TableCell>
                                <TableCell>{item.estimated_certifications}</TableCell>
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
                              <TableCell>Est. Certifications</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.upcoming_lots.map((lot, idx) => (
                              <TableRow key={idx}>
                                <TableCell><code>{lot.lot_number}</code></TableCell>
                                <TableCell>{lot.saree_category}</TableCell>
                                <TableCell><code>{lot.design_code}</code></TableCell>
                                <TableCell>{lot.estimated_certifications}</TableCell>
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
