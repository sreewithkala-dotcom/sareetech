import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'
import EnterprisePanel from '../components/EnterprisePanel'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const QUALITY_INSPECTOR_TABS = [
  { id: 'preprocess', label: 'Pre-Process from SUP Loom Floor Supervisor' },
  { id: 'inspection', label: 'Quality Inspection & Grading' },
  { id: 'certificates', label: 'Certificates & Product Details' },
  { id: 'forecast', label: 'Sales Forecast & Material Plan' },
]

const INSPECTION_TABLE_TYPE_OPTIONS = [
  { value: 'ILLUMINATED_LED_DUAL_SURFACE_TABLE', label: 'Illuminated LED Dual-Surface Table [Default]' },
  { value: 'STANDARD_OVERHEAD_LIGHT_TABLE', label: 'Standard Overhead Light Table' },
  { value: 'MANUAL_FLAT_BENCH', label: 'Manual Flat Bench' },
]

const PRIMARY_FABRIC_DEFECT_CODE_OPTIONS = [
  { value: 'DEFECT_NONE_CLEAN_PIECE', label: 'DEFECT_NONE_CLEAN_PIECE [Default]' },
  { value: 'HOOK_MISLIFT_PATTERN_ERROR', label: 'HOOK_MISLIFT_PATTERN_ERROR' },
  { value: 'ZARI_FLOAT_TENSION_FAULT', label: 'ZARI_FLOAT_TENSION_FAULT' },
  { value: 'REED_MARK_GAP', label: 'REED_MARK_GAP' },
  { value: 'WEFT_PICK_BAR_DENSITY_VAR', label: 'WEFT_PICK_BAR_DENSITY_VAR' },
  { value: 'WARP_END_BREAK_MEND_MARK', label: 'WARP_END_BREAK_MEND_MARK' },
]

const ZARI_TARNISH_VISUAL_CHECK_OPTIONS = [
  { value: 'PASSED_FULL_LUSTER', label: 'PASSED_FULL_LUSTER [Default]' },
  { value: 'WARNING_MINOR_DISCOLORATION', label: 'WARNING_MINOR_DISCOLORATION' },
  { value: 'REJECTED_OXIDIZED_ZARI', label: 'REJECTED_OXIDIZED_ZARI' },
]

const FINAL_FABRIC_QUALITY_GRADE_OPTIONS = [
  { value: 'GRADE_A_EXPORT_PREMIUM', label: 'GRADE_A_EXPORT_PREMIUM [Default]' },
  { value: 'GRADE_B_DOMESTIC_MINOR_DEFECT', label: 'GRADE_B_DOMESTIC_MINOR_DEFECT' },
  { value: 'GRADE_C_RESERVE_DISCOUNT', label: 'GRADE_C_RESERVE_DISCOUNT' },
  { value: 'REJECTED_SCRAP', label: 'REJECTED_SCRAP' },
]

const QA_INSPECTOR_APPROVAL_STATE_OPTIONS = [
  { value: 'INSPECTION_IN_PROGRESS', label: 'INSPECTION_IN_PROGRESS [Default]' },
  { value: 'PASSED_CLEARED_FOR_PACKING', label: 'PASSED_CLEARED_FOR_PACKING' },
  { value: 'HOLD_SECOND_AUDIT_REQUIRED', label: 'HOLD_SECOND_AUDIT_REQUIRED' },
  { value: 'REJECTED_RETURN_TO_SUPERVISOR', label: 'REJECTED_RETURN_TO_SUPERVISOR' },
]

export default function DashboardQualityInspector() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [supSupervisorLogs, setSupSupervisorLogs] = useState([])
  const [inspectionLogs, setInspectionLogs] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [form, setForm] = useState({
    saree_serial_barcode: '',
    loom_id_ref: '',
    sup_supervisor_log_id: '',
    inspection_table_type: 'ILLUMINATED_LED_DUAL_SURFACE_TABLE',
    fabric_weight_grams_per_sqm: '',
    pick_density_measured_ppi: '',
    warp_density_measured_epi: '',
    primary_fabric_defect_code: 'DEFECT_NONE_CLEAN_PIECE',
    pallu_length_measured_cm: '',
    total_saree_length_measured_meters: '',
    border_width_symmetry_offset_mm: '',
    zari_tarnish_visual_check: 'PASSED_FULL_LUSTER',
    actual_body_length_meters: '',
    actual_blouse_length_meters: '',
    actual_width_inches: '',
    total_finished_weight_grams: '',
    warp_break_streaks_count: 0,
    weft_barriness_detected: false,
    zari_tarnishing_present: false,
    has_oil_grease_stains: false,
    loose_zari_floats_count: 0,
    final_fabric_quality_grade: 'GRADE_A_EXPORT_PREMIUM',
    qa_inspector_approval_state: 'INSPECTION_IN_PROGRESS'
  })

  useEffect(() => {
    fetchSupSupervisorLogs()
    fetchInspectionLogs()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchSupSupervisorLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/sup-loom-floor-supervisor/logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setSupSupervisorLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch supervisor logs:', error)
    }
  }

  const fetchInspectionLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/quality-inspector/logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setInspectionLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch inspection logs:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/quality-inspector/certificates`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/quality-inspector`, {
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
        fabric_weight_grams_per_sqm: form.fabric_weight_grams_per_sqm ? parseFloat(form.fabric_weight_grams_per_sqm) : null,
        pick_density_measured_ppi: form.pick_density_measured_ppi ? parseFloat(form.pick_density_measured_ppi) : null,
        warp_density_measured_epi: form.warp_density_measured_epi ? parseFloat(form.warp_density_measured_epi) : null,
        pallu_length_measured_cm: form.pallu_length_measured_cm ? parseFloat(form.pallu_length_measured_cm) : null,
        total_saree_length_measured_meters: form.total_saree_length_measured_meters ? parseFloat(form.total_saree_length_measured_meters) : null,
        border_width_symmetry_offset_mm: form.border_width_symmetry_offset_mm ? parseFloat(form.border_width_symmetry_offset_mm) : null,
        actual_body_length_meters: form.actual_body_length_meters ? parseFloat(form.actual_body_length_meters) : null,
        actual_blouse_length_meters: form.actual_blouse_length_meters ? parseFloat(form.actual_blouse_length_meters) : null,
        actual_width_inches: form.actual_width_inches ? parseFloat(form.actual_width_inches) : null,
        total_finished_weight_grams: form.total_finished_weight_grams ? parseInt(form.total_finished_weight_grams) : null,
        warp_break_streaks_count: form.warp_break_streaks_count ? parseInt(form.warp_break_streaks_count) : 0,
        loose_zari_floats_count: form.loose_zari_floats_count ? parseInt(form.loose_zari_floats_count) : 0,
      }

      const response = await fetch(`${API_URL}/quality-inspector/logs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Inspection ${data.inspection_id} submitted`, 'success')
        setValidationResult({ type: 'success', data })
        fetchInspectionLogs()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to submit inspection', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCertify = async (logId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/quality-inspector/logs/${logId}/certify`, {
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
        fetchInspectionLogs()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify inspection', 'error')
    }
  }

  const getApprovalColor = (state) => {
    switch (state) {
      case 'PASSED_CLEARED_FOR_PACKING':
      case 'GRADE_A_EXPORT_PREMIUM':
      case 'PASSED_SHIFT_TARGETS_MET':
        return 'success'
      case 'HOLD_SECOND_AUDIT_REQUIRED':
      case 'REJECTED_RETURN_TO_SUPERVISOR':
      case 'REJECTED_SCRAP':
      case 'GRADE_C_RESERVE_DISCOUNT':
        return 'error'
      case 'INSPECTION_IN_PROGRESS':
      case 'SHIFT_ACTIVE_NORMAL':
      case 'WEAVING_IN_PROGRESS':
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

  const getSeverityColor = (severity) => {
    switch (severity) {
      case 'CRITICAL': return 'error'
      case 'WARNING': return 'warning'
      case 'BLOCK': return 'error'
      default: return 'info'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Quality Inspector — Fabric QA
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
          Inspection {validationResult.data.inspection_id} submitted successfully
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Inspection ID:</strong> {certificateDetail.inspection_id}</Typography>
          <Typography variant="body2"><strong>Hash:</strong> {certificateDetail.certificate_hash}</Typography>
          <Typography variant="body2"><strong>QR Tag:</strong> {certificateDetail.qr_tag_id}</Typography>
        </Alert>
      )}

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="fullWidth">
          {QUALITY_INSPECTOR_TABS.map((t) => (
            <Tab key={t.id} label={t.label} value={t.id} />
          ))}
        </Tabs>
      </Paper>

      {/* ===================== PRE-PROCESS TAB ===================== */}
      {tab === 'preprocess' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>SUP Loom Floor Supervisor Approved Logs (Pre-Process)</Typography>
              {supSupervisorLogs.length === 0 ? (
                <Typography color="text.secondary">No approved supervisor logs found. Supervisor must submit and approve floor operations before Quality Inspector can start inspection.</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Log ID</TableCell>
                        <TableCell>Line ID</TableCell>
                        <TableCell>Humidity (%)</TableCell>
                        <TableCell>Temp (°C)</TableCell>
                        <TableCell>OEE (%)</TableCell>
                        <TableCell>Stops/Hr</TableCell>
                        <TableCell>First Saree Audit</TableCell>
                        <TableCell>Handover State</TableCell>
                        <TableCell>Batch Release</TableCell>
                        <TableCell>Routing</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {supSupervisorLogs.map((log) => (
                        <TableRow key={log.id}>
                          <TableCell>{log.supervisor_log_id}</TableCell>
                          <TableCell>{log.loom_shed_line_id}</TableCell>
                          <TableCell>{log.ambient_relative_humidity_pct}</TableCell>
                          <TableCell>{log.ambient_temperature_celsius}</TableCell>
                          <TableCell>{log.shift_target_oee_pct}</TableCell>
                          <TableCell>{log.loom_stop_rate_per_hour}</TableCell>
                          <TableCell><Chip label={log.first_saree_dimensional_audit} color={getApprovalColor(log.first_saree_dimensional_audit)} size="small" /></TableCell>
                          <TableCell><Chip label={log.shift_handover_approval_state} color={getApprovalColor(log.shift_handover_approval_state)} size="small" /></TableCell>
                          <TableCell><Chip label={log.saree_batch_release_authorization} color={getApprovalColor(log.saree_batch_release_authorization)} size="small" /></TableCell>
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

      {/* ===================== INSPECTION TAB ===================== */}
      {tab === 'inspection' && (
        <Grid container spacing={3}>
          {/* Identity & Production Traceability */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Identity & Production Traceability
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Saree Serial Barcode"
                    value={form.saree_serial_barcode}
                    onChange={(e) => setForm({ ...form, saree_serial_barcode: e.target.value })}
                    placeholder="SAR-80W-001"
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Loom ID Ref"
                    value={form.loom_id_ref}
                    onChange={(e) => setForm({ ...form, loom_id_ref: e.target.value })}
                    placeholder="LOOM-2401"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="SUP Supervisor Log ID"
                    value={form.sup_supervisor_log_id}
                    onChange={(e) => setForm({ ...form, sup_supervisor_log_id: e.target.value })}
                    placeholder="SUP-20240820-1234"
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Category A: Inspection Setup & Material Metadata */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category A: Inspection Setup & Material Metadata
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Inspection Table Type</InputLabel>
                    <Select
                      value={form.inspection_table_type}
                      label="Inspection Table Type"
                      onChange={(e) => setForm({ ...form, inspection_table_type: e.target.value })}
                    >
                      {INSPECTION_TABLE_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Fabric Weight (g/sqm)"
                    type="number"
                    value={form.fabric_weight_grams_per_sqm}
                    onChange={(e) => setForm({ ...form, fabric_weight_grams_per_sqm: e.target.value })}
                    inputProps={{ min: 0, step: 0.1 }}
                    helperText="65.0 - 85.0 g/m²"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Pick Density Measured (PPI)"
                    type="number"
                    value={form.pick_density_measured_ppi}
                    onChange={(e) => setForm({ ...form, pick_density_measured_ppi: e.target.value })}
                    inputProps={{ min: 0, step: 0.1 }}
                    helperText="120.0 - 144.0 PPI"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Warp Density Measured (EPI)"
                    type="number"
                    value={form.warp_density_measured_epi}
                    onChange={(e) => setForm({ ...form, warp_density_measured_epi: e.target.value })}
                    inputProps={{ min: 0, step: 0.1 }}
                    helperText="144.0 EPI nominal"
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Category B: Defect Classification & Dimensional Audit */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category B: Defect Classification & Dimensional Audit
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Primary Fabric Defect Code</InputLabel>
                    <Select
                      value={form.primary_fabric_defect_code}
                      label="Primary Fabric Defect Code"
                      onChange={(e) => setForm({ ...form, primary_fabric_defect_code: e.target.value })}
                    >
                      {PRIMARY_FABRIC_DEFECT_CODE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Pallu Length Measured (cm)"
                    type="number"
                    value={form.pallu_length_measured_cm}
                    onChange={(e) => setForm({ ...form, pallu_length_measured_cm: e.target.value })}
                    inputProps={{ min: 0, step: 0.1 }}
                    helperText="90.0 - 110.0 cm"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Total Saree Length Measured (m)"
                    type="number"
                    value={form.total_saree_length_measured_meters}
                    onChange={(e) => setForm({ ...form, total_saree_length_measured_meters: e.target.value })}
                    inputProps={{ min: 0, step: 0.01 }}
                    helperText="6.28 - 6.32 m"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Border Width Symmetry Offset (mm)"
                    type="number"
                    value={form.border_width_symmetry_offset_mm}
                    onChange={(e) => setForm({ ...form, border_width_symmetry_offset_mm: e.target.value })}
                    inputProps={{ min: 0, step: 0.1 }}
                    helperText="≤0.5 mm"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Zari Tarnish Visual Check</InputLabel>
                    <Select
                      value={form.zari_tarnish_visual_check}
                      label="Zari Tarnish Visual Check"
                      onChange={(e) => setForm({ ...form, zari_tarnish_visual_check: e.target.value })}
                    >
                      {ZARI_TARNISH_VISUAL_CHECK_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Dimensional Metrics (Physical Measurements) */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Dimensional Metrics (Physical Measurements)
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Actual Body Length (m)"
                    type="number"
                    value={form.actual_body_length_meters}
                    onChange={(e) => setForm({ ...form, actual_body_length_meters: e.target.value })}
                    inputProps={{ min: 0, step: 0.01 }}
                    helperText="Target: 5.50 m"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Actual Blouse Length (m)"
                    type="number"
                    value={form.actual_blouse_length_meters}
                    onChange={(e) => setForm({ ...form, actual_blouse_length_meters: e.target.value })}
                    inputProps={{ min: 0, step: 0.01 }}
                    helperText="Target: 0.80 m"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Actual Width (inches)"
                    type="number"
                    value={form.actual_width_inches}
                    onChange={(e) => setForm({ ...form, actual_width_inches: e.target.value })}
                    inputProps={{ min: 0, step: 0.1 }}
                    helperText="Target: 46.0 - 48.0 inches"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Total Finished Weight (grams)"
                    type="number"
                    value={form.total_finished_weight_grams}
                    onChange={(e) => setForm({ ...form, total_finished_weight_grams: e.target.value })}
                    inputProps={{ min: 0, step: 1 }}
                    helperText="Target: 650g - 750g"
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Defect Checklist Toggles */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Defect Checklist
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Warp Break Streaks Count"
                    type="number"
                    value={form.warp_break_streaks_count}
                    onChange={(e) => setForm({ ...form, warp_break_streaks_count: parseInt(e.target.value) || 0 })}
                    inputProps={{ min: 0 }}
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Weft Barriness Detected</InputLabel>
                    <Select
                      value={form.weft_barriness_detected}
                      label="Weft Barriness Detected"
                      onChange={(e) => setForm({ ...form, weft_barriness_detected: e.target.value })}
                    >
                      <MenuItem value={false}>No</MenuItem>
                      <MenuItem value={true}>Yes</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Zari Tarnishing Present</InputLabel>
                    <Select
                      value={form.zari_tarnishing_present}
                      label="Zari Tarnishing Present"
                      onChange={(e) => setForm({ ...form, zari_tarnishing_present: e.target.value })}
                    >
                      <MenuItem value={false}>No</MenuItem>
                      <MenuItem value={true}>Yes</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Has Oil/Grease Stains</InputLabel>
                    <Select
                      value={form.has_oil_grease_stains}
                      label="Has Oil/Grease Stains"
                      onChange={(e) => setForm({ ...form, has_oil_grease_stains: e.target.value })}
                    >
                      <MenuItem value={false}>No</MenuItem>
                      <MenuItem value={true}>Yes</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Loose Zari Floats Count"
                    type="number"
                    value={form.loose_zari_floats_count}
                    onChange={(e) => setForm({ ...form, loose_zari_floats_count: parseInt(e.target.value) || 0 })}
                    inputProps={{ min: 0 }}
                  />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Category C: Quality Grading & Final Batch Disposition */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category C: Quality Grading & Final Batch Disposition
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel>Final Fabric Quality Grade</InputLabel>
                    <Select
                      value={form.final_fabric_quality_grade}
                      label="Final Fabric Quality Grade"
                      onChange={(e) => setForm({ ...form, final_fabric_quality_grade: e.target.value })}
                    >
                      {FINAL_FABRIC_QUALITY_GRADE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel>QA Inspector Approval State</InputLabel>
                    <Select
                      value={form.qa_inspector_approval_state}
                      label="QA Inspector Approval State"
                      onChange={(e) => setForm({ ...form, qa_inspector_approval_state: e.target.value })}
                    >
                      {QA_INSPECTOR_APPROVAL_STATE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12}>
                  <Button
                    variant="contained"
                    size="large"
                    onClick={handleSubmit}
                    disabled={submitting}
                    fullWidth
                  >
                    {submitting ? 'Submitting...' : 'Submit Inspection Report'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Recent Inspection Logs */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Recent Inspection Logs
              </Typography>
              {inspectionLogs.length === 0 ? (
                <Typography color="text.secondary">No inspection logs yet</Typography>
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
                        <TableCell>Piece Rate</TableCell>
                        <TableCell>B2B Status</TableCell>
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {inspectionLogs.map((log) => (
                        <TableRow key={log.id}>
                          <TableCell>{log.inspection_id}</TableCell>
                          <TableCell>{log.saree_serial_barcode}</TableCell>
                          <TableCell>{log.loom_id_ref}</TableCell>
                          <TableCell><Chip label={log.primary_fabric_defect_code} color={log.primary_fabric_defect_code === 'DEFECT_NONE_CLEAN_PIECE' ? 'success' : 'error'} size="small" /></TableCell>
                          <TableCell>{log.total_saree_length_measured_meters}</TableCell>
                          <TableCell><Chip label={log.final_fabric_quality_grade} color={getApprovalColor(log.final_fabric_quality_grade)} size="small" /></TableCell>
                          <TableCell><Chip label={log.qa_inspector_approval_state} color={getApprovalColor(log.qa_inspector_approval_state)} size="small" /></TableCell>
                          <TableCell><Chip label={log.piece_rate_release_status} color={log.piece_rate_release_status === 'RELEASED_FULL' ? 'success' : 'warning'} size="small" /></TableCell>
                          <TableCell><Chip label={log.b2b_order_status} color={log.b2b_order_status === 'MATCHED_READY_FOR_PACKING' ? 'success' : 'default'} size="small" /></TableCell>
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
                Quality Inspector Certificates & Product Details
              </Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates generated yet. Certificates are created when an inspection is certified.</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag ID</TableCell>
                        <TableCell>Inspection ID</TableCell>
                        <TableCell>Saree Serial</TableCell>
                        <TableCell>Loom ID</TableCell>
                        <TableCell>Length (m)</TableCell>
                        <TableCell>Pallu (cm)</TableCell>
                        <TableCell>Defect Code</TableCell>
                        <TableCell>Grade</TableCell>
                        <TableCell>Piece Rate</TableCell>
                        <TableCell>B2B Status</TableCell>
                        <TableCell>Status</TableCell>
                        <TableCell>Certified At</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {certificates.map((cert) => (
                        <TableRow key={cert.id}>
                          <TableCell><code>{cert.certificate_hash}</code></TableCell>
                          <TableCell><code>{cert.qr_tag_id}</code></TableCell>
                          <TableCell>{cert.inspection_id}</TableCell>
                          <TableCell>{cert.saree_serial_barcode}</TableCell>
                          <TableCell>{cert.loom_id_ref}</TableCell>
                          <TableCell>{cert.total_saree_length_measured_meters}</TableCell>
                          <TableCell>{cert.pallu_length_measured_cm}</TableCell>
                          <TableCell><Chip label={cert.primary_fabric_defect_code} color={cert.primary_fabric_defect_code === 'DEFECT_NONE_CLEAN_PIECE' ? 'success' : 'error'} size="small" /></TableCell>
                          <TableCell><Chip label={cert.final_fabric_quality_grade} color={getApprovalColor(cert.final_fabric_quality_grade)} size="small" /></TableCell>
                          <TableCell>
                            <Chip label={cert.piece_rate_release_status} color={cert.piece_rate_release_status === 'RELEASED_FULL' ? 'success' : 'warning'} size="small" />
                            {cert.piece_rate_penalty_applied && (
                              <Typography variant="caption" display="block" color="error">
                                -{cert.piece_rate_penalty_percent}%
                              </Typography>
                            )}
                          </TableCell>
                          <TableCell><Chip label={cert.b2b_order_status} color={cert.b2b_order_status === 'MATCHED_READY_FOR_PACKING' ? 'success' : 'default'} size="small" /></TableCell>
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
                Sales Forecast: Quality Inspector Material Processing Plan
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
                              <TableCell>Inspection Table</TableCell>
                              <TableCell>Pick Density Tolerance</TableCell>
                              <TableCell>Zari Float Tolerance (mm)</TableCell>
                              <TableCell>Dimensional Tolerance (cm)</TableCell>
                              <TableCell>Est. Inspections</TableCell>
                              <TableCell>Priority</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.material_requirements.map((item, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{item.saree_category}</TableCell>
                                <TableCell><code>{item.design_code}</code></TableCell>
                                <TableCell>{item.inspection_table_type}</TableCell>
                                <TableCell>{item.pick_density_tolerance}</TableCell>
                                <TableCell>{item.zari_float_tolerance_mm}</TableCell>
                                <TableCell>{item.dimensional_tolerance_cm}</TableCell>
                                <TableCell>{item.estimated_inspections}</TableCell>
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
                              <TableCell>Est. Inspections</TableCell>
                              <TableCell>Inspection Table</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.upcoming_lots.map((lot, idx) => (
                              <TableRow key={idx}>
                                <TableCell><code>{lot.lot_number}</code></TableCell>
                                <TableCell>{lot.saree_category}</TableCell>
                                <TableCell><code>{lot.design_code}</code></TableCell>
                                <TableCell>{lot.estimated_inspections}</TableCell>
                                <TableCell>{lot.inspection_table_type}</TableCell>
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
      <EnterprisePanel userId={user?.id} factoryNodeId={user?.factory_node_id} />
    </Container>
  )
}
