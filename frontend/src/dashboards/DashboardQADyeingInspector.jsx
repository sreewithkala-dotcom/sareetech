import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const QA_DYEING_INSPECTOR_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Quality Inspector' },
  { id: 'inspection', label: 'Dyeing Inspection & Color QA' },
  { id: 'certificates', label: 'Certificates & Product Details' },
  { id: 'forecast', label: 'Sales Forecast & Material Plan' },
]

const DYEING_PROCESS_TYPE_OPTIONS = [
  { value: 'SKEIN_DYE', label: 'Skein Dye [Default]' },
  { value: 'HANK_DYE', label: 'Hank Dye' },
  { value: 'PIECE_DYE', label: 'Piece Dye' },
  { value: 'YARN_DYE', label: 'Yarn Dye' },
]

const COLOR_FASTNESS_GRADE_OPTIONS = [
  { value: 'GRADE_A_EXCELLENT', label: 'GRADE_A_EXCELLENT [Default]' },
  { value: 'GRADE_B_GOOD', label: 'GRADE_B_GOOD' },
  { value: 'GRADE_C_ACCEPTABLE', label: 'GRADE_C_ACCEPTABLE' },
  { value: 'GRADE_D_POOR', label: 'GRADE_D_POOR' },
]

const METAMERISM_RISK_OPTIONS = [
  { value: 'LOW', label: 'LOW [Default]' },
  { value: 'MEDIUM', label: 'MEDIUM' },
  { value: 'HIGH', label: 'HIGH' },
]

const PRIMARY_DYE_DEFECT_CODE_OPTIONS = [
  { value: 'DEFECT_NONE_CLEAN_BATCH', label: 'DEFECT_NONE_CLEAN_BATCH [Default]' },
  { value: 'SHADE_VARIATION_LOT', label: 'SHADE_VARIATION_LOT' },
  { value: 'DYE_STREAK_MARK', label: 'DYE_STREAK_MARK' },
  { value: 'UNEVEN_PENETRATION', label: 'UNEVEN_PENETRATION' },
  { value: 'COLOR_FASTNESS_FAIL', label: 'COLOR_FASTNESS_FAIL' },
  { value: 'METAMERISM_DETECTED', label: 'METAMERISM_DETECTED' },
]

const BATCH_CLEARANCE_STATUS_OPTIONS = [
  { value: 'CLEARED_FOR_FINISHING', label: 'CLEARED_FOR_FINISHING [Default]' },
  { value: 'HOLD_RE_DYE_REQUIRED', label: 'HOLD_RE_DYE_REQUIRED' },
  { value: 'REJECTED_SCRAP_BATCH', label: 'REJECTED_SCRAP_BATCH' },
]

const QA_DYEING_APPROVAL_STATE_OPTIONS = [
  { value: 'INSPECTION_IN_PROGRESS', label: 'INSPECTION_IN_PROGRESS [Default]' },
  { value: 'PASSED_CLEARED_FOR_FINISHING', label: 'PASSED_CLEARED_FOR_FINISHING' },
  { value: 'HOLD_SECOND_AUDIT_REQUIRED', label: 'HOLD_SECOND_AUDIT_REQUIRED' },
  { value: 'REJECTED_RETURN_TO_DYEING', label: 'REJECTED_RETURN_TO_DYEING' },
]

export default function DashboardQADyeingInspector() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [qualityInspectorLogs, setQualityInspectorLogs] = useState([])
  const [dyeingLogs, setDyeingLogs] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [form, setForm] = useState({
    batch_id: '',
    saree_serial_barcode: '',
    quality_inspector_log_id: '',
    sup_supervisor_log_id: '',
    dye_batch_ref: '',
    color_code: '',
    color_name: '',
    dyeing_process_type: 'SKEIN_DYE',
    color_delta_e: '',
    color_fastness_grade: 'GRADE_A_EXCELLENT',
    shade_variation_detected: false,
    dye_penetration_uniform: true,
    metamerism_risk: 'LOW',
    primary_dye_defect_code: 'DEFECT_NONE_CLEAN_BATCH',
    batch_clearance_status: 'CLEARED_FOR_FINISHING',
    qa_dyeing_approval_state: 'INSPECTION_IN_PROGRESS'
  })

  useEffect(() => {
    fetchQualityInspectorLogs()
    fetchDyeingLogs()
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

  const fetchDyeingLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/qa-dyeing-inspector/logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setDyeingLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch dyeing logs:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/qa-dyeing-inspector/certificates`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/qa-dyeing-inspector`, {
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
        color_delta_e: form.color_delta_e ? parseFloat(form.color_delta_e) : null,
      }

      const response = await fetch(`${API_URL}/qa-dyeing-inspector/logs`, {
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
        fetchDyeingLogs()
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
      const response = await fetch(`${API_URL}/qa-dyeing-inspector/logs/${logId}/certify`, {
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
        fetchDyeingLogs()
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
      case 'PASSED_CLEARED_FOR_FINISHING':
      case 'GRADE_A_EXCELLENT':
      case 'DEFECT_NONE_CLEAN_BATCH':
        return 'success'
      case 'HOLD_SECOND_AUDIT_REQUIRED':
      case 'REJECTED_RETURN_TO_DYEING':
      case 'GRADE_D_POOR':
        return 'error'
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
            QA Dyeing Inspector — Color QA
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
          {QA_DYEING_INSPECTOR_TABS.map((t) => (
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
                <Typography color="text.secondary">No approved quality inspector logs found. Quality Inspector must submit and clear inspection before QA Dyeing Inspector can start dye inspection.</Typography>
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
                    label="Batch ID"
                    value={form.batch_id}
                    onChange={(e) => setForm({ ...form, batch_id: e.target.value })}
                    placeholder="BATCH-DYE-001"
                    required
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Saree Serial Barcode"
                    value={form.saree_serial_barcode}
                    onChange={(e) => setForm({ ...form, saree_serial_barcode: e.target.value })}
                    placeholder="SAR-80W-001"
                  />
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
              </Grid>
            </Paper>
          </Grid>

          {/* Category A: Dye Batch Identification & Color Reference */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category A: Dye Batch Identification & Color Reference
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Dye Batch Ref"
                    value={form.dye_batch_ref}
                    onChange={(e) => setForm({ ...form, dye_batch_ref: e.target.value })}
                    placeholder="DYE-BATCH-001"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Color Code"
                    value={form.color_code}
                    onChange={(e) => setForm({ ...form, color_code: e.target.value })}
                    placeholder="DEEP_MAROON"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Color Name"
                    value={form.color_name}
                    onChange={(e) => setForm({ ...form, color_name: e.target.value })}
                    placeholder="Deep Maroon"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Dyeing Process Type</InputLabel>
                    <Select
                      value={form.dyeing_process_type}
                      label="Dyeing Process Type"
                      onChange={(e) => setForm({ ...form, dyeing_process_type: e.target.value })}
                    >
                      {DYEING_PROCESS_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Category B: Color Consistency Verification */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category B: Color Consistency Verification
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField
                    fullWidth
                    label="Color Delta-E"
                    type="number"
                    value={form.color_delta_e}
                    onChange={(e) => setForm({ ...form, color_delta_e: e.target.value })}
                    inputProps={{ min: 0, step: 0.01 }}
                    helperText="Threshold: ≤1.0"
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Color Fastness Grade</InputLabel>
                    <Select
                      value={form.color_fastness_grade}
                      label="Color Fastness Grade"
                      onChange={(e) => setForm({ ...form, color_fastness_grade: e.target.value })}
                    >
                      {COLOR_FASTNESS_GRADE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Metamerism Risk</InputLabel>
                    <Select
                      value={form.metamerism_risk}
                      label="Metamerism Risk"
                      onChange={(e) => setForm({ ...form, metamerism_risk: e.target.value })}
                    >
                      {METAMERISM_RISK_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Shade Variation Detected</InputLabel>
                    <Select
                      value={form.shade_variation_detected}
                      label="Shade Variation Detected"
                      onChange={(e) => setForm({ ...form, shade_variation_detected: e.target.value })}
                    >
                      <MenuItem value={false}>No</MenuItem>
                      <MenuItem value={true}>Yes</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Dye Penetration Uniform</InputLabel>
                    <Select
                      value={form.dye_penetration_uniform}
                      label="Dye Penetration Uniform"
                      onChange={(e) => setForm({ ...form, dye_penetration_uniform: e.target.value })}
                    >
                      <MenuItem value={true}>Yes</MenuItem>
                      <MenuItem value={false}>No</MenuItem>
                    </Select>
                  </FormControl>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Category C: Defect Classification & Batch Disposition */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Category C: Defect Classification & Batch Disposition
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Primary Dye Defect Code</InputLabel>
                    <Select
                      value={form.primary_dye_defect_code}
                      label="Primary Dye Defect Code"
                      onChange={(e) => setForm({ ...form, primary_dye_defect_code: e.target.value })}
                    >
                      {PRIMARY_DYE_DEFECT_CODE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Batch Clearance Status</InputLabel>
                    <Select
                      value={form.batch_clearance_status}
                      label="Batch Clearance Status"
                      onChange={(e) => setForm({ ...form, batch_clearance_status: e.target.value })}
                    >
                      {BATCH_CLEARANCE_STATUS_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>QA Dyeing Approval State</InputLabel>
                    <Select
                      value={form.qa_dyeing_approval_state}
                      label="QA Dyeing Approval State"
                      onChange={(e) => setForm({ ...form, qa_dyeing_approval_state: e.target.value })}
                    >
                      {QA_DYEING_APPROVAL_STATE_OPTIONS.map((opt) => (
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
                    {submitting ? 'Submitting...' : 'Submit Dyeing Inspection'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          {/* Recent Dyeing Logs */}
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Recent Dyeing Inspections
              </Typography>
              {dyeingLogs.length === 0 ? (
                <Typography color="text.secondary">No dyeing inspections yet</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Inspection ID</TableCell>
                        <TableCell>Batch ID</TableCell>
                        <TableCell>Color</TableCell>
                        <TableCell>Delta-E</TableCell>
                        <TableCell>Fastness</TableCell>
                        <TableCell>Defect Code</TableCell>
                        <TableCell>Approval State</TableCell>
                        <TableCell>Piece Rate</TableCell>
                        <TableCell>B2B Status</TableCell>
                        <TableCell>Actions</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {dyeingLogs.map((log) => (
                        <TableRow key={log.id}>
                          <TableCell>{log.inspection_id}</TableCell>
                          <TableCell>{log.batch_id}</TableCell>
                          <TableCell>{log.color_name}</TableCell>
                          <TableCell>{log.color_delta_e}</TableCell>
                          <TableCell><Chip label={log.color_fastness_grade} color={getApprovalColor(log.color_fastness_grade)} size="small" /></TableCell>
                          <TableCell><Chip label={log.primary_dye_defect_code} color={log.primary_dye_defect_code === 'DEFECT_NONE_CLEAN_BATCH' ? 'success' : 'error'} size="small" /></TableCell>
                          <TableCell><Chip label={log.qa_dyeing_approval_state} color={getApprovalColor(log.qa_dyeing_approval_state)} size="small" /></TableCell>
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
                QA Dyeing Inspector Certificates & Product Details
              </Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates generated yet. Certificates are created when a dyeing inspection is certified.</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag ID</TableCell>
                        <TableCell>Inspection ID</TableCell>
                        <TableCell>Batch ID</TableCell>
                        <TableCell>Color</TableCell>
                        <TableCell>Delta-E</TableCell>
                        <TableCell>Fastness</TableCell>
                        <TableCell>Defect Code</TableCell>
                        <TableCell>Approval State</TableCell>
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
                          <TableCell>{cert.batch_id}</TableCell>
                          <TableCell>{cert.color_name}</TableCell>
                          <TableCell>{cert.color_delta_e}</TableCell>
                          <TableCell><Chip label={cert.color_fastness_grade} color={getApprovalColor(cert.color_fastness_grade)} size="small" /></TableCell>
                          <TableCell><Chip label={cert.primary_dye_defect_code} color={cert.primary_dye_defect_code === 'DEFECT_NONE_CLEAN_BATCH' ? 'success' : 'error'} size="small" /></TableCell>
                          <TableCell><Chip label={cert.qa_dyeing_approval_state} color={getApprovalColor(cert.qa_dyeing_approval_state)} size="small" /></TableCell>
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
                Sales Forecast: QA Dyeing Inspector Material Processing Plan
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
                              <TableCell>Color Code</TableCell>
                              <TableCell>Dyeing Process</TableCell>
                              <TableCell>Delta-E Threshold</TableCell>
                              <TableCell>Fastness Grade</TableCell>
                              <TableCell>Est. Batches</TableCell>
                              <TableCell>Priority</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.material_requirements.map((item, idx) => (
                              <TableRow key={idx}>
                                <TableCell>{item.saree_category}</TableCell>
                                <TableCell><code>{item.design_code}</code></TableCell>
                                <TableCell>{item.color_code}</TableCell>
                                <TableCell>{item.dyeing_process_type}</TableCell>
                                <TableCell>{item.color_delta_e_threshold}</TableCell>
                                <TableCell>{item.color_fastness_grade}</TableCell>
                                <TableCell>{item.estimated_batches}</TableCell>
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
                              <TableCell>Color Code</TableCell>
                              <TableCell>Est. Batches</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {forecast.upcoming_lots.map((lot, idx) => (
                              <TableRow key={idx}>
                                <TableCell><code>{lot.lot_number}</code></TableCell>
                                <TableCell>{lot.saree_category}</TableCell>
                                <TableCell><code>{lot.design_code}</code></TableCell>
                                <TableCell>{lot.color_code}</TableCell>
                                <TableCell>{lot.estimated_batches}</TableCell>
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
