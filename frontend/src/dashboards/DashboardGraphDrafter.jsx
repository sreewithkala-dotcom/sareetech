import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'
import EnterprisePanel from '../components/EnterprisePanel'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const DESIGN_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Pirn Winder' },
  { id: 'design-creation', label: 'Graph Design & Hook Mapping' },
  { id: 'weave-rules', label: 'Weave Rules & Structural Selection' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const TARGET_HOOK_CAPACITY_OPTIONS = [
  { value: 2400, label: '2400 Hook [Default]' },
  { value: 1536, label: '1536 Hook' },
  { value: 1200, label: '1200 Hook' },
  { value: 600, label: '600 Hook' },
]

const HOOK_ALLOCATION_PROFILE_OPTIONS = [
  { value: '2250_DESIGN_150_BORDER_SELVAGE', label: '2250 Design + 150 Border/Selvage [Default]' },
  { value: '2304_DESIGN_96_BORDER_SELVAGE', label: '2304 Design + 96 Border/Selvage' },
  { value: '2160_DESIGN_240_DOUBLE_BORDER', label: '2160 Design + 240 Double Border' },
  { value: 'CUSTOM_ALLOCATION', label: 'Custom Allocation' },
]

const GROUND_WEAVE_STRUCTURE_OPTIONS = [
  { value: '16_END_SHADED_SATIN', label: '16-End Shaded Satin [Default]' },
  { value: '8_END_WARP_SATIN', label: '8-End Warp Satin' },
  { value: '5_END_WEFT_TWILL', label: '5-End Weft Twill' },
  { value: '1_1_PLAIN_WEAVE', label: '1/1 Plain Weave' },
  { value: 'DAMASK_INTERLOCK', label: 'Damask Interlock' },
]

const ZARI_BINDING_WEAVE_TYPE_OPTIONS = [
  { value: '8_END_SATIN_INTERLOCK', label: '8-End Satin Interlock [Default]' },
  { value: '5_END_TWILL_CATCH', label: '5-End Twill Catch' },
  { value: 'PLAIN_WEAVE_GROUND', label: 'Plain Weave Ground' },
  { value: '3_1_BROKEN_TWILL', label: '3/1 Broken Twill' },
]

const BORDER_BINDING_TECHNIQUE_OPTIONS = [
  { value: 'MICRO_STEP_CATCHING', label: 'Micro-Step Catching [Default]' },
  { value: 'STANDARD_1_PIXEL_CATCH', label: 'Standard 1-Pixel Catch' },
  { value: 'INTERLOCKING_SATIN_BORDER', label: 'Interlocking Satin Border' },
  { value: 'SAWTOOTH_EDGE_LOCK', label: 'Sawtooth Edge Lock' },
]

const SHADING_TECHNIQUE_OPTIONS = [
  { value: 'MULTI_LEVEL_SHADED_SATIN', label: 'Multi-Level Shaded Satin [Default]' },
  { value: 'DITHERED_POINT_PAPER', label: 'Ditched Point-Paper' },
  { value: 'CROSS_HATCH_HATCHING', label: 'Cross-Hatch Hatching' },
  { value: 'SOLID_FILL', label: 'Solid Fill' },
]

const MAX_FLOAT_ENFORCEMENT_RULE_OPTIONS = [
  { value: 'STRICT_WARP_LE_4_WEFT_LE_5', label: 'Strict Warp ≤ 4 / Weft ≤ 5 [Default]' },
  { value: 'STANDARD_WARP_LE_7_WEFT_LE_8', label: 'Standard Warp ≤ 7 / Weft ≤ 8' },
  { value: 'CUSTOM_LIMIT', label: 'Custom Limit' },
  { value: 'DISABLED_RAW_IMPORT', label: 'Disabled (Raw Import)' },
]

const CAD_OUTPUT_FORMAT_OPTIONS = [
  { value: 'JC5', label: '.JC5 (Stäubli) [Default]' },
  { value: '.EP', label: '.EP (Bonas)' },
  { value: '.DAT', label: '.DAT (Nedgraphics)' },
  { value: '.BMP', label: '.BMP (Generic Bitmap)' },
]

const DESIGN_APPROVAL_STATE_OPTIONS = [
  { value: 'DRAFT', label: 'DRAFT [Default]' },
  { value: 'PENDING_FLOAT_CHECK', label: 'PENDING_FLOAT_CHECK' },
  { value: 'APPROVED_FOR_PUNCHING', label: 'APPROVED_FOR_PUNCHING' },
  { value: 'REJECTED_FLOAT_EXCEEDED', label: 'REJECTED_FLOAT_EXCEEDED' },
  { value: 'ARCHIVED', label: 'ARCHIVED' },
]

export default function DashboardGraphDrafter() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)

  const [pirnJobs, setPirnJobs] = useState([])
  const [designs, setDesigns] = useState([])
  const [designIterations, setDesignIterations] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [designForm, setDesignForm] = useState({
    design_master_id: '',
    graph_iteration_v: 1.0,
    target_hook_capacity: 2400,
    top_border_hooks: 150,
    bottom_border_hooks: 150,
    body_motif_hooks: 2100,
    selvedge_hooks: 100,
    grid_width_pixels: 2400,
    grid_height_picks: '',
    maximum_float_length: 7,
    digital_cad_file_upload: '',
    hook_allocation_profile: '2250_DESIGN_150_BORDER_SELVAGE',
    warp_ends_per_inch_epi: 144,
    weft_picks_per_inch_ppi: 120,
    ground_weave_structure: '16_END_SHADED_SATIN',
    zari_binding_weave_type: '8_END_SATIN_INTERLOCK',
    border_binding_technique: 'MICRO_STEP_CATCHING',
    shading_technique: 'MULTI_LEVEL_SHADED_SATIN',
    max_float_enforcement_rule: 'STRICT_WARP_LE_4_WEFT_LE_5',
    max_warp_float_ends: 4,
    max_weft_float_picks: 5,
    selvage_hook_count: 100,
    cad_output_format: 'JC5',
    design_approval_state: 'DRAFT'
  })

  const [iterationForm, setIterationForm] = useState({
    iteration_v: 1.1,
    change_reason: '',
    grid_width_pixels: 2400,
    grid_height_picks: '',
    hook_allocation_profile: '2250_DESIGN_150_BORDER_SELVAGE',
    ground_weave_structure: '16_END_SHADED_SATIN',
    max_float_enforcement_rule: 'STRICT_WARP_LE_4_WEFT_LE_5',
    max_warp_float_ends: 4,
    max_weft_float_picks: 5
  })

  useEffect(() => {
    fetchPirnJobs()
    fetchDesigns()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  const fetchPirnJobs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/pirn-winding/jobs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setPirnJobs(data.jobs || [])
    } catch (error) {
      console.error('Failed to fetch pirn jobs:', error)
    }
  }

  const fetchDesigns = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/design/graphs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setDesigns(data.designs || [])
    } catch (error) {
      console.error('Failed to fetch designs:', error)
    }
  }

  const fetchCertificates = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/design/certificates`, {
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
      const response = await fetch(`${API_URL}/sales/forecast/design`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setForecast(data)
    } catch (error) {
      console.error('Failed to fetch sales forecast:', error)
    }
  }

  const handleDesignSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...designForm,
        grid_height_picks: designForm.grid_height_picks ? parseInt(designForm.grid_height_picks) : null,
        warp_ends_per_inch_epi: designForm.warp_ends_per_inch_epi ? parseInt(designForm.warp_ends_per_inch_epi) : null,
        weft_picks_per_inch_ppi: designForm.weft_picks_per_inch_ppi ? parseInt(designForm.weft_picks_per_inch_ppi) : null,
        top_border_hooks: parseInt(designForm.top_border_hooks) || 0,
        bottom_border_hooks: parseInt(designForm.bottom_border_hooks) || 0,
        body_motif_hooks: parseInt(designForm.body_motif_hooks) || 0,
        selvedge_hooks: parseInt(designForm.selvedge_hooks) || 0
      }

      const response = await fetch(`${API_URL}/design/graphs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Design ${data.design_master_id} created`, 'success')
        setValidationResult({ type: 'success', data })
        setDesignForm({
          design_master_id: '', graph_iteration_v: 1.0, target_hook_capacity: 2400,
          top_border_hooks: 150, bottom_border_hooks: 150, body_motif_hooks: 2100,
          selvedge_hooks: 100, grid_width_pixels: 2400, grid_height_picks: '',
          maximum_float_length: 7, digital_cad_file_upload: '',
          hook_allocation_profile: '2250_DESIGN_150_BORDER_SELVAGE',
          warp_ends_per_inch_epi: 144, weft_picks_per_inch_ppi: 120,
          ground_weave_structure: '16_END_SHADED_SATIN',
          zari_binding_weave_type: '8_END_SATIN_INTERLOCK',
          border_binding_technique: 'MICRO_STEP_CATCHING',
          shading_technique: 'MULTI_LEVEL_SHADED_SATIN',
          max_float_enforcement_rule: 'STRICT_WARP_LE_4_WEFT_LE_5',
          max_warp_float_ends: 4, max_weft_float_picks: 5,
          selvage_hook_count: 100, cad_output_format: 'JC5',
          design_approval_state: 'DRAFT'
        })
        fetchDesigns()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create design', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleApprove = async (designId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/design/graphs/${designId}/approve`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ notes: 'Approved by Graph Drafter' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Design approved: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchDesigns()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Approval failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to approve design', 'error')
    }
  }

  const handleReject = async (designId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/design/graphs/${designId}/reject`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason: 'Rejected by Graph Drafter' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Design rejected', 'warning')
        fetchDesigns()
      } else {
        addNotification(data.error || 'Rejection failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to reject design', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'APPROVED_FOR_PUNCHING': return 'success'
      case 'PENDING_FLOAT_CHECK': return 'warning'
      case 'REJECTED_FLOAT_EXCEEDED': return 'error'
      case 'ARCHIVED': return 'default'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'JACQUARD_2400_READY': return 'success'
      case 'JACQUARD_1536_READY': return 'info'
      case 'CAD_EXPORT_FAILED': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Graph Drafter (2400 Hook) — Jacquard Design & CAD Graph Translation
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
          {DESIGN_TABS.map((t) => (
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
          Design {validationResult.data.design_master_id} created successfully
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Design:</strong> {certificateDetail.design_master_id}</Typography>
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
              <Typography variant="h6" gutterBottom>Pirn Winding Jobs (Pre-Process)</Typography>
              {pirnJobs.length === 0 ? (
                <Typography color="text.secondary">No pirn winding jobs found</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job No</TableCell>
                        <TableCell>Machine</TableCell>
                        <TableCell>Source Bobbin Lot</TableCell>
                        <TableCell>Pirn Count</TableCell>
                        <TableCell>Input (kg)</TableCell>
                        <TableCell>Output (kg)</TableCell>
                        <TableCell>Routing</TableCell>
                        <TableCell>Status</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {pirnJobs.map((job) => (
                        <TableRow key={job.id}>
                          <TableCell>{job.pirn_winding_job_no}</TableCell>
                          <TableCell>{job.winding_machine_id}</TableCell>
                          <TableCell>{job.source_bobbin_lot_no}</TableCell>
                          <TableCell>{job.target_pirn_count_qty}</TableCell>
                          <TableCell>{job.input_yarn_weight_kg}</TableCell>
                          <TableCell>{job.output_pirn_net_weight_kg}</TableCell>
                          <TableCell><Chip label={job.auto_assigned_routing} color={getRoutingColor(job.auto_assigned_routing)} size="small" /></TableCell>
                          <TableCell><Chip label={job.status} color={getStatusColor(job.status)} size="small" /></TableCell>
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

      {/* ===================== DESIGN CREATION TAB ===================== */}
      {tab === 'design-creation' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>1. Design Identity & Version Control</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Design Master ID" value={designForm.design_master_id}
                    onChange={(e) => setDesignForm({ ...designForm, design_master_id: e.target.value })}
                    placeholder="e.g., DES-2026-BR-09" required />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Graph Iteration V" type="number"
                    value={designForm.graph_iteration_v}
                    onChange={(e) => setDesignForm({ ...designForm, graph_iteration_v: parseFloat(e.target.value) || 1.0 })}
                    inputProps={{ step: '0.1', min: 0.1 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Target Hook Capacity</InputLabel>
                    <Select value={designForm.target_hook_capacity} label="Target Hook Capacity"
                      onChange={(e) => setDesignForm({ ...designForm, target_hook_capacity: parseInt(e.target.value) })}>
                      {TARGET_HOOK_CAPACITY_OPTIONS.map((opt) => (
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
              <Typography variant="h6" gutterBottom>2. Jacquard Architecture & Hook Mapping (2400 Configuration)</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Top Border Hooks" type="number"
                    value={designForm.top_border_hooks}
                    onChange={(e) => setDesignForm({ ...designForm, top_border_hooks: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Bottom Border Hooks" type="number"
                    value={designForm.bottom_border_hooks}
                    onChange={(e) => setDesignForm({ ...designForm, bottom_border_hooks: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Body Motif Hooks" type="number"
                    value={designForm.body_motif_hooks}
                    onChange={(e) => setDesignForm({ ...designForm, body_motif_hooks: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={3}>
                  <TextField fullWidth label="Selvedge Hooks" type="number"
                    value={designForm.selvedge_hooks}
                    onChange={(e) => setDesignForm({ ...designForm, selvedge_hooks: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Hook Allocation Profile</InputLabel>
                    <Select value={designForm.hook_allocation_profile} label="Hook Allocation Profile"
                      onChange={(e) => setDesignForm({ ...designForm, hook_allocation_profile: e.target.value })}>
                      {HOOK_ALLOCATION_PROFILE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Grid Width (pixels)" type="number"
                    value={designForm.grid_width_pixels}
                    onChange={(e) => setDesignForm({ ...designForm, grid_width_pixels: parseInt(e.target.value) || 2400 })}
                    inputProps={{ step: '1', min: 1 }} />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Grid Height (picks)" type="number"
                    value={designForm.grid_height_picks}
                    onChange={(e) => setDesignForm({ ...designForm, grid_height_picks: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} />
                </Grid>
              </Grid>
              <Alert severity="info" sx={{ mt: 2 }}>
                <Typography variant="body2">
                  <strong>2400 Hook Mathematical Gate:</strong> (Top {designForm.top_border_hooks} + Bottom {designForm.bottom_border_hooks} + Body {designForm.body_motif_hooks} + Selvedge {designForm.selvedge_hooks}) = {parseInt(designForm.top_border_hooks || 0) + parseInt(designForm.bottom_border_hooks || 0) + parseInt(designForm.body_motif_hooks || 0) + parseInt(designForm.selvedge_hooks || 0)} hooks
                  {parseInt(designForm.top_border_hooks || 0) + parseInt(designForm.bottom_border_hooks || 0) + parseInt(designForm.body_motif_hooks || 0) + parseInt(designForm.selvedge_hooks || 0) !== 2400 && designForm.target_hook_capacity === 2400 && (
                    <span style={{ color: 'red' }}> — Must equal exactly 2400!</span>
                  )}
                </Typography>
              </Alert>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>3. Category A: Design Metadata & Resolution Control</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Warp Ends per Inch (EPI)" type="number"
                    value={designForm.warp_ends_per_inch_epi}
                    onChange={(e) => setDesignForm({ ...designForm, warp_ends_per_inch_epi: e.target.value })}
                    inputProps={{ step: '1', min: 1 }} helperText="e.g., 144 for 2400-hook" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Weft Picks per Inch (PPI)" type="number"
                    value={designForm.weft_picks_per_inch_ppi}
                    onChange={(e) => setDesignForm({ ...designForm, weft_picks_per_inch_ppi: e.target.value })}
                    inputProps={{ step: '1', min: 1 }} helperText="e.g., 120 for fine Jacquard" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Graph Aspect Ratio" type="number"
                    value={designForm.warp_ends_per_inch_epi && designForm.weft_picks_per_inch_ppi ? 
                      (parseInt(designForm.warp_ends_per_inch_epi) / parseInt(designForm.weft_picks_per_inch_ppi)).toFixed(3) : ''}
                    disabled helperText="Auto-calculated EPI/PPI" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Max Float Enforcement Rule</InputLabel>
                    <Select value={designForm.max_float_enforcement_rule} label="Max Float Enforcement Rule"
                      onChange={(e) => setDesignForm({ ...designForm, max_float_enforcement_rule: e.target.value })}>
                      {MAX_FLOAT_ENFORCEMENT_RULE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Max Warp Float Ends" type="number"
                    value={designForm.max_warp_float_ends}
                    onChange={(e) => setDesignForm({ ...designForm, max_warp_float_ends: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="Target: ≤4 for 2400-hook" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="Max Weft Float Picks" type="number"
                    value={designForm.max_weft_float_picks}
                    onChange={(e) => setDesignForm({ ...designForm, max_weft_float_picks: e.target.value })}
                    inputProps={{ step: '1', min: 0 }} helperText="Target: ≤5 for 2400-hook" />
                </Grid>
              </Grid>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Box sx={{ display: 'flex', gap: 2 }}>
                <Button variant="contained" onClick={handleDesignSubmit} disabled={submitting}>
                  {submitting ? 'Submitting...' : 'Save Design Graph'}
                </Button>
              </Box>
            </Paper>
          </Grid>

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Design Graphs</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Design ID</TableCell>
                      <TableCell>Iteration</TableCell>
                      <TableCell>Hook Capacity</TableCell>
                      <TableCell>Allocated Hooks</TableCell>
                      <TableCell>Grid (WxH)</TableCell>
                      <TableCell>EPI/PPI</TableCell>
                      <TableCell>Aspect Ratio</TableCell>
                      <TableCell>Max Float</TableCell>
                      <TableCell>Approval State</TableCell>
                      <TableCell>Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {designs.map((design) => (
                      <TableRow key={design.id}>
                        <TableCell>{design.design_master_id}</TableCell>
                        <TableCell>v{design.graph_iteration_v}</TableCell>
                        <TableCell>{design.target_hook_capacity}</TableCell>
                        <TableCell>{design.total_allocated_hooks}</TableCell>
                        <TableCell>{design.grid_width_pixels} x {design.grid_height_picks}</TableCell>
                        <TableCell>{design.warp_ends_per_inch_epi}/{design.weft_picks_per_inch_ppi}</TableCell>
                        <TableCell>{design.graph_aspect_ratio}</TableCell>
                        <TableCell>{design.max_warp_float_ends} / {design.max_weft_float_picks}</TableCell>
                        <TableCell><Chip label={design.design_approval_state} color={getStatusColor(design.design_approval_state)} size="small" /></TableCell>
                        <TableCell>
                          {(design.design_approval_state === 'DRAFT' || design.design_approval_state === 'PENDING_FLOAT_CHECK') && (
                            <>
                              <Button size="small" variant="outlined" color="success" onClick={() => handleApprove(design.id)}>Approve</Button>
                              <Button size="small" variant="outlined" color="error" onClick={() => handleReject(design.id)} sx={{ ml: 1 }}>Reject</Button>
                            </>
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

      {/* ===================== WEAVE RULES TAB ===================== */}
      {tab === 'weave-rules' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>1. Weave Rules & Structural Selection</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Ground Weave Structure</InputLabel>
                    <Select value={designForm.ground_weave_structure} label="Ground Weave Structure"
                      onChange={(e) => setDesignForm({ ...designForm, ground_weave_structure: e.target.value })}>
                      {GROUND_WEAVE_STRUCTURE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Zari Binding Weave Type</InputLabel>
                    <Select value={designForm.zari_binding_weave_type} label="Zari Binding Weave Type"
                      onChange={(e) => setDesignForm({ ...designForm, zari_binding_weave_type: e.target.value })}>
                      {ZARI_BINDING_WEAVE_TYPE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Border Binding Technique</InputLabel>
                    <Select value={designForm.border_binding_technique} label="Border Binding Technique"
                      onChange={(e) => setDesignForm({ ...designForm, border_binding_technique: e.target.value })}>
                      {BORDER_BINDING_TECHNIQUE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Shading Technique</InputLabel>
                    <Select value={designForm.shading_technique} label="Shading Technique"
                      onChange={(e) => setDesignForm({ ...designForm, shading_technique: e.target.value })}>
                      {SHADING_TECHNIQUE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>CAD Output Format</InputLabel>
                    <Select value={designForm.cad_output_format} label="CAD Output Format"
                      onChange={(e) => setDesignForm({ ...designForm, cad_output_format: e.target.value })}>
                      {CAD_OUTPUT_FORMAT_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth>
                    <InputLabel>Design Approval State</InputLabel>
                    <Select value={designForm.design_approval_state} label="Design Approval State"
                      onChange={(e) => setDesignForm({ ...designForm, design_approval_state: e.target.value })}>
                      {DESIGN_APPROVAL_STATE_OPTIONS.map((opt) => (
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
              <Typography variant="h6" gutterBottom>2. Automated ERP Routing & Design Validation Rules</Typography>
              <Alert severity="info">
                <Typography variant="subtitle2">Automated Design Validation Gates</Typography>
                <Typography variant="body2">
                  • 2400 Hook + Max Warp Float > 4 OR Float Rule = Disabled → REJECT_DESIGN_FILE (EXCESSIVE_FLOAT_LENGTH_RISKS_WARP_SNAG)<br/>
                  • 2400 Hook + Total Allocated Hooks > 2400 → CAD_EXPORT_FAILED (EXCEEDS_2400_PHYSICAL_HOOK_LIMIT)<br/>
                  • Aspect Ratio ≠ EPI/PPI → MOTIF_DISTORTION_DETECTED (CHECK_PIXEL_SCALE)<br/>
                  • Design Approval State ≠ APPROVED_FOR_PUNCHING → PREVENT_FILE_EXPORT (.JC5 / .EP / .DAT)
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
              <Typography variant="h6" gutterBottom>Design Certificates — Post-Process Output</Typography>
              {certificates.length === 0 ? (
                <Typography color="text.secondary">No certificates issued yet</Typography>
              ) : (
                <TableContainer>
                  <Table>
                    <TableHead>
                      <TableRow>
                        <TableCell>Certificate Hash</TableCell>
                        <TableCell>QR Tag</TableCell>
                        <TableCell>Design ID</TableCell>
                        <TableCell>Hooks</TableCell>
                        <TableCell>Allocation Profile</TableCell>
                        <TableCell>Grid (WxH)</TableCell>
                        <TableCell>Max Float</TableCell>
                        <TableCell>CAD Format</TableCell>
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
                          <TableCell>{cert.design_master_id_ref}</TableCell>
                          <TableCell>{cert.total_hook_capacity}</TableCell>
                          <TableCell>{cert.hook_allocation_profile}</TableCell>
                          <TableCell>{cert.grid_width_pixels} x {cert.grid_height_picks}</TableCell>
                          <TableCell>{cert.max_warp_float_ends} / {cert.max_weft_float_picks}</TableCell>
                          <TableCell>{cert.cad_output_format}</TableCell>
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
              <Typography variant="h6" gutterBottom>Sales Forecast — Graph Drafter Material Processing Plan</Typography>
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
                          <Typography color="textSecondary" gutterBottom>Design Lines</Typography>
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
                          <TableCell>Design Code</TableCell>
                          <TableCell>Hook Capacity</TableCell>
                          <TableCell>Allocation Profile</TableCell>
                          <TableCell>Est. Designs</TableCell>
                          <TableCell>CAD Format</TableCell>
                          <TableCell>Priority</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.material_requirements.map((item, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{item.saree_category}</TableCell>
                            <TableCell>{item.design_code}</TableCell>
                            <TableCell>{item.target_hook_capacity}</TableCell>
                            <TableCell>{item.hook_allocation_profile}</TableCell>
                            <TableCell>{item.estimated_designs}</TableCell>
                            <TableCell>{item.cad_output_format}</TableCell>
                            <TableCell><Chip label={item.priority} color={item.priority === 'HIGH' ? 'error' : 'warning'} size="small" /></TableCell>
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
                          <TableCell>Design Code</TableCell>
                          <TableCell>Est. Designs</TableCell>
                          <TableCell>Hook Capacity</TableCell>
                          <TableCell>CAD Format</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {forecast.upcoming_lots.map((lot, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{lot.lot_number}</TableCell>
                            <TableCell>{lot.saree_category}</TableCell>
                            <TableCell>{lot.design_code}</TableCell>
                            <TableCell>{lot.estimated_designs}</TableCell>
                            <TableCell>{lot.target_hook_capacity}</TableCell>
                            <TableCell>{lot.cad_output_format}</TableCell>
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
