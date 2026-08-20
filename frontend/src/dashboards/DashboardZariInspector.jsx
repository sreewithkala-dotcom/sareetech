import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider, Checkbox, FormControlLabel } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

const ZARI_TABS = [
  { id: 'preprocess', label: 'Pre-Process from Zari Refinery' },
  { id: 'xrf-purity', label: 'XRF & Purity Verification' },
  { id: 'physical-geometrics', label: 'Physical & Geometrics Inspection' },
  { id: 'aesthetic-weight', label: 'Aesthetic & Weight Audit' },
  { id: 'defect-routing', label: 'Defect Logging & ERP Routing' },
  { id: 'certificates-forecast', label: 'Certificates & Sales Forecast' },
]

const ZARI_TYPE_OPTIONS = [
  { value: 'REAL_ZARI_GOLD_SILVER', label: 'Real Zari (Gold & Silver)' },
  { value: 'REAL_ZARI_PURE_SILVER_WHITE', label: 'Real Zari (Pure Silver White)' },
  { value: 'HALF_FINE_ZARI_SILVER_COPPER', label: 'Half-Fine Zari (Silver-Copper)' },
  { value: 'HALF_FINE_ZARI_NICKEL_CORE', label: 'Half-Fine Zari (Nickel Core)' },
  { value: 'IMITATION_ZARI_METALLIZED_POLYESTER', label: 'Imitation Zari (Metallized Polyester)' },
  { value: 'PLASTIC_ZARI_LUREX', label: 'Plastic Zari Lurex' },
]

const ORIGIN_CLUSTER_OPTIONS = [
  { value: 'SURAT_GUJARAT', label: 'Surat, Gujarat' },
  { value: 'KANCHIPURAM_TAMIL_NADU', label: 'Kanchipuram, Tamil Nadu' },
  { value: 'VARANASI_UTTAR_PRADESH', label: 'Varanasi, Uttar Pradesh' },
  { value: 'KYOTO_JAPAN', label: 'Kyoto, Japan' },
  { value: 'LYON_FRANCE', label: 'Lyon, France' },
  { value: 'CHANGZHOU_CHINA', label: 'Changzhou, China' },
]

const CORE_YARN_OPTIONS = [
  { value: 'PURE_SILK_RED_MAROON_DYED', label: 'Pure Silk (Red/Maroon Dyed)' },
  { value: 'PURE_SILK_UN_DYED_WHITE', label: 'Pure Silk (Undyed White)' },
  { value: 'PURE_COTTON_COMBED_FINE', label: 'Pure Cotton (Combed Fine)' },
  { value: 'POLYESTER_FILAMENT_HIGH_TENACITY', label: 'Polyester Filament (High Tenacity)' },
  { value: 'VISCOSE_RAYON_CORE', label: 'Viscose Rayon Core' },
  { value: 'NYLON_MONOFILAMENT', label: 'Nylon Monofilament' },
]

const DENIER_OPTIONS = [
  { value: '13/15_DENIER', label: '13/15 Denier — Ultra-fine' },
  { value: '16/18_DENIER', label: '16/18 Denier — Fine' },
  { value: '20/22_DENIER', label: '20/22 Denier — Industry Standard' },
  { value: '24/26_DENIER', label: '24/26 Denier — Medium-heavy' },
  { value: '28/30_DENIER', label: '28/30 Denier — Heavy' },
]

const WINDING_INTEGRITY_OPTIONS = [
  { value: 'EXCELLENT', label: 'Excellent' },
  { value: 'GOOD', label: 'Good' },
  { value: 'POOR', label: 'Poor' },
  { value: 'FAIL', label: 'Fail' },
]

const TARGET_MACHINE_OPTIONS = [
  { value: '1536_HOOK_JACQUARD', label: '1536 Hook Jacquard (Standard Luxury Brocade)' },
  { value: '2400_HOOK_JACQUARD', label: '2400 Hook Jacquard (High-Density Fine Motif)' },
  { value: 'HANDLOOM', label: 'Handloom' },
  { value: 'POWERLOOM', label: 'Powerloom' },
  { value: 'RAPIER_LOOM', label: 'Rapier Loom' },
]

const COATING_OPTIONS = [
  { value: 'STANDARD_PARAFFIN', label: 'Standard Paraffin Wax' },
  { value: 'SILICONE_MICRO_WAX', label: 'Silicone Micro-wax' },
  { value: 'HIGH_GRADE_SILICONE', label: 'High-Grade Silicone' },
  { value: 'NONE', label: 'None' },
]

const AUDIT_METHOD_OPTIONS = [
  { value: 'BURN_TEST', label: 'Burn Test' },
  { value: 'CHEMICAL_STRIP', label: 'Chemical Strip' },
  { value: 'MICROSCOPE_VISUAL', label: 'Microscope Visual' },
  { value: 'FTIR_SPECTROSCOPY', label: 'FTIR Spectroscopy' },
]

export default function DashboardZariInspector() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [tab, setTab] = useState('preprocess')
  const [submitting, setSubmitting] = useState(false)
  const [validationResult, setValidationResult] = useState(null)
  const [certificateDetail, setCertificateDetail] = useState(null)
  const [selectedAssayId, setSelectedAssayId] = useState('')
  const [currentInspection, setCurrentInspection] = useState(null)

  const [lotBatches, setLotBatches] = useState([])
  const [assays, setAssays] = useState([])
  const [inspections, setInspections] = useState([])
  const [certificates, setCertificates] = useState([])
  const [forecast, setForecast] = useState(null)

  const [xrfForm, setXrfForm] = useState({
    zari_assay_id: '',
    zari_lot_batch_id: '',
    xrf_silver_purity_pct: '',
    xrf_gold_plating_pct: '',
    xrf_verification_passed: false
  })

  const [physicalForm, setPhysicalForm] = useState({
    zari_assay_id: '',
    zari_lot_batch_id: '',
    core_yarn_audit_result: 'PURE_SILK_RED_MAROON_DYED',
    core_yarn_audit_method: 'BURN_TEST',
    core_yarn_audit_passed: false,
    denier_measured: '',
    denier_target: '20/22_DENIER',
    tensile_strength_gd: '',
    bobbin_winding_integrity: 'GOOD'
  })

  const [aestheticForm, setAestheticForm] = useState({
    zari_assay_id: '',
    zari_lot_batch_id: '',
    tarnish_free_scan: false,
    color_luster_match: false,
    delta_e_value: '',
    gross_scale_weight_gm: '',
    tare_weight_gm: '',
    net_zari_weight_gm: '',
    moisture_reading_pct: ''
  })

  const [defectForm, setDefectForm] = useState({
    zari_assay_id: '',
    zari_lot_batch_id: '',
    wire_cuts_per_1000m: '',
    micro_cuts_detected: false,
    frayed_joints_detected: false,
    target_machine_type: '1536_HOOK_JACQUARD',
    flattened_wire_width_mm: '',
    surface_coating_lubrication: 'STANDARD_PARAFFIN',
    surface_coating_check_passed: false
  })

  useEffect(() => {
    fetchLotBatches()
    fetchAssays()
    fetchInspections()
    fetchCertificates()
    fetchSalesForecast()
  }, [])

  useEffect(() => {
    if (selectedAssayId) {
      fetchInspectionByAssay(selectedAssayId)
    }
  }, [selectedAssayId])

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

  const fetchInspections = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/zari/inspection`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setInspections(data.inspections || [])
    } catch (error) {
      console.error('Failed to fetch inspections:', error)
    }
  }

  const fetchInspectionByAssay = async (assayId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/zari/inspection?assay_id=${assayId}`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok && data.inspections && data.inspections.length > 0) {
        const insp = data.inspections[0]
        setCurrentInspection(insp)
        setSelectedAssayId(insp.zari_assay_id)
        // Populate forms
        setXrfForm({
          zari_assay_id: insp.zari_assay_id,
          zari_lot_batch_id: insp.zari_lot_batch_id,
          xrf_silver_purity_pct: insp.xrf_silver_purity_pct || '',
          xrf_gold_plating_pct: insp.xrf_gold_plating_pct || '',
          xrf_verification_passed: insp.xrf_verification_passed
        })
        setPhysicalForm({
          zari_assay_id: insp.zari_assay_id,
          zari_lot_batch_id: insp.zari_lot_batch_id,
          core_yarn_audit_result: insp.core_yarn_audit_result || 'PURE_SILK_RED_MAROON_DYED',
          core_yarn_audit_method: insp.core_yarn_audit_method || 'BURN_TEST',
          core_yarn_audit_passed: insp.core_yarn_audit_passed,
          denier_measured: insp.denier_measured || '',
          denier_target: insp.denier_target || '20/22_DENIER',
          tensile_strength_gd: insp.tensile_strength_gd || '',
          bobbin_winding_integrity: insp.bobbin_winding_integrity || 'GOOD'
        })
        setAestheticForm({
          zari_assay_id: insp.zari_assay_id,
          zari_lot_batch_id: insp.zari_lot_batch_id,
          tarnish_free_scan: insp.tarnish_free_scan,
          color_luster_match: insp.color_luster_match,
          delta_e_value: insp.delta_e_value || '',
          gross_scale_weight_gm: insp.gross_scale_weight_gm || '',
          tare_weight_gm: insp.tare_weight_gm || '',
          net_zari_weight_gm: insp.net_zari_weight_gm || '',
          moisture_reading_pct: insp.moisture_reading_pct || ''
        })
        setDefectForm({
          zari_assay_id: insp.zari_assay_id,
          zari_lot_batch_id: insp.zari_lot_batch_id,
          wire_cuts_per_1000m: insp.wire_cuts_per_1000m || '',
          micro_cuts_detected: insp.micro_cuts_detected,
          frayed_joints_detected: insp.frayed_joints_detected,
          target_machine_type: insp.target_machine_type || '1536_HOOK_JACQUARD',
          flattened_wire_width_mm: insp.flattened_wire_width_mm || '',
          surface_coating_lubrication: insp.surface_coating_lubrication || 'STANDARD_PARAFFIN',
          surface_coating_check_passed: insp.surface_coating_check_passed
        })
      }
    } catch (error) {
      console.error('Failed to fetch inspection by assay:', error)
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

  const handleAssayChange = (assayId) => {
    setSelectedAssayId(assayId)
    const assay = assays.find(a => a.id === assayId)
    if (assay) {
      setXrfForm({
        zari_assay_id: assayId,
        zari_lot_batch_id: assay.zari_lot_batch_id,
        xrf_silver_purity_pct: '',
        xrf_gold_plating_pct: '',
        xrf_verification_passed: false
      })
      setPhysicalForm({
        zari_assay_id: assayId,
        zari_lot_batch_id: assay.zari_lot_batch_id,
        core_yarn_audit_result: 'PURE_SILK_RED_MAROON_DYED',
        core_yarn_audit_method: 'BURN_TEST',
        core_yarn_audit_passed: false,
        denier_measured: '',
        denier_target: '20/22_DENIER',
        tensile_strength_gd: '',
        bobbin_winding_integrity: 'GOOD'
      })
      setAestheticForm({
        zari_assay_id: assayId,
        zari_lot_batch_id: assay.zari_lot_batch_id,
        tarnish_free_scan: false,
        color_luster_match: false,
        delta_e_value: '',
        gross_scale_weight_gm: '',
        tare_weight_gm: '',
        net_zari_weight_gm: '',
        moisture_reading_pct: ''
      })
      setDefectForm({
        zari_assay_id: assayId,
        zari_lot_batch_id: assay.zari_lot_batch_id,
        wire_cuts_per_1000m: '',
        micro_cuts_detected: false,
        frayed_joints_detected: false,
        target_machine_type: '1536_HOOK_JACQUARD',
        flattened_wire_width_mm: '',
        surface_coating_lubrication: 'STANDARD_PARAFFIN',
        surface_coating_check_passed: false
      })
    }
  }

  const handleXrfSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...xrfForm,
        zari_assay_id: xrfForm.zari_assay_id || null,
        zari_lot_batch_id: xrfForm.zari_lot_batch_id || null,
        xrf_silver_purity_pct: xrfForm.xrf_silver_purity_pct ? parseFloat(xrfForm.xrf_silver_purity_pct) : null,
        xrf_gold_plating_pct: xrfForm.xrf_gold_plating_pct ? parseFloat(xrfForm.xrf_gold_plating_pct) : null
      }

      const response = await fetch(`${API_URL}/zari/inspection`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`XRF inspection created`, 'success')
        setValidationResult({ type: 'success', data })
        setCurrentInspection(data)
        fetchInspections()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to create XRF inspection', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handlePhysicalSubmit = async () => {
    if (!currentInspection) {
      addNotification('Please create XRF inspection first', 'error')
      return
    }
    setSubmitting(true)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...physicalForm,
        zari_assay_id: currentInspection.zari_assay_id,
        zari_lot_batch_id: currentInspection.zari_lot_batch_id,
        denier_measured: physicalForm.denier_measured ? parseFloat(physicalForm.denier_measured) : null,
        tensile_strength_gd: physicalForm.tensile_strength_gd ? parseFloat(physicalForm.tensile_strength_gd) : null
      }

      const response = await fetch(`${API_URL}/zari/inspection/${currentInspection.id}`, {
        method: 'PUT',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Physical inspection updated', 'success')
        setValidationResult({ type: 'success', data })
        fetchInspections()
      } else {
        addNotification(data.error || 'Update failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to update physical inspection', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleAestheticSubmit = async () => {
    if (!currentInspection) {
      addNotification('Please create XRF inspection first', 'error')
      return
    }
    setSubmitting(true)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...aestheticForm,
        zari_assay_id: currentInspection.zari_assay_id,
        zari_lot_batch_id: currentInspection.zari_lot_batch_id,
        delta_e_value: aestheticForm.delta_e_value ? parseFloat(aestheticForm.delta_e_value) : null,
        gross_scale_weight_gm: aestheticForm.gross_scale_weight_gm ? parseFloat(aestheticForm.gross_scale_weight_gm) : null,
        tare_weight_gm: aestheticForm.tare_weight_gm ? parseFloat(aestheticForm.tare_weight_gm) : null,
        net_zari_weight_gm: aestheticForm.net_zari_weight_gm ? parseFloat(aestheticForm.net_zari_weight_gm) : null,
        moisture_reading_pct: aestheticForm.moisture_reading_pct ? parseFloat(aestheticForm.moisture_reading_pct) : null
      }

      const response = await fetch(`${API_URL}/zari/inspection/${currentInspection.id}`, {
        method: 'PUT',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Aesthetic & weight audit updated', 'success')
        setValidationResult({ type: 'success', data })
        fetchInspections()
      } else {
        addNotification(data.error || 'Update failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to update aesthetic audit', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleDefectSubmit = async () => {
    if (!currentInspection) {
      addNotification('Please create XRF inspection first', 'error')
      return
    }
    setSubmitting(true)
    try {
      const token = localStorage.getItem('access_token')
      const payload = {
        ...defectForm,
        zari_assay_id: currentInspection.zari_assay_id,
        zari_lot_batch_id: currentInspection.zari_lot_batch_id,
        wire_cuts_per_1000m: defectForm.wire_cuts_per_1000m ? parseInt(defectForm.wire_cuts_per_1000m) : 0,
        flattened_wire_width_mm: defectForm.flattened_wire_width_mm ? parseFloat(defectForm.flattened_wire_width_mm) : null
      }

      const response = await fetch(`${API_URL}/zari/inspection/${currentInspection.id}`, {
        method: 'PUT',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Defect logging & routing updated', 'success')
        setValidationResult({ type: 'success', data })
        fetchInspections()
      } else {
        addNotification(data.error || 'Update failed', 'error')
        setValidationResult({ type: 'error', data })
      }
    } catch (error) {
      addNotification('Failed to update defect logging', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleCertify = async () => {
    if (!currentInspection) {
      addNotification('No inspection to certify', 'error')
      return
    }
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/zari/inspection/${currentInspection.id}/certify`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ notes: 'Approved by Zari Inspector' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Inspection certified: ${data.certificate_hash}`, 'success')
        setCertificateDetail(data)
        fetchInspections()
        fetchCertificates()
      } else {
        addNotification(data.error || 'Certification failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to certify inspection', 'error')
    }
  }

  const handleReject = async () => {
    if (!currentInspection) {
      addNotification('No inspection to reject', 'error')
      return
    }
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/zari/inspection/${currentInspection.id}/reject`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason: 'Rejected by Zari Inspector' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Inspection rejected', 'warning')
        fetchInspections()
      } else {
        addNotification(data.error || 'Rejection failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to reject inspection', 'error')
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
      case 'DOWNGRADE_TO_1536_HOOK_OR_HANDLOOM': return 'warning'
      default: return 'default'
    }
  }

  const getRoutingColor = (routing) => {
    switch (routing) {
      case 'LUXURY_JACQUARD_LOOM_POOL': return 'success'
      case 'COMMERCIAL_SEMI_PREMIUM': return 'info'
      case 'QC_REJECT_HOLD': return 'error'
      case 'DOWNGRADE_TO_1536_HOOK_OR_HANDLOOM': return 'warning'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Zari Inspector — Post-Process Quality Control
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
          <Typography variant="subtitle1" gutterBottom>Guardrail Violations</Typography>
          {validationResult.data.validation_errors.map((err, idx) => (
            <Typography key={idx} variant="body2">• [{err.code}] {err.message}</Typography>
          ))}
        </Alert>
      )}

      {validationResult && validationResult.type === 'success' && (
        <Alert severity="success" sx={{ mb: 3 }}>
          {tab === 'xrf-purity' && 'XRF inspection created'}
          {tab === 'physical-geometrics' && 'Physical inspection updated'}
          {tab === 'aesthetic-weight' && 'Aesthetic & weight audit updated'}
          {tab === 'defect-routing' && 'Defect logging & routing updated'}
        </Alert>
      )}

      {certificateDetail && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>Certificate Generated</Typography>
          <Typography variant="body2"><strong>Hash:</strong> {certificateDetail.certificate_hash}</Typography>
          <Typography variant="body2"><strong>QR Tag:</strong> {certificateDetail.qr_tag_id}</Typography>
          <Typography variant="body2"><strong>Routing:</strong> {certificateDetail.auto_assigned_routing}</Typography>
          {certificateDetail.precious_metal_value_estimate && (
            <Typography variant="body2"><strong>Precious Metal Value:</strong> ₹{certificateDetail.precious_metal_value_estimate}</Typography>
          )}
        </Alert>
      )}

      {/* ===================== PRE-PROCESS TAB ===================== */}
      {tab === 'preprocess' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Zari Lot Batches from Refinery (Pre-Process)</Typography>
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

          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Zari Assay Records from Refinery (Pre-Process)</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Assay Cert No</TableCell>
                      <TableCell>Lot Batch</TableCell>
                      <TableCell>Zari Type</TableCell>
                      <TableCell>Silver %</TableCell>
                      <TableCell>Gold %</TableCell>
                      <TableCell>Copper %</TableCell>
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
                        <TableCell>{assay.copper_base_pct}%</TableCell>
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

      {/* ===================== XRF & PURITY TAB ===================== */}
      {tab === 'xrf-purity' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>1. XRF Spectrometer Testing & Assay Verification</Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <FormControl fullWidth>
                    <InputLabel>Select Assay from Refinery</InputLabel>
                    <Select value={xrfForm.zari_assay_id} label="Select Assay from Refinery"
                      onChange={(e) => handleAssayChange(e.target.value)}>
                      <MenuItem value="">Select assay</MenuItem>
                      {assays.map((assay) => (
                        <MenuItem key={assay.id} value={assay.id}>{assay.assay_certificate_no} — {assay.zari_lot_batch_no}</MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="XRF Silver Purity %" type="number"
                    value={xrfForm.xrf_silver_purity_pct}
                    onChange={(e) => setXrfForm({ ...xrfForm, xrf_silver_purity_pct: e.target.value })}
                    inputProps={{ step: '0.01', min: 0, max: 100 }} helperText="Luxury standard: 55% to 57%" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <TextField fullWidth label="XRF Gold Plating %" type="number"
                    value={xrfForm.xrf_gold_plating_pct}
                    onChange={(e) => setXrfForm({ ...xrfForm, xrf_gold_plating_pct: e.target.value })}
                    inputProps={{ step: '0.01', min: 0, max: 100 }} helperText="Luxury standard: 0.5% to 1.0%" />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControlLabel
                    control={
                      <Checkbox
                        checked={xrfForm.xrf_verification_passed}
                        onChange={(e) => setXrfForm({ ...xrfForm, xrf_verification_passed: e.target.checked })}
                      />
                    }
                    label="XRF Verification Passed (matches refinery assay within tolerance)"
                  />
                </Grid>
                <Grid item xs={12}>
                  <Button variant="contained" onClick={handleXrfSubmit} disabled={submitting || !xrfForm.zari_assay_id}>
                    {submitting ? 'Submitting...' : 'Create XRF Inspection'}
                  </Button>
                </Grid>
              </Grid>
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== PHYSICAL & GEOMETRICS TAB ===================== */}
      {tab === 'physical-geometrics' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>2. Physical & Textile Geometrics Inspection</Typography>
              {!currentInspection ? (
                <Alert severity="info">Please create XRF inspection first</Alert>
              ) : (
                <Grid container spacing={2}>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Core Yarn Audit Result" select
                      value={physicalForm.core_yarn_audit_result}
                      onChange={(e) => setPhysicalForm({ ...physicalForm, core_yarn_audit_result: e.target.value })}>
                      {CORE_YARN_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </TextField>
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Core Yarn Audit Method" select
                      value={physicalForm.core_yarn_audit_method}
                      onChange={(e) => setPhysicalForm({ ...physicalForm, core_yarn_audit_method: e.target.value })}>
                      {AUDIT_METHOD_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </TextField>
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <FormControlLabel
                      control={
                        <Checkbox
                          checked={physicalForm.core_yarn_audit_passed}
                          onChange={(e) => setPhysicalForm({ ...physicalForm, core_yarn_audit_passed: e.target.checked })}
                        />
                      }
                      label="Core Yarn Audit Passed"
                    />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Denier Measured" type="number"
                      value={physicalForm.denier_measured}
                      onChange={(e) => setPhysicalForm({ ...physicalForm, denier_measured: e.target.value })}
                      inputProps={{ step: '0.1', min: 0 }} helperText="Measured thread thickness" />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Denier Target" select
                      value={physicalForm.denier_target}
                      onChange={(e) => setPhysicalForm({ ...physicalForm, denier_target: e.target.value })}>
                      {DENIER_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </TextField>
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Tensile Strength (g/d)" type="number"
                      value={physicalForm.tensile_strength_gd}
                      onChange={(e) => setPhysicalForm({ ...physicalForm, tensile_strength_gd: e.target.value })}
                      inputProps={{ step: '0.1', min: 0 }} helperText="Min 3.5 g/d (3.8+ for powerlooms)" />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Bobbin Winding Integrity" select
                      value={physicalForm.bobbin_winding_integrity}
                      onChange={(e) => setPhysicalForm({ ...physicalForm, bobbin_winding_integrity: e.target.value })}>
                      {WINDING_INTEGRITY_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </TextField>
                  </Grid>
                  <Grid item xs={12}>
                    <Button variant="contained" onClick={handlePhysicalSubmit} disabled={submitting}>
                      {submitting ? 'Saving...' : 'Save Physical Inspection'}
                    </Button>
                  </Grid>
                </Grid>
              )}
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== AESTHETIC & WEIGHT TAB ===================== */}
      {tab === 'aesthetic-weight' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>3. Aesthetic & Oxidation Control + Precision Weight Auditing</Typography>
              {!currentInspection ? (
                <Alert severity="info">Please create XRF inspection first</Alert>
              ) : (
                <Grid container spacing={2}>
                  <Grid item xs={12} md={4}>
                    <FormControlLabel
                      control={
                        <Checkbox
                          checked={aestheticForm.tarnish_free_scan}
                          onChange={(e) => setAestheticForm({ ...aestheticForm, tarnish_free_scan: e.target.checked })}
                        />
                      }
                      label="Free From Tarnishing (No dark/black oxidation under 5000K lamp)"
                    />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <FormControlLabel
                      control={
                        <Checkbox
                          checked={aestheticForm.color_luster_match}
                          onChange={(e) => setAestheticForm({ ...aestheticForm, color_luster_match: e.target.checked })}
                        />
                      }
                      label="Color & Luster Match (Delta-E < 1.0 against master sample)"
                    />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Delta-E Value" type="number"
                      value={aestheticForm.delta_e_value}
                      onChange={(e) => setAestheticForm({ ...aestheticForm, delta_e_value: e.target.value })}
                      inputProps={{ step: '0.01', min: 0 }} helperText="Target < 1.0" />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Gross Scale Weight (gm)" type="number"
                      value={aestheticForm.gross_scale_weight_gm}
                      onChange={(e) => setAestheticForm({ ...aestheticForm, gross_scale_weight_gm: e.target.value })}
                      inputProps={{ step: '0.001', min: 0 }} />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Tare Weight (gm)" type="number"
                      value={aestheticForm.tare_weight_gm}
                      onChange={(e) => setAestheticForm({ ...aestheticForm, tare_weight_gm: e.target.value })}
                      inputProps={{ step: '0.001', min: 0 }} />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Net Zari Weight (gm)" type="number"
                      value={aestheticForm.net_zari_weight_gm}
                      onChange={(e) => setAestheticForm({ ...aestheticForm, net_zari_weight_gm: e.target.value })}
                      inputProps={{ step: '0.001', min: 0 }} helperText="Auto-calculated: Gross - Tare" />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Moisture Reading %" type="number"
                      value={aestheticForm.moisture_reading_pct}
                      onChange={(e) => setAestheticForm({ ...aestheticForm, moisture_reading_pct: e.target.value })}
                      inputProps={{ step: '0.1', min: 0, max: 100 }} helperText="Ensure moisture-free storage" />
                  </Grid>
                  <Grid item xs={12}>
                    <Button variant="contained" onClick={handleAestheticSubmit} disabled={submitting}>
                      {submitting ? 'Saving...' : 'Save Aesthetic & Weight Audit'}
                    </Button>
                  </Grid>
                </Grid>
              )}
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== DEFECT LOGGING & ROUTING TAB ===================== */}
      {tab === 'defect-routing' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>4. Defect Logging & Inventory Routing</Typography>
              {!currentInspection ? (
                <Alert severity="info">Please create XRF inspection first</Alert>
              ) : (
                <Grid container spacing={2}>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Wire Cuts per 1000m" type="number"
                      value={defectForm.wire_cuts_per_1000m}
                      onChange={(e) => setDefectForm({ ...defectForm, wire_cuts_per_1000m: e.target.value })}
                      inputProps={{ step: '1', min: 0 }} helperText="Zero tolerance for 2400 Hook" />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <FormControlLabel
                      control={
                        <Checkbox
                          checked={defectForm.micro_cuts_detected}
                          onChange={(e) => setDefectForm({ ...defectForm, micro_cuts_detected: e.target.checked })}
                        />
                      }
                      label="Micro-Cuts Detected"
                    />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <FormControlLabel
                      control={
                        <Checkbox
                          checked={defectForm.frayed_joints_detected}
                          onChange={(e) => setDefectForm({ ...defectForm, frayed_joints_detected: e.target.checked })}
                        />
                      }
                      label="Frayed Joints Detected"
                    />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Target Machine Type" select
                      value={defectForm.target_machine_type}
                      onChange={(e) => setDefectForm({ ...defectForm, target_machine_type: e.target.value })}>
                      {TARGET_MACHINE_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </TextField>
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Flattened Wire Width (mm)" type="number"
                      value={defectForm.flattened_wire_width_mm}
                      onChange={(e) => setDefectForm({ ...defectForm, flattened_wire_width_mm: e.target.value })}
                      inputProps={{ step: '0.01', min: 0 }} helperText="0.10-0.12mm for 2400 Hook, 0.15-0.22mm for 1536 Hook" />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <TextField fullWidth label="Surface Coating & Lubrication" select
                      value={defectForm.surface_coating_lubrication}
                      onChange={(e) => setDefectForm({ ...defectForm, surface_coating_lubrication: e.target.value })}>
                      {COATING_OPTIONS.map((opt) => (
                        <MenuItem key={opt.value} value={opt.value}>{opt.label}</MenuItem>
                      ))}
                    </TextField>
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <FormControlLabel
                      control={
                        <Checkbox
                          checked={defectForm.surface_coating_check_passed}
                          onChange={(e) => setDefectForm({ ...defectForm, surface_coating_check_passed: e.target.checked })}
                        />
                      }
                      label="Surface Coating Check Passed"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <Alert severity="info" sx={{ mb: 2 }}>
                      <Typography variant="subtitle2">Automated ERP Routing Rules</Typography>
                      <Typography variant="body2">
                        • 2400 Hook + Non-Silk Core → BLOCK (Reject for 2400 Hook)<br/>
                        • 2400 Hook + Joints > 0 → DOWNGRADE to 1536 Hook or Handloom<br/>
                        • 2400 Hook + Badla Width > 0.15mm → WARNING (Fabric stiffness risk)
                      </Typography>
                    </Alert>
                  </Grid>
                  <Grid item xs={12}>
                    <Box display="flex" gap={2}>
                      <Button variant="contained" onClick={handleDefectSubmit} disabled={submitting}>
                        {submitting ? 'Saving...' : 'Save Defect Logging & Routing'}
                      </Button>
                      <Button variant="outlined" color="success" onClick={handleCertify} disabled={!currentInspection}>
                        Certify Inspection
                      </Button>
                      <Button variant="outlined" color="error" onClick={handleReject} disabled={!currentInspection}>
                        Reject Inspection
                      </Button>
                    </Box>
                  </Grid>
                </Grid>
              )}
            </Paper>
          </Grid>
        </Grid>
      )}

      {/* ===================== CERTIFICATES & FORECAST TAB ===================== */}
      {tab === 'certificates-forecast' && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Zari Inspector Certificates — Post-Process Output</Typography>
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
                          <TableCell>{cert.zari_lot_batch_no}</TableCell>
                          <TableCell>{cert.zari_type}</TableCell>
                          <TableCell>{cert.zari_origin_cluster}</TableCell>
                          <TableCell>{cert.silver_purity_pct}%</TableCell>
                          <TableCell>{cert.gold_plating_pct}%</TableCell>
                          <TableCell>{cert.net_zari_weight_gm}</TableCell>
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
