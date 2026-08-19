import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip, Avatar, TextField, Select, MenuItem, FormControl, InputLabel, Alert } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'
import { useNavigate } from 'react-router-dom'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5009/api/v1'

export default function DashboardIoTDeviceManager() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()

  const devices = [
    { id: 'ECU-001', mac: '24:0A:C4:8B:58:A1', loom_id: 'LOOM-2401', location: 'Cluster A', status: 'ONLINE', last_seen: '2026-08-19T22:45:00Z' },
    { id: 'ECU-002', mac: '24:0A:C4:8B:58:A2', loom_id: 'LOOM-2402', location: 'Cluster A', status: 'ONLINE', last_seen: '2026-08-19T22:44:30Z' },
    { id: 'ECU-003', mac: '24:0A:C4:8B:58:A3', loom_id: 'LOOM-2403', location: 'Cluster B', status: 'OFFLINE', last_seen: '2026-08-19T20:30:00Z' },
    { id: 'ECU-004', mac: '24:0A:C4:8B:58:A4', loom_id: 'LOOM-2404', location: 'Cluster B', status: 'ONLINE', last_seen: '2026-08-19T22:45:10Z' },
  ]

  const [skus, setSkus] = useState([])
  const [loadingSkus, setLoadingSkus] = useState(true)
  const [injecting, setInjecting] = useState(false)
  const [injectionResult, setInjectionResult] = useState(null)
  const [injectionForm, setInjectionForm] = useState({
    loom_id: '',
    pattern_id: '',
    file_hash: '',
    binary_url: '',
    sku_ref_id: ''
  })

  const telemetryStats = [
    { label: 'Total Devices', value: '1,247', color: 'primary' },
    { label: 'Online Now', value: '1,198', color: 'success' },
    { label: 'Offline', value: '49', color: 'error' },
    { label: 'Alerts Today', value: '12', color: 'warning' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'ONLINE': return 'success'
      case 'OFFLINE': return 'error'
      case 'WARNING': return 'warning'
      default: return 'default'
    }
  }

  useEffect(() => {
    fetchSkus()
  }, [])

  const fetchSkus = async () => {
    try {
      const response = await fetch(`${API_URL}/sku?limit=100`)
      const data = await response.json()
      setSkus(data)
    } catch (error) {
      console.error('Failed to fetch SKUs:', error)
    } finally {
      setLoadingSkus(false)
    }
  }

  const handleInjectDesign = async () => {
    if (!injectionForm.loom_id || !injectionForm.pattern_id || !injectionForm.file_hash || !injectionForm.binary_url) {
      addNotification('Please fill all required fields', 'error')
      return
    }

    setInjecting(true)
    setInjectionResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch('http://localhost:5004/api/v1/iot/design/inject', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          ...injectionForm,
          sku_ref_id: injectionForm.sku_ref_id || null
        })
      })

      const data = await response.json()
      if (response.ok) {
        setInjectionResult(data)
        addNotification(`Design ${injectionForm.pattern_id} queued for injection`, 'success')
        setInjectionForm({ loom_id: '', pattern_id: '', file_hash: '', binary_url: '', sku_ref_id: '' })
      } else {
        addNotification(data.error || 'Injection failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to inject design', 'error')
    } finally {
      setInjecting(false)
    }
  }

  const selectedSku = skus.find(s => s.sku_ref_id === injectionForm.sku_ref_id)

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            IoT Device Management
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
        <Button variant="outlined" onClick={() => navigate('/scanner')}>
          Open Scanner
        </Button>
      </Box>

      {injectionResult && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            Design Injection Queued
          </Typography>
          <Typography variant="body2">
            <strong>Injection ID:</strong> {injectionResult.injection_id}
          </Typography>
          <Typography variant="body2">
            <strong>Pattern ID:</strong> {injectionResult.pattern_id}
          </Typography>
          <Typography variant="body2">
            <strong>Loom ID:</strong> {injectionResult.loom_id}
          </Typography>
          {injectionResult.sku_ref_id && (
            <Typography variant="body2">
              <strong>SKU:</strong> {injectionResult.sku_ref_id}
            </Typography>
          )}
        </Alert>
      )}

      {/* Stats */}
      <Grid container spacing={3} mb={3}>
        {telemetryStats.map((stat, index) => (
          <Grid item xs={12} sm={6} md={3} key={index}>
            <Card>
              <CardContent>
                <Typography color="textSecondary" gutterBottom>
                  {stat.label}
                </Typography>
                <Typography variant="h4" component="div" color={`${stat.color}.main`}>
                  {stat.value}
                </Typography>
              </CardContent>
            </Card>
          </Grid>
        ))}
      </Grid>

      {/* Design Injection Form */}
      <Grid container spacing={3} mb={3}>
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Design Injection to ECU
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12} md={3}>
                <TextField
                  fullWidth
                  label="Loom ID"
                  value={injectionForm.loom_id}
                  onChange={(e) => setInjectionForm({...injectionForm, loom_id: e.target.value})}
                  placeholder="LOOM-2401"
                />
              </Grid>
              <Grid item xs={12} md={3}>
                <TextField
                  fullWidth
                  label="Pattern ID"
                  value={injectionForm.pattern_id}
                  onChange={(e) => setInjectionForm({...injectionForm, pattern_id: e.target.value})}
                  placeholder="DESIGN-20260819-00001"
                />
              </Grid>
              <Grid item xs={12} md={3}>
                <TextField
                  fullWidth
                  label="File Hash"
                  value={injectionForm.file_hash}
                  onChange={(e) => setInjectionForm({...injectionForm, file_hash: e.target.value})}
                  placeholder="SHA-256 hash"
                />
              </Grid>
              <Grid item xs={12} md={3}>
                <TextField
                  fullWidth
                  label="Binary URL"
                  value={injectionForm.binary_url}
                  onChange={(e) => setInjectionForm({...injectionForm, binary_url: e.target.value})}
                  placeholder="https://cdn.example.com/designs/..."
                />
              </Grid>
              <Grid item xs={12} md={3}>
                <FormControl fullWidth disabled={loadingSkus}>
                  <InputLabel>SKU Reference (Optional)</InputLabel>
                  <Select
                    value={injectionForm.sku_ref_id}
                    label="SKU Reference (Optional)"
                    onChange={(e) => setInjectionForm({...injectionForm, sku_ref_id: e.target.value})}
                  >
                    <MenuItem value="">None</MenuItem>
                    {skus.map((sku) => (
                      <MenuItem key={sku.sku_ref_id} value={sku.sku_ref_id}>
                        {sku.sku_ref_id} - {sku.geographic_hub} - {sku.jacquard_capacity}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={3}>
                <Button
                  variant="contained"
                  fullWidth
                  size="large"
                  onClick={handleInjectDesign}
                  disabled={injecting}
                  sx={{ height: '56px' }}
                >
                  {injecting ? 'Injecting...' : 'Inject Design'}
                </Button>
              </Grid>
            </Grid>
            {selectedSku && (
              <Box sx={{ mt: 2, p: 1, bgcolor: 'grey.100', borderRadius: 1 }}>
                <Typography variant="body2" color="text.secondary">
                  <strong>Selected SKU:</strong> {selectedSku.sku_ref_id} | {selectedSku.jacquard_capacity} | {selectedSku.weight_category_profile}
                </Typography>
              </Box>
            )}
          </Paper>
        </Grid>
      </Grid>

      {/* Devices List */}
      <Grid container spacing={3}>
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Edge Controller Units (ECU)
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Device ID</TableCell>
                    <TableCell>MAC Address</TableCell>
                    <TableCell>Loom ID</TableCell>
                    <TableCell>Location</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Last Seen</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {devices.map((device) => (
                    <TableRow key={device.id}>
                      <TableCell>{device.id}</TableCell>
                      <TableCell>{device.mac}</TableCell>
                      <TableCell>{device.loom_id}</TableCell>
                      <TableCell>{device.location}</TableCell>
                      <TableCell>
                        <Chip
                          label={device.status}
                          color={getStatusColor(device.status)}
                          size="small"
                        />
                      </TableCell>
                      <TableCell>{new Date(device.last_seen).toLocaleString()}</TableCell>
                      <TableCell>
                        <Button size="small" variant="outlined">
                          Details
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>
        </Grid>
      </Grid>
    </Container>
  )
}
