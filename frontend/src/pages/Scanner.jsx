import { useState } from 'react'
import { Container, TextField, Button, Box, Typography, Alert, Paper, Grid } from '@mui/material'
import { useNavigate } from 'react-router-dom'
import axios from 'axios'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000/api/v1'

export default function Scanner() {
  const [assetId, setAssetId] = useState('')
  const [scanType, setScanType] = useState('input')
  const [result, setResult] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const navigate = useNavigate()

  const handleScan = async () => {
    if (!assetId.trim()) {
      setError('Please enter an asset ID')
      return
    }

    setLoading(true)
    setError('')
    setResult(null)

    try {
      const token = localStorage.getItem('access_token')
      const endpoint = scanType === 'input' ? '/scanner/input' : '/scanner/output'
      
      const response = await axios.post(
        `${API_URL}${endpoint}`,
        {
          asset_id: assetId,
          factory_node_id: 'FACT-BLR-01'
        },
        {
          headers: {
            'Authorization': `Bearer ${token}`
          }
        }
      )

      setResult({
        success: true,
        message: response.data.message,
        data: response.data
      })
      setAssetId('')
    } catch (err) {
      setError(err.response?.data?.message || 'Scan failed')
      setResult(null)
    } finally {
      setLoading(false)
    }
  }

  return (
    <Container maxWidth="sm" sx={{ mt: 4 }}>
      <Paper sx={{ p: 3 }}>
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
          <Typography variant="h4">Scanner</Typography>
          <Button variant="outlined" onClick={() => navigate('/dashboard')}>
            Back to Dashboard
          </Button>
        </Box>

        {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
        {result && (
          <Alert severity="success" sx={{ mb: 2 }}>
            {result.message}
            {result.data.next_role_id && (
              <Typography variant="body2">
                Next role ID: {result.data.next_role_id}
              </Typography>
            )}
          </Alert>
        )}

        <Grid container spacing={2} mb={3}>
          <Grid item xs={12}>
            <TextField
              fullWidth
              label="Asset ID (Barcode/QR/RFID)"
              value={assetId}
              onChange={(e) => setAssetId(e.target.value)}
              disabled={loading}
            />
          </Grid>
          <Grid item xs={12}>
            <Button
              variant={scanType === 'input' ? 'contained' : 'outlined'}
              onClick={() => setScanType('input')}
              sx={{ mr: 1 }}
            >
              Input Scan
            </Button>
            <Button
              variant={scanType === 'output' ? 'contained' : 'outlined'}
              onClick={() => setScanType('output')}
            >
              Output Scan
            </Button>
          </Grid>
          <Grid item xs={12}>
            <Button
              variant="contained"
              fullWidth
              size="large"
              onClick={handleScan}
              disabled={loading}
            >
              {loading ? 'Scanning...' : 'Scan'}
            </Button>
          </Grid>
        </Grid>
      </Paper>
    </Container>
  )
}
