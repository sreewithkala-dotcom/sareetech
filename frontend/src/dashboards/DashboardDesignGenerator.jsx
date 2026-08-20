import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Slider, Switch, FormControlLabel, FormControl, InputLabel, Select, MenuItem, Chip } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'
import EnterprisePanel from '../components/EnterprisePanel'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5009/api/v1'

export default function DashboardDesignGenerator() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [generating, setGenerating] = useState(false)
  const [designCount, setDesignCount] = useState(10)
  const [motifStyle, setMotifStyle] = useState('kanchipuram')
  const [selectedSku, setSelectedSku] = useState('')
  const [skus, setSkus] = useState([])
  const [loadingSkus, setLoadingSkus] = useState(true)

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

  const handleGenerate = async () => {
    setGenerating(true)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch('http://localhost:5005/api/v1/design/generate', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          count: designCount,
          region: 'ap-south-1',
          motif_style: motifStyle,
          sku_ref_id: selectedSku || null
        })
      })
      
      const data = await response.json()
      if (response.ok) {
        addNotification(`Generated ${data.count} designs successfully!`, 'success')
      } else {
        addNotification(data.error || 'Generation failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to generate designs', 'error')
    } finally {
      setGenerating(false)
    }
  }

  const selectedSkuData = skus.find(s => s.sku_ref_id === selectedSku)

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Design Generator
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
      </Box>

      <Grid container spacing={3}>
        {/* Generation Controls */}
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              GAN Generation Controls
            </Typography>
            <Box display="flex" flexDirection="column" gap={3}>
              <TextField
                label="Number of Designs"
                type="number"
                value={designCount}
                onChange={(e) => setDesignCount(Math.min(100, Math.max(1, parseInt(e.target.value) || 1)))}
                inputProps={{ min: 1, max: 100 }}
                helperText="Max 100 designs per batch"
              />
              
              <TextField
                label="Motif Style"
                select
                value={motifStyle}
                onChange={(e) => setMotifStyle(e.target.value)}
                SelectProps={{ native: true }}
              >
                <option value="kanchipuram">Kanchipuram</option>
                <option value="banarasi">Banarasi</option>
                <option value="paithani">Paithani</option>
              </TextField>

              <FormControl fullWidth disabled={loadingSkus}>
                <InputLabel>SKU Reference (Optional)</InputLabel>
                <Select
                  value={selectedSku}
                  label="SKU Reference (Optional)"
                  onChange={(e) => setSelectedSku(e.target.value)}
                >
                  <MenuItem value="">None</MenuItem>
                  {skus.map((sku) => (
                    <MenuItem key={sku.sku_ref_id} value={sku.sku_ref_id}>
                      {sku.sku_ref_id} - {sku.geographic_hub} - {sku.jacquard_capacity}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>

              {selectedSkuData && (
                <Box sx={{ p: 1, bgcolor: 'grey.100', borderRadius: 1 }}>
                  <Typography variant="body2" color="text.secondary">
                    <strong>Hooks:</strong> {selectedSkuData.jacquard_capacity}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    <strong>Weight:</strong> {selectedSkuData.total_saree_weight_g}g
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    <strong>Category:</strong> {selectedSkuData.weight_category_profile}
                  </Typography>
                </Box>
              )}

              <Button
                variant="contained"
                fullWidth
                size="large"
                onClick={handleGenerate}
                disabled={generating}
              >
                {generating ? 'Generating...' : 'Generate Designs'}
              </Button>
            </Box>
          </Paper>
        </Grid>

        {/* Generation Queue */}
        <Grid item xs={12} md={8}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Recent Generations
            </Typography>
            <Box sx={{ maxHeight: 400, overflow: 'auto' }}>
              {[1, 2, 3, 4, 5].map((item) => (
                <Card key={item} sx={{ mb: 1 }}>
                  <CardContent>
                    <Box display="flex" justifyContent="space-between" alignItems="center">
                      <div>
                        <Typography variant="subtitle1">
                          DESIGN-20260819-{10000 + item}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Motif: {motifStyle} | Hooks: {selectedSku ? selectedSkuData?.jacquard_capacity : '2400'} | Size: 1.2GB
                        </Typography>
                      </div>
                      <Chip label="Pending" color="warning" size="small" />
                    </Box>
                  </CardContent>
                </Card>
              ))}
            </Box>
          </Paper>
        </Grid>
      </Grid>
      <EnterprisePanel userId={user?.id} factoryNodeId={user?.factory_node_id} />
    </Container>
  )
}
