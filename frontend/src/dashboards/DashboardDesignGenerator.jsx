import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Slider, Switch, FormControlLabel } from '@mui/material'
import { useState } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'

export default function DashboardDesignGenerator() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [generating, setGenerating] = useState(false)
  const [designCount, setDesignCount] = useState(10)
  const [motifStyle, setMotifStyle] = useState('kanchipuram')

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
          motif_style: motifStyle
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
                          Motif: Kanchipuram | Hooks: 2400 | Size: 1.2GB
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
    </Container>
  )
}
