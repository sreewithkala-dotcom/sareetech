import { useEffect } from 'react'
import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'
import { useNavigate } from 'react-router-dom'
import QrCodeScannerIcon from '@mui/icons-material/QrCodeScanner'

export default function Dashboard() {
  const { user, logout } = useAuth()
  const { inputQueue, refreshInputQueue, loading, addNotification } = useDashboard()
  const navigate = useNavigate()

  useEffect(() => {
    refreshInputQueue()
    const interval = setInterval(refreshInputQueue, 30000) // Poll every 30s
    return () => clearInterval(interval)
  }, [refreshInputQueue])

  const handleScannerClick = () => {
    navigate('/scanner')
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            {user?.role?.name || 'Dashboard'}
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
        <Button variant="outlined" onClick={logout}>
          Logout
        </Button>
      </Box>

      <Grid container spacing={3}>
        {/* Input Queue Card */}
        <Grid item xs={12} md={8}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Input Queue ({inputQueue.length} lots waiting)
            </Typography>
            {loading ? (
              <Typography>Loading...</Typography>
            ) : inputQueue.length === 0 ? (
              <Typography color="text.secondary">No lots in queue</Typography>
            ) : (
              <Box sx={{ maxHeight: 400, overflow: 'auto' }}>
                {inputQueue.map((lot) => (
                  <Card key={lot.lot_id} sx={{ mb: 1 }}>
                    <CardContent>
                      <Typography variant="subtitle1">{lot.lot_number}</Typography>
                      <Typography variant="body2" color="text.secondary">
                        Asset ID: {lot.asset_id}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        Priority: {lot.priority} | Created: {new Date(lot.created_at).toLocaleString()}
                      </Typography>
                    </CardContent>
                  </Card>
                ))}
              </Box>
            )}
          </Paper>
        </Grid>

        {/* Actions Card */}
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Actions
            </Typography>
            <Button
              variant="contained"
              fullWidth
              size="large"
              startIcon={<QrCodeScannerIcon />}
              onClick={handleScannerClick}
              sx={{ mb: 2 }}
            >
              Open Scanner
            </Button>
            <Button
              variant="outlined"
              fullWidth
              onClick={refreshInputQueue}
              sx={{ mb: 2 }}
            >
              Refresh Queue
            </Button>
          </Paper>
        </Grid>
      </Grid>
    </Container>
  )
}
