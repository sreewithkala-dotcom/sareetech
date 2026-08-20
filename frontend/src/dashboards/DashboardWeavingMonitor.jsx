import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'
import { useNavigate } from 'react-router-dom'
import QrCodeScannerIcon from '@mui/icons-material/QrCodeScanner'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import ErrorIcon from '@mui/icons-material/Error'
import WarningIcon from '@mui/icons-material/Warning'
import EnterprisePanel from '../components/EnterprisePanel'

export default function DashboardWeavingMonitor() {
  const { user } = useAuth()
  const { inputQueue, refreshInputQueue, loading, addNotification } = useDashboard()
  const navigate = useNavigate()

  useEffect(() => {
    refreshInputQueue()
    const interval = setInterval(refreshInputQueue, 30000)
    return () => clearInterval(interval)
  }, [refreshInputQueue])

  const getStatusColor = (status) => {
    if (status.includes('PASS')) return 'success'
    if (status.includes('FAIL')) return 'error'
    if (status.includes('WARNING')) return 'warning'
    return 'default'
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Weaving Monitor
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
        <Button variant="outlined" onClick={() => navigate('/scanner')}>
          Open Scanner
        </Button>
      </Box>

      <Grid container spacing={3}>
        {/* Input Queue */}
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

        {/* Quick Stats */}
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Today's Stats
            </Typography>
            <Box display="flex" flexDirection="column" gap={2}>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Typography>Lots Processed</Typography>
                <Typography variant="h6">24</Typography>
              </Box>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Typography>Defects Detected</Typography>
                <Typography variant="h6" color="error">3</Typography>
              </Box>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Typography>Efficiency</Typography>
                <Typography variant="h6" color="success.main">94%</Typography>
              </Box>
            </Box>
          </Paper>
        </Grid>
      </Grid>
      <EnterprisePanel userId={user?.id} factoryNodeId={user?.factory_node_id} />
    </Container>
  )
}
