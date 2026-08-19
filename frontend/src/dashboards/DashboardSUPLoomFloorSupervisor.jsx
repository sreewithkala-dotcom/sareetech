import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardSUPLoomFloorSupervisor() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const loomFloors = [
    { id: 'LF-001', floor: 'Floor A', looms: 50, active: 48, efficiency: '94%', status: 'OPERATIONAL' },
    { id: 'LF-002', floor: 'Floor B', looms: 50, active: 50, efficiency: '96%', status: 'OPERATIONAL' },
    { id: 'LF-003', floor: 'Floor C', looms: 40, active: 35, efficiency: '88%', status: 'MAINTENANCE' },
  ]

  const alerts = [
    { id: 1, type: 'MAINTENANCE', message: 'LOOM-2403 requires maintenance', time: '10 mins ago' },
    { id: 2, type: 'QUALITY', message: 'Defect detected on LOOM-2405', time: '25 mins ago' },
    { id: 3, type: 'MATERIAL', message: 'Zari stock running low', time: '1 hour ago' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'OPERATIONAL': return 'success'
      case 'MAINTENANCE': return 'warning'
      case 'STOPPED': return 'error'
      default: return 'default'
    }
  }

  const getAlertColor = (type) => {
    switch (type) {
      case 'MAINTENANCE': return 'warning'
      case 'QUALITY': return 'error'
      case 'MATERIAL': return 'info'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Loom Floor Supervisor
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
        {/* Floor Stats */}
        <Grid item xs={12} md={8}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Loom Floors
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Floor ID</TableCell>
                    <TableCell>Floor</TableCell>
                    <TableCell>Total Looms</TableCell>
                    <TableCell>Active</TableCell>
                    <TableCell>Efficiency</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {loomFloors.map((floor) => (
                    <TableRow key={floor.id}>
                      <TableCell>{floor.id}</TableCell>
                      <TableCell>{floor.floor}</TableCell>
                      <TableCell>{floor.looms}</TableCell>
                      <TableCell>{floor.active}</TableCell>
                      <TableCell>{floor.efficiency}</TableCell>
                      <TableCell>
                        <Chip label={floor.status} color={getStatusColor(floor.status)} size="small" />
                      </TableCell>
                      <TableCell>
                        <Button size="small" variant="outlined">Details</Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>
        </Grid>

        {/* Alerts */}
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Recent Alerts
            </Typography>
            <Box display="flex" flexDirection="column" gap={2}>
              {alerts.map((alert) => (
                <Card key={alert.id} variant="outlined">
                  <CardContent>
                    <Box display="flex" justifyContent="space-between" alignItems="flex-start">
                      <Chip label={alert.type} color={getAlertColor(alert.type)} size="small" />
                      <Typography variant="caption" color="text.secondary">
                        {alert.time}
                      </Typography>
                    </Box>
                    <Typography variant="body2" sx={{ mt: 1 }}>
                      {alert.message}
                    </Typography>
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
