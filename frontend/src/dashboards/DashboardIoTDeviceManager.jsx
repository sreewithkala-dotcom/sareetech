import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip, Avatar } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'
import { useNavigate } from 'react-router-dom'

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
