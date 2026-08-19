import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardGuildManager() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const navigate = useNavigate()

  const guilds = [
    { id: 'G-001', name: 'Kanchipuram Weavers Guild', members: 450, location: 'Kanchipuram, TN', status: 'ACTIVE' },
    { id: 'G-002', name: 'Banaras Zari Artisans', members: 320, location: 'Varanasi, UP', status: 'ACTIVE' },
    { id: 'G-003', name: 'Pochampally Ikkat Weavers', members: 280, location: 'Pochampally, TS', status: 'ACTIVE' },
  ]

  const recentPayments = [
    { id: 'PAY-001', guild: 'Kanchipuram Weavers Guild', amount: 450000, date: '2026-08-19', status: 'COMPLETED' },
    { id: 'PAY-002', guild: 'Banaras Zari Artisans', amount: 320000, date: '2026-08-18', status: 'PROCESSING' },
    { id: 'PAY-003', guild: 'Pochampally Ikkat Weavers', amount: 280000, date: '2026-08-18', status: 'COMPLETED' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'ACTIVE': return 'success'
      case 'INACTIVE': return 'error'
      case 'PROCESSING': return 'warning'
      case 'COMPLETED': return 'success'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Guild Management
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
        {/* Guilds List */}
        <Grid item xs={12} md={8}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Active Guilds
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>ID</TableCell>
                    <TableCell>Name</TableCell>
                    <TableCell>Location</TableCell>
                    <TableCell>Members</TableCell>
                    <TableCell>Status</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {guilds.map((guild) => (
                    <TableRow key={guild.id}>
                      <TableCell>{guild.id}</TableCell>
                      <TableCell>{guild.name}</TableCell>
                      <TableCell>{guild.location}</TableCell>
                      <TableCell>{guild.members}</TableCell>
                      <TableCell>
                        <Chip
                          label={guild.status}
                          color={getStatusColor(guild.status)}
                          size="small"
                        />
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>
        </Grid>

        {/* Quick Stats */}
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Payment Summary
            </Typography>
            <Box display="flex" flexDirection="column" gap={2}>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Typography>Total Guilds</Typography>
                <Typography variant="h6">3</Typography>
              </Box>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Typography>Total Members</Typography>
                <Typography variant="h6">1,050</Typography>
              </Box>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Typography>Pending Payments</Typography>
                <Typography variant="h6" color="warning.main">₹3.2L</Typography>
              </Box>
            </Box>
          </Paper>
        </Grid>

        {/* Recent Payments */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Recent Payments
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Payment ID</TableCell>
                    <TableCell>Guild</TableCell>
                    <TableCell>Amount</TableCell>
                    <TableCell>Date</TableCell>
                    <TableCell>Status</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {recentPayments.map((payment) => (
                    <TableRow key={payment.id}>
                      <TableCell>{payment.id}</TableCell>
                      <TableCell>{payment.guild}</TableCell>
                      <TableCell>₹{payment.amount.toLocaleString()}</TableCell>
                      <TableCell>{payment.date}</TableCell>
                      <TableCell>
                        <Chip
                          label={payment.status}
                          color={getStatusColor(payment.status)}
                          size="small"
                        />
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
