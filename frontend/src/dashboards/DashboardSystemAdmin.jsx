import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import EnterprisePanel from '../components/EnterprisePanel'

export default function DashboardSystemAdmin() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const systemStats = [
    { label: 'Total Users', value: '2,847', color: 'primary' },
    { label: 'Active Lots', value: '1,234', color: 'success' },
    { label: 'Pending QC', value: '89', color: 'warning' },
    { label: 'Quarantined', value: '12', color: 'error' },
  ]

  const recentActivity = [
    { id: 1, action: 'User Login', user: 'admin@factory.com', time: '2 mins ago' },
    { id: 2, action: 'Lot Certified', user: 'qa.inspector@factory.com', time: '5 mins ago' },
    { id: 3, action: 'Design Generated', user: 'design.generator@factory.com', time: '10 mins ago' },
    { id: 4, action: 'Payment Processed', user: 'guild.manager@factory.com', time: '15 mins ago' },
  ]

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            System Administration
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Admin: {user?.full_name}
          </Typography>
        </div>
      </Box>

      {/* System Stats */}
      <Grid container spacing={3} mb={3}>
        {systemStats.map((stat, index) => (
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

      {/* Recent Activity */}
      <Grid container spacing={3}>
        <Grid item xs={12} md={8}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Recent Activity
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Action</TableCell>
                    <TableCell>User</TableCell>
                    <TableCell>Time</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {recentActivity.map((activity) => (
                    <TableRow key={activity.id}>
                      <TableCell>{activity.action}</TableCell>
                      <TableCell>{activity.user}</TableCell>
                      <TableCell>{activity.time}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>
        </Grid>

        {/* Quick Actions */}
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Quick Actions
            </Typography>
            <Box display="flex" flexDirection="column" gap={2}>
              <Button
                variant="contained"
                fullWidth
                onClick={() => navigate('/scanner')}
              >
                Open Scanner
              </Button>
              <Button variant="outlined" fullWidth>
                Manage Users
              </Button>
              <Button variant="outlined" fullWidth>
                View Audit Logs
              </Button>
              <Button variant="outlined" fullWidth>
                System Configuration
              </Button>
            </Box>
          </Paper>
        </Grid>
      </Grid>
      <EnterprisePanel userId={user?.id} factoryNodeId={user?.factory_node_id} />
    </Container>
  )
}
