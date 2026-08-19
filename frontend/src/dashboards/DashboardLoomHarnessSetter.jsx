import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardLoomHarnessSetter() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const harnesses = [
    { id: 'LH-001', loom_id: 'LOOM-2401', hooks: 2400, shedding: 'Regular', status: 'SET' },
    { id: 'LH-002', loom_id: 'LOOM-2402', hooks: 2400, shedding: 'Dobby', status: 'SET' },
    { id: 'LH-003', loom_id: 'LOOM-2403', hooks: 2400, shedding: 'Jacquard', status: 'IN_PROGRESS' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'SET': return 'success'
      case 'IN_PROGRESS': return 'warning'
      case 'ERROR': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Loom Harness Setting
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
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Harness Configurations
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Harness ID</TableCell>
                    <TableCell>Loom ID</TableCell>
                    <TableCell>Hooks</TableCell>
                    <TableCell>Shedding</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {harnesses.map((harness) => (
                    <TableRow key={harness.id}>
                      <TableCell>{harness.id}</TableCell>
                      <TableCell>{harness.loom_id}</TableCell>
                      <TableCell>{harness.hooks}</TableCell>
                      <TableCell>{harness.shedding}</TableCell>
                      <TableCell>
                        <Chip label={harness.status} color={getStatusColor(harness.status)} size="small" />
                      </TableCell>
                      <TableCell>
                        <Button size="small" variant="outlined">Configure</Button>
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
