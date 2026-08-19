import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } = 'react-router-dom'

export default function DashboardThrowsterTwister() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const throwsterJobs = [
    { id: 'TJ-001', yarn_type: 'Silk Weft', twist: 'S-Twist', tension: '8.5 N', status: 'COMPLETED' },
    { id: 'TJ-002', yarn_type: 'Zari Core', twist: 'Z-Twist', tension: '9.0 N', status: 'IN_PROGRESS' },
    { id: 'TJ-003', yarn_type: 'Silk Warp', twist: 'S-Twist', tension: '8.2 N', status: 'PENDING' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'COMPLETED': return 'success'
      case 'IN_PROGRESS': return 'warning'
      case 'PENDING': return 'default'
      case 'DEFECT': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Throwster & Twister
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
              Throwster Jobs
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Job ID</TableCell>
                    <TableCell>Yarn Type</TableCell>
                    <TableCell>Twist</TableCell>
                    <TableCell>Tension</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {throwsterJobs.map((job) => (
                    <TableRow key={job.id}>
                      <TableCell>{job.id}</TableCell>
                      <TableCell>{job.yarn_type}</TableCell>
                      <TableCell>{job.twist}</TableCell>
                      <TableCell>{job.tension}</TableCell>
                      <TableCell>
                        <Chip label={job.status} color={getStatusColor(job.status)} size="small" />
                      </TableCell>
                      <TableCell>
                        <Button size="small" variant="outlined">Scan</Button>
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
