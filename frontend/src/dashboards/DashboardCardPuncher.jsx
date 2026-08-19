import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardCardPuncher() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const cardPunching = [
    { id: 'CP-001', design_id: 'DES-001', hooks: 2400, picks: 3500000, format: 'EP', status: 'COMPLETED' },
    { id: 'CP-002', design_id: 'DES-002', hooks: 2400, picks: 2800000, format: 'JC5', status: 'IN_PROGRESS' },
    { id: 'CP-003', design_id: 'DES-003', hooks: 2400, picks: 4200000, format: 'EP', status: 'PENDING' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'COMPLETED': return 'success'
      case 'IN_PROGRESS': return 'warning'
      case 'PENDING': return 'default'
      case 'ERROR': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Card Puncher
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
              Card Punching Queue
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Job ID</TableCell>
                    <TableCell>Design ID</TableCell>
                    <TableCell>Hooks</TableCell>
                    <TableCell>Picks</TableCell>
                    <TableCell>Format</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {cardPunching.map((job) => (
                    <TableRow key={job.id}>
                      <TableCell>{job.id}</TableCell>
                      <TableCell>{job.design_id}</TableCell>
                      <TableCell>{job.hooks}</TableCell>
                      <TableCell>{job.picks.toLocaleString()}</TableCell>
                      <TableCell>{job.format}</TableCell>
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
