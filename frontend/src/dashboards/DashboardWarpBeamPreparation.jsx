import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardWarpBeamPreparation() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const warpBeams = [
    { id: 'WB-001', lot_id: 'LOT-2024-001', thread_count: 2400, tension: '12.5 N', status: 'READY' },
    { id: 'WB-002', lot_id: 'LOT-2024-002', thread_count: 2400, tension: '12.3 N', status: 'IN_PROGRESS' },
    { id: 'WB-003', lot_id: 'LOT-2024-003', thread_count: 2400, tension: '12.4 N', status: 'READY' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'READY': return 'success'
      case 'IN_PROGRESS': return 'warning'
      case 'DEFECT': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Warp Beam Preparation
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
              Warp Beams
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Beam ID</TableCell>
                    <TableCell>Lot ID</TableCell>
                    <TableCell>Thread Count</TableCell>
                    <TableCell>Tension</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {warpBeams.map((beam) => (
                    <TableRow key={beam.id}>
                      <TableCell>{beam.id}</TableCell>
                      <TableCell>{beam.lot_id}</TableCell>
                      <TableCell>{beam.thread_count}</TableCell>
                      <TableCell>{beam.tension}</TableCell>
                      <TableCell>
                        <Chip label={beam.status} color={getStatusColor(beam.status)} size="small" />
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
