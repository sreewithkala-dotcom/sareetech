import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardSilkDegummingMaster() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const batches = [
    { id: 'DG-001', lot_id: 'LOT-2024-001', sericin_pct: '18.5%', temperature: '85°C', status: 'COMPLETED' },
    { id: 'DG-002', lot_id: 'LOT-2024-002', sericin_pct: '17.8%', temperature: '88°C', status: 'IN_PROGRESS' },
    { id: 'DG-003', lot_id: 'LOT-2024-003', sericin_pct: '19.2%', temperature: '82°C', status: 'PENDING' },
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
            Silk Degumming Master
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
              Degumming Batches
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Batch ID</TableCell>
                    <TableCell>Lot ID</TableCell>
                    <TableCell>Sericin %</TableCell>
                    <TableCell>Temperature</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {batches.map((batch) => (
                    <TableRow key={batch.id}>
                      <TableCell>{batch.id}</TableCell>
                      <TableCell>{batch.lot_id}</TableCell>
                      <TableCell>{batch.sericin_pct}</TableCell>
                      <TableCell>{batch.temperature}</TableCell>
                      <TableCell>
                        <Chip label={batch.status} color={getStatusColor(batch.status)} size="small" />
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
