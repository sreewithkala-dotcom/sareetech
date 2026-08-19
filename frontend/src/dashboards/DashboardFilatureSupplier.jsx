import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardFilatureSupplier() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const silkLots = [
    { id: 'SL-001', origin: 'Karnataka', denier: '22-24', quality: 'A+', quantity: '50 kg', status: 'CERTIFIED' },
    { id: 'SL-002', origin: 'Tamil Nadu', denier: '20-22', quality: 'A', quantity: '75 kg', status: 'CERTIFIED' },
    { id: 'SL-003', origin: 'Andhra Pradesh', denier: '24-26', quality: 'A+', quantity: '40 kg', status: 'PENDING' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'CERTIFIED': return 'success'
      case 'PENDING': return 'warning'
      case 'REJECTED': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Filature Supplier
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
              Silk Lots
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Lot ID</TableCell>
                    <TableCell>Origin</TableCell>
                    <TableCell>Denier</TableCell>
                    <TableCell>Quality</TableCell>
                    <TableCell>Quantity</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {silkLots.map((lot) => (
                    <TableRow key={lot.id}>
                      <TableCell>{lot.id}</TableCell>
                      <TableCell>{lot.origin}</TableCell>
                      <TableCell>{lot.denier}</TableCell>
                      <TableCell>{lot.quality}</TableCell>
                      <TableCell>{lot.quantity}</TableCell>
                      <TableCell>
                        <Chip label={lot.status} color={getStatusColor(lot.status)} size="small" />
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
