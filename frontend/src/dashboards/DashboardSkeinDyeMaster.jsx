import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardSkeinDyeMaster() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const dyeVats = [
    { id: 'DV-001', color: 'Deep Maroon', temperature: '85°C', ph: 6.8, status: 'ACTIVE' },
    { id: 'DV-002', color: 'Royal Blue', temperature: '82°C', ph: 6.5, status: 'ACTIVE' },
    { id: 'DV-003', color: 'Emerald Green', temperature: '88°C', ph: 7.0, status: 'STANDBY' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'ACTIVE': return 'success'
      case 'STANDBY': return 'warning'
      case 'ERROR': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Skein Dye Master
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
              Dye Vats
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Vat ID</TableCell>
                    <TableCell>Color</TableCell>
                    <TableCell>Temperature</TableCell>
                    <TableCell>pH</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {dyeVats.map((vat) => (
                    <TableRow key={vat.id}>
                      <TableCell>{vat.id}</TableCell>
                      <TableCell>{vat.color}</TableCell>
                      <TableCell>{vat.temperature}</TableCell>
                      <TableCell>{vat.ph}</TableCell>
                      <TableCell>
                        <Chip label={vat.status} color={getStatusColor(vat.status)} size="small" />
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
      </Grid>
    </Container>
  )
}
