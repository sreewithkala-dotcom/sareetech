import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardMasterColorist() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const colorFormulations = [
    { id: 'CF-001', name: 'Deep Maroon', base_color: 'Red', additives: 'Blue + Yellow', delta_e: '0.8', status: 'APPROVED' },
    { id: 'CF-002', name: 'Royal Blue', base_color: 'Blue', additives: 'Red + Green', delta_e: '1.2', status: 'APPROVED' },
    { id: 'CF-003', name: 'Emerald Green', base_color: 'Green', additives: 'Yellow + Blue', delta_e: '0.5', status: 'APPROVED' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'APPROVED': return 'success'
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
            Master Colorist
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
              Color Formulations
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>ID</TableCell>
                    <TableCell>Name</TableCell>
                    <TableCell>Base Color</TableCell>
                    <TableCell>Additives</TableCell>
                    <TableCell>Delta-E</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {colorFormulations.map((formula) => (
                    <TableRow key={formula.id}>
                      <TableCell>{formula.id}</TableCell>
                      <TableCell>{formula.name}</TableCell>
                      <TableCell>{formula.base_color}</TableCell>
                      <TableCell>{formula.additives}</TableCell>
                      <TableCell>{formula.delta_e}</TableCell>
                      <TableCell>
                        <Chip label={formula.status} color={getStatusColor(formula.status)} size="small" />
                      </TableCell>
                      <TableCell>
                        <Button size="small" variant="outlined">Edit</Button>
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
