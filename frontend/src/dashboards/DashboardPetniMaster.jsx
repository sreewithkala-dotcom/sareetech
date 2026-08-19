import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardPetniMaster() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const petniWork = [
    { id: 'PNW-001', saree_id: 'SARE-2024-0001', operation: 'Pallu Setting', status: 'COMPLETED' },
    { id: 'PNW-002', saree_id: 'SARE-2024-0002', operation: 'Border Tassel', status: 'IN_PROGRESS' },
    { id: 'PNW-003', saree_id: 'SARE-2024-0003', operation: 'Pallu Cutting', status: 'PENDING' },
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
            Petni Master
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
              Petni Work Queue
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Work ID</TableCell>
                    <TableCell>Saree ID</TableCell>
                    <TableCell>Operation</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {petniWork.map((work) => (
                    <TableRow key={work.id}>
                      <TableCell>{work.id}</TableCell>
                      <TableCell>{work.saree_id}</TableCell>
                      <TableCell>{work.operation}</TableCell>
                      <TableCell>
                        <Chip label={work.status} color={getStatusColor(work.status)} size="small" />
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
