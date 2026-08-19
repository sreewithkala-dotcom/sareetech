import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardPirnWinders() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const pirns = [
    { id: 'PN-001', weft_color: 'Deep Maroon', length: '500m', status: 'READY' },
    { id: 'PN-002', weft_color: 'Royal Blue', length: '480m', status: 'IN_PROGRESS' },
    { id: 'PN-003', weft_color: 'Emerald Green', length: '520m', status: 'READY' },
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
            Pirn Winders
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
              Pirn Queue
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Pirn ID</TableCell>
                    <TableCell>Weft Color</TableCell>
                    <TableCell>Length</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {pirns.map((pirn) => (
                    <TableRow key={pirn.id}>
                      <TableCell>{pirn.id}</TableCell>
                      <TableCell>{pirn.weft_color}</TableCell>
                      <TableCell>{pirn.length}</TableCell>
                      <TableCell>
                        <Chip label={pirn.status} color={getStatusColor(pirn.status)} size="small" />
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
