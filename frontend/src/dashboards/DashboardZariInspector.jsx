import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardZariInspector() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const zariBatches = [
    { id: 'ZB-001', type: 'Pure Gold Zari', weight: '500g', purity: '99.9%', status: 'PASS' },
    { id: 'ZB-002', type: 'Pure Silver Zari', weight: '750g', purity: '99.5%', status: 'PASS' },
    { id: 'ZB-003', type: 'Tested Zari', weight: '1000g', purity: '85.0%', status: 'PENDING' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'PASS': return 'success'
      case 'FAIL': return 'error'
      case 'PENDING': return 'warning'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Zari Inspection
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
              Zari Batches
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Batch ID</TableCell>
                    <TableCell>Type</TableCell>
                    <TableCell>Weight</TableCell>
                    <TableCell>Purity</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {zariBatches.map((batch) => (
                    <TableRow key={batch.id}>
                      <TableCell>{batch.id}</TableCell>
                      <TableCell>{batch.type}</TableCell>
                      <TableCell>{batch.weight}</TableCell>
                      <TableCell>{batch.purity}</TableCell>
                      <TableCell>
                        <Chip label={batch.status} color={getStatusColor(batch.status)} size="small" />
                      </TableCell>
                      <TableCell>
                        <Button size="small" variant="outlined">Inspect</Button>
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
