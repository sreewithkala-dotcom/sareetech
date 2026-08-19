import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardQADyeingInspector() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const inspections = [
    { id: 'QDI-001', batch_id: 'BATCH-001', color: 'Deep Maroon', delta_e: '0.8', result: 'PASS' },
    { id: 'QDI-002', batch_id: 'BATCH-002', color: 'Royal Blue', delta_e: '1.5', result: 'FAIL' },
    { id: 'QDI-003', batch_id: 'BATCH-003', color: 'Emerald Green', delta_e: '0.6', result: 'PASS' },
  ]

  const getResultColor = (result) => {
    switch (result) {
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
            QA Dyeing Inspector
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
              Dyeing Inspections
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Inspection ID</TableCell>
                    <TableCell>Batch ID</TableCell>
                    <TableCell>Color</TableCell>
                    <TableCell>Delta-E</TableCell>
                    <TableCell>Result</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {inspections.map((insp) => (
                    <TableRow key={insp.id}>
                      <TableCell>{insp.id}</TableCell>
                      <TableCell>{insp.batch_id}</TableCell>
                      <TableCell>{insp.color}</TableCell>
                      <TableCell>{insp.delta_e}</TableCell>
                      <TableCell>
                        <Chip label={insp.result} color={getResultColor(insp.result)} size="small" />
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
