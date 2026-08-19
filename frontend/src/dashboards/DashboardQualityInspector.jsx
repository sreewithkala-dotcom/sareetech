import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardQualityInspector() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const inspections = [
    { id: 'QI-001', saree_id: 'SARE-2024-0001', defect_type: 'None', severity: 'NONE', result: 'PASS' },
    { id: 'QI-002', saree_id: 'SARE-2024-0002', defect_type: 'Thread Break', severity: 'LOW', result: 'PENDING' },
    { id: 'QI-003', saree_id: 'SARE-2024-0003', defect_type: 'Tassel Misalign', severity: 'MEDIUM', result: 'FAIL' },
  ]

  const getResultColor = (result) => {
    switch (result) {
      case 'PASS': return 'success'
      case 'FAIL': return 'error'
      case 'PENDING': return 'warning'
      default: return 'default'
    }
  }

  const getSeverityColor = (severity) => {
    switch (severity) {
      case 'NONE': return 'success'
      case 'LOW': return 'info'
      case 'MEDIUM': return 'warning'
      case 'HIGH': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Quality Inspector
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
              Quality Inspections
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Inspection ID</TableCell>
                    <TableCell>Saree ID</TableCell>
                    <TableCell>Defect Type</TableCell>
                    <TableCell>Severity</TableCell>
                    <TableCell>Result</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {inspections.map((insp) => (
                    <TableRow key={insp.id}>
                      <TableCell>{insp.id}</TableCell>
                      <TableCell>{insp.saree_id}</TableCell>
                      <TableCell>{insp.defect_type}</TableCell>
                      <TableCell>
                        <Chip label={insp.severity} color={getSeverityColor(insp.severity)} size="small" />
                      </TableCell>
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
