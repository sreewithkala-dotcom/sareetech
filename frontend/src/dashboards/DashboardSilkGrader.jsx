import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardSilkGrader() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const silkGrading = [
    { id: 'SG-001', lot_id: 'LOT-2024-001', grade: 'A+', denier: '22-24', luster: 'High', status: 'CERTIFIED' },
    { id: 'SG-002', lot_id: 'LOT-2024-002', grade: 'A', denier: '20-22', luster: 'Medium', status: 'CERTIFIED' },
    { id: 'SG-003', lot_id: 'LOT-2024-003', grade: 'B+', denier: '18-20', luster: 'Medium', status: 'PENDING' },
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
            Silk Grader
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
              Silk Grading
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Grade ID</TableCell>
                    <TableCell>Lot ID</TableCell>
                    <TableCell>Grade</TableCell>
                    <TableCell>Denier</TableCell>
                    <TableCell>Luster</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {silkGrading.map((grade) => (
                    <TableRow key={grade.id}>
                      <TableCell>{grade.id}</TableCell>
                      <TableCell>{grade.lot_id}</TableCell>
                      <TableCell>{grade.grade}</TableCell>
                      <TableCell>{grade.denier}</TableCell>
                      <TableCell>{grade.luster}</TableCell>
                      <TableCell>
                        <Chip label={grade.status} color={getStatusColor(grade.status)} size="small" />
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
