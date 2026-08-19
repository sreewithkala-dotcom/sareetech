import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardWarpJoiner() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const joins = [
    { id: 'WJ-001', warp_id: 'WB-001', technique: 'Splicing', strength: '98%', status: 'PASS' },
    { id: 'WJ-002', warp_id: 'WB-002', technique: 'Weaving', strength: '95%', status: 'PASS' },
    { id: 'WJ-003', warp_id: 'WB-003', technique: 'Splicing', strength: '92%', status: 'PENDING' },
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
            Warp Joining
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
              Warp Joins
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Join ID</TableCell>
                    <TableCell>Warp ID</TableCell>
                    <TableCell>Technique</TableCell>
                    <TableCell>Strength</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {joins.map((join) => (
                    <TableRow key={join.id}>
                      <TableCell>{join.id}</TableCell>
                      <TableCell>{join.warp_id}</TableCell>
                      <TableCell>{join.technique}</TableCell>
                      <TableCell>{join.strength}</TableCell>
                      <TableCell>
                        <Chip label={join.status} color={getStatusColor(join.status)} size="small" />
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
