import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardBobbinWinder() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const bobbins = [
    { id: 'BB-001', weft_color: 'Gold', length: '2000m', tension: '4.5 N', status: 'READY' },
    { id: 'BB-002', weft_color: 'Silver', length: '1950m', tension: '4.3 N', status: 'IN_PROGRESS' },
    { id: 'BB-003', weft_color: 'Red', length: '2100m', tension: '4.6 N', status: 'READY' },
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
            Bobbin Winder
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
              Bobbin Queue
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Bobbin ID</TableCell>
                    <TableCell>Weft Color</TableCell>
                    <TableCell>Length</TableCell>
                    <TableCell>Tension</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {bobbins.map((bobbin) => (
                    <TableRow key={bobbin.id}>
                      <TableCell>{bobbin.id}</TableCell>
                      <TableCell>{bobbin.weft_color}</TableCell>
                      <TableCell>{bobbin.length}</TableCell>
                      <TableCell>{bobbin.tension}</TableCell>
                      <TableCell>
                        <Chip label={bobbin.status} color={getStatusColor(bobbin.status)} size="small" />
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
