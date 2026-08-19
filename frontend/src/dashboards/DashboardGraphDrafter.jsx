import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardGraphDrafter() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const designs = [
    { id: 'DES-001', name: 'Kanchipuram Classic', hooks: 2400, size: '1.2GB', status: 'APPROVED' },
    { id: 'DES-002', name: 'Banarasi Wedding', hooks: 2400, size: '1.1GB', status: 'PENDING' },
    { id: 'DES-003', name: 'Paithani Peacock', hooks: 2400, size: '1.3GB', status: 'APPROVED' },
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
            Graph Drafter
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
              Design Drafts
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Design ID</TableCell>
                    <TableCell>Name</TableCell>
                    <TableCell>Hooks</TableCell>
                    <TableCell>Size</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {designs.map((design) => (
                    <TableRow key={design.id}>
                      <TableCell>{design.id}</TableCell>
                      <TableCell>{design.name}</TableCell>
                      <TableCell>{design.hooks}</TableCell>
                      <TableCell>{design.size}</TableCell>
                      <TableCell>
                        <Chip label={design.status} color={getStatusColor(design.status)} size="small" />
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
