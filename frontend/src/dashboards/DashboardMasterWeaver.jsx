import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardMasterWeaver() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const weavers = [
    { id: 'WVR-001', name: 'Ramesh Kumar', guild: 'Kanchipuram Weavers', efficiency: '95%', status: 'ACTIVE' },
    { id: 'WVR-002', name: 'Lakshmi Devi', guild: 'Banaras Zari Artisans', efficiency: '92%', status: 'ACTIVE' },
    { id: 'WVR-003', name: 'Suresh Babu', guild: 'Pochampally Ikkat', efficiency: '88%', status: 'ON_LEAVE' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'ACTIVE': return 'success'
      case 'ON_LEAVE': return 'warning'
      case 'INACTIVE': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Master Weaver
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
              Weavers Under Supervision
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Weaver ID</TableCell>
                    <TableCell>Name</TableCell>
                    <TableCell>Guild</TableCell>
                    <TableCell>Efficiency</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {weavers.map((weaver) => (
                    <TableRow key={weaver.id}>
                      <TableCell>{weaver.id}</TableCell>
                      <TableCell>{weaver.name}</TableCell>
                      <TableCell>{weaver.guild}</TableCell>
                      <TableCell>{weaver.efficiency}</TableCell>
                      <TableCell>
                        <Chip label={weaver.status} color={getStatusColor(weaver.status)} size="small" />
                      </TableCell>
                      <TableCell>
                        <Button size="small" variant="outlined">Details</Button>
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
