import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardSilkMarkOfficer() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const certificates = [
    { id: 'CERT-001', saree_id: 'SARE-2024-0001', silk_purity: '99.8%', zari_purity: '99.5%', status: 'ISSUED' },
    { id: 'CERT-002', saree_id: 'SARE-2024-0002', silk_purity: '99.5%', zari_purity: '98.0%', status: 'ISSUED' },
    { id: 'CERT-003', saree_id: 'SARE-2024-0003', silk_purity: '99.9%', zari_purity: '99.9%', status: 'PENDING' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'ISSUED': return 'success'
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
            Silk Mark Officer
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
              Silk Mark Certificates
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Certificate ID</TableCell>
                    <TableCell>Saree ID</TableCell>
                    <TableCell>Silk Purity</TableCell>
                    <TableCell>Zari Purity</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {certificates.map((cert) => (
                    <TableRow key={cert.id}>
                      <TableCell>{cert.id}</TableCell>
                      <TableCell>{cert.saree_id}</TableCell>
                      <TableCell>{cert.silk_purity}</TableCell>
                      <TableCell>{cert.zari_purity}</TableCell>
                      <TableCell>
                        <Chip label={cert.status} color={getStatusColor(cert.status)} size="small" />
                      </TableCell>
                      <TableCell>
                        <Button size="small" variant="outlined">Issue</Button>
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
