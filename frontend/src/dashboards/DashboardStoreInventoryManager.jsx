import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

export default function DashboardStoreInventoryManager() {
  const { user } = useAuth()
  const navigate = useNavigate()

  const inventory = [
    { id: 'INV-001', item: 'Raw Silk', quantity: '500 kg', unit: 'KG', reorder_level: '100 kg', status: 'OK' },
    { id: 'INV-002', item: 'Gold Zari', quantity: '50 kg', unit: 'KG', reorder_level: '20 kg', status: 'LOW' },
    { id: 'INV-003', item: 'Silver Zari', quantity: '75 kg', unit: 'KG', reorder_level: '25 kg', status: 'OK' },
    { id: 'INV-004', item: 'Dye - Maroon', quantity: '200 L', unit: 'L', reorder_level: '50 L', status: 'OK' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'OK': return 'success'
      case 'LOW': return 'warning'
      case 'CRITICAL': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Store & Inventory Management
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
              Inventory Items
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Item ID</TableCell>
                    <TableCell>Item Name</TableCell>
                    <TableCell>Quantity</TableCell>
                    <TableCell>Unit</TableCell>
                    <TableCell>Reorder Level</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {inventory.map((item) => (
                    <TableRow key={item.id}>
                      <TableCell>{item.id}</TableCell>
                      <TableCell>{item.item}</TableCell>
                      <TableCell>{item.quantity}</TableCell>
                      <TableCell>{item.unit}</TableCell>
                      <TableCell>{item.reorder_level}</TableCell>
                      <TableCell>
                        <Chip label={item.status} color={getStatusColor(item.status)} size="small" />
                      </TableCell>
                      <TableCell>
                        <Button size="small" variant="outlined">Update</Button>
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
