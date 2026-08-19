import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip, Avatar, Alert } from '@mui/material'
import { useState } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'

export default function DashboardBuyBackManager() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [selectedGuarantee, setSelectedGuarantee] = useState(null)
  const [valuationResult, setValuationResult] = useState(null)

  const buybackRequests = [
    {
      id: 'BB-001',
      saree_id: 'SARE-2024-0001',
      customer_name: 'Priya Sharma',
      request_date: '2024-01-15',
      ai_valuation: 45000,
      payout_status: 'PENDING',
      scan_data: { fabric_thinning_pct: 5, gold_oxidation_pct: 2 }
    },
    {
      id: 'BB-002',
      saree_id: 'SARE-2024-0002',
      customer_name: 'Lakshmi Devi',
      request_date: '2024-01-14',
      ai_valuation: 52000,
      payout_status: 'APPROVED',
      scan_data: { fabric_thinning_pct: 3, gold_oxidation_pct: 1 }
    },
    {
      id: 'BB-003',
      saree_id: 'SARE-2024-0003',
      customer_name: 'Kavitha Reddy',
      request_date: '2024-01-14',
      ai_valuation: 38000,
      payout_status: 'PENDING',
      scan_data: { fabric_thinning_pct: 8, gold_oxidation_pct: 4 }
    },
  ]

  const handleValuate = async (nfcId, scanData) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch('http://localhost:5006/api/v1/buyback/valuate', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ nfc_id: nfcId, scan_data: scanData })
      })
      
      const data = await response.json()
      if (response.ok) {
        setValuationResult(data)
        addNotification(`Buy-back value: ₹${data.buyback_value_inr}`, 'success')
      } else {
        addNotification(data.error || 'Valuation failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to valuate buy-back', 'error')
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'PENDING': return 'warning'
      case 'APPROVED': return 'info'
      case 'PAID': return 'success'
      case 'REJECTED': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Buy-Back Management
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
      </Box>

      {valuationResult && (
        <Alert severity="success" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            Valuation Result for {valuationResult.nfc_id}
          </Typography>
          <Typography variant="body2">
            <strong>Base Value:</strong> ₹{valuationResult.base_value_inr?.toLocaleString()}
          </Typography>
          <Typography variant="body2">
            <strong>Buy-Back Value:</strong> ₹{valuationResult.buyback_value_inr?.toLocaleString()}
          </Typography>
          {valuationResult.sku_ref_id && (
            <>
              <Typography variant="body2">
                <strong>SKU:</strong> {valuationResult.sku_ref_id}
              </Typography>
              <Typography variant="body2">
                <strong>Weight Category:</strong> {valuationResult.weight_category_profile}
              </Typography>
            </>
          )}
          <Typography variant="body2">
            <strong>Fabric Thinning:</strong> {valuationResult.depreciation_breakdown?.fabric_thinning_pct}%
          </Typography>
          <Typography variant="body2">
            <strong>Gold Oxidation:</strong> {valuationResult.depreciation_breakdown?.gold_oxidation_pct}%
          </Typography>
        </Alert>
      )}

      <Grid container spacing={3}>
        {/* Buy-Back Requests */}
        <Grid item xs={12} md={8}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Buy-Back Requests
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>ID</TableCell>
                    <TableCell>Saree ID</TableCell>
                    <TableCell>Customer</TableCell>
                    <TableCell>AI Valuation</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {buybackRequests.map((request) => (
                    <TableRow key={request.id}>
                      <TableCell>{request.id}</TableCell>
                      <TableCell>{request.saree_id}</TableCell>
                      <TableCell>{request.customer_name}</TableCell>
                      <TableCell>₹{request.ai_valuation.toLocaleString()}</TableCell>
                      <TableCell>
                        <Chip
                          label={request.payout_status}
                          color={getStatusColor(request.payout_status)}
                          size="small"
                        />
                      </TableCell>
                      <TableCell>
                        <Button
                          size="small"
                          variant="outlined"
                          onClick={() => handleValuate(request.nfc_id, request.scan_data)}
                        >
                          Valuate
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>
        </Grid>

        {/* Quick Stats */}
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Buy-Back Statistics
            </Typography>
            <Box display="flex" flexDirection="column" gap={2}>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Typography>Pending Requests</Typography>
                <Typography variant="h6" color="warning.main">12</Typography>
              </Box>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Typography>Approved This Month</Typography>
                <Typography variant="h6" color="success.main">₹2.4L</Typography>
              </Box>
              <Box display="flex" justifyContent="space-between" alignItems="center">
                <Typography>Avg. Depreciation</Typography>
                <Typography variant="h6">18%</Typography>
              </Box>
            </Box>
          </Paper>
        </Grid>
      </Grid>
    </Container>
  )
}
