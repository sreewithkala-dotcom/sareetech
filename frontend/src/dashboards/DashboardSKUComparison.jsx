import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip, FormControl, InputLabel, Select, MenuItem, Alert } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import EnterprisePanel from '../components/EnterprisePanel'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5009/api/v1'

export default function DashboardSKUComparison() {
  const { user } = useAuth()
  const [skus, setSkus] = useState([])
  const [loading, setLoading] = useState(true)
  const [selectedSku1, setSelectedSku1] = useState('')
  const [selectedSku2, setSelectedSku2] = useState('')
  const [comparison, setComparison] = useState(null)

  useEffect(() => {
    fetchSkus()
  }, [])

  const fetchSkus = async () => {
    try {
      const response = await fetch(`${API_URL}/sku?limit=100`)
      const data = await response.json()
      setSkus(data)
    } catch (error) {
      console.error('Failed to fetch SKUs:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleCompare = () => {
    if (!selectedSku1 || !selectedSku2) return
    const sku1 = skus.find(s => s.sku_ref_id === selectedSku1)
    const sku2 = skus.find(s => s.sku_ref_id === selectedSku2)
    setComparison({ sku1, sku2 })
  }

  const getDifferenceColor = (val1, val2) => {
    if (val1 === val2) return 'default'
    return val1 > val2 ? 'error' : 'success'
  }

  const renderComparisonRow = (label, val1, val2, format = (v) => v) => {
    if (!comparison) return null
    const diff = val2 !== 0 ? ((val1 - val2) / val2 * 100).toFixed(1) : 0
    return (
      <TableRow key={label}>
        <TableCell>{label}</TableCell>
        <TableCell>{format(val1)}</TableCell>
        <TableCell>{format(val2)}</TableCell>
        <TableCell>
          <Chip
            label={`${diff > 0 ? '+' : ''}${diff}%`}
            color={getDifferenceColor(val1, val2)}
            size="small"
          />
        </TableCell>
      </TableRow>
    )
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            SKU Comparison Tool
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
      </Box>

      <Grid container spacing={3}>
        {/* Selection Form */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Select SKUs to Compare
            </Typography>
            <Grid container spacing={2} alignItems="center">
              <Grid item xs={12} md={4}>
                <FormControl fullWidth disabled={loading}>
                  <InputLabel>SKU 1</InputLabel>
                  <Select
                    value={selectedSku1}
                    label="SKU 1"
                    onChange={(e) => setSelectedSku1(e.target.value)}
                  >
                    <MenuItem value="">Select SKU</MenuItem>
                    {skus.map((sku) => (
                      <MenuItem key={sku.sku_ref_id} value={sku.sku_ref_id}>
                        {sku.sku_ref_id} - {sku.geographic_hub} - {sku.jacquard_capacity}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={4}>
                <FormControl fullWidth disabled={loading}>
                  <InputLabel>SKU 2</InputLabel>
                  <Select
                    value={selectedSku2}
                    label="SKU 2"
                    onChange={(e) => setSelectedSku2(e.target.value)}
                  >
                    <MenuItem value="">Select SKU</MenuItem>
                    {skus.map((sku) => (
                      <MenuItem key={sku.sku_ref_id} value={sku.sku_ref_id}>
                        {sku.sku_ref_id} - {sku.geographic_hub} - {sku.jacquard_capacity}
                      </MenuItem>
                    ))}
                  </FormControl>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={4}>
                <Button
                  variant="contained"
                  fullWidth
                  size="large"
                  onClick={handleCompare}
                  disabled={!selectedSku1 || !selectedSku2}
                >
                  Compare SKUs
                </Button>
              </Grid>
            </Grid>
          </Paper>
        </Grid>

        {/* Comparison Results */}
        {comparison && comparison.sku1 && comparison.sku2 && (
          <Grid item xs={12}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Comparison Results
              </Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Attribute</TableCell>
                      <TableCell>{comparison.sku1.sku_ref_id}</TableCell>
                      <TableCell>{comparison.sku2.sku_ref_id}</TableCell>
                      <TableCell>Difference</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {renderComparisonRow('Geographic Hub', comparison.sku1.geographic_hub, comparison.sku2.geographic_hub)}
                    {renderComparisonRow('Weave Category', comparison.sku1.weave_category, comparison.sku2.weave_category)}
                    {renderComparisonRow('Jacquard Capacity', comparison.sku1.jacquard_capacity, comparison.sku2.jacquard_capacity)}
                    {renderComparisonRow('Zari Configuration', comparison.sku1.zari_configuration, comparison.sku2.zari_configuration)}
                    {renderComparisonRow('Total Weight', comparison.sku1.total_saree_weight_g, comparison.sku2.total_saree_weight_g, (v) => `${v}g`)}
                    {renderComparisonRow('Yarn Raw Cost', comparison.sku1.yarn_raw_cost_inr, comparison.sku2.yarn_raw_cost_inr, (v) => `₹${v.toLocaleString()}`)}
                    {renderComparisonRow('Zari Raw Cost', comparison.sku1.zari_raw_cost_inr, comparison.sku2.zari_raw_cost_inr, (v) => `₹${v.toLocaleString()}`)}
                    {renderComparisonRow('Labor Surcharge', comparison.sku1.labor_surcharge_inr, comparison.sku2.labor_surcharge_inr, (v) => `₹${v.toLocaleString()}`)}
                    {renderComparisonRow('Total MFG Cost', comparison.sku1.total_mfg_cost_inr, comparison.sku2.total_mfg_cost_inr, (v) => `₹${v.toLocaleString()}`)}
                    {renderComparisonRow('MRP', comparison.sku1.mrp_inr, comparison.sku2.mrp_inr, (v) => `₹${v.toLocaleString()}`)}
                    {renderComparisonRow('Selling Price', comparison.sku1.selling_price_inr, comparison.sku2.selling_price_inr, (v) => `₹${v.toLocaleString()}`)}
                    {renderComparisonRow('Min Floor Price', comparison.sku1.min_floor_price_inr, comparison.sku2.min_floor_price_inr, (v) => `₹${v.toLocaleString()}`)}
                    {renderComparisonRow('Weight Category', comparison.sku1.weight_category_profile, comparison.sku2.weight_category_profile)}
                  </TableBody>
                </Table>
              </TableContainer>
            </Paper>
          </Grid>
        )}

        {comparison && comparison.sku1 && comparison.sku2 && (
          <Grid item xs={12}>
            <Alert severity="info">
              <Typography variant="body2">
                <strong>Design Feasibility Note:</strong> Compare jacquard capacity, weight categories, and cost structures to determine which SKU is more suitable for your target market and production capabilities.
              </Typography>
            </Alert>
          </Grid>
        )}
      </Grid>
      <EnterprisePanel userId={user?.id} factoryNodeId={user?.factory_node_id} />
    </Container>
  )
}
