import { useEffect, useState } from 'react'
import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, TablePagination, TextField, InputAdornment, Chip, FormControl, InputLabel, Select, MenuItem } from '@mui/material'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5009/api/v1'

export default function DashboardSKUManager() {
  const { user } = useAuth()
  const navigate = useNavigate()
  const [skus, setSkus] = useState([])
  const [filteredSkus, setFilteredSkus] = useState([])
  const [loading, setLoading] = useState(true)
  const [page, setPage] = useState(0)
  const [rowsPerPage, setRowsPerPage] = useState(25)
  const [filters, setFilters] = useState({
    search: '',
    geographic_hub: '',
    weave_category: '',
    jacquard_capacity: '',
    weight_category: ''
  })
  const [filterOptions, setFilterOptions] = useState({
    geographic_hubs: [],
    weave_categories: [],
    jacquard_capacities: [],
    weight_categories: []
  })

  useEffect(() => {
    fetchSkus()
    fetchFilterOptions()
  }, [])

  useEffect(() => {
    applyFilters()
  }, [filters, skus])

  const fetchSkus = async () => {
    try {
      const response = await fetch(API_URL + '/sku')
      const data = await response.json()
      setSkus(data)
      setFilteredSkus(data)
    } catch (error) {
      console.error('Failed to fetch SKUs:', error)
    } finally {
      setLoading(false)
    }
  }

  const fetchFilterOptions = async () => {
    try {
      const response = await fetch(API_URL + '/sku/filters/options')
      const data = await response.json()
      setFilterOptions(data)
    } catch (error) {
      console.error('Failed to fetch filter options:', error)
    }
  }

  const applyFilters = () => {
    let filtered = [...skus]
    
    if (filters.search) {
      const searchLower = filters.search.toLowerCase()
      filtered = filtered.filter(sku => 
        sku.sku_ref_id.toLowerCase().includes(searchLower) ||
        sku.geographic_hub.toLowerCase().includes(searchLower) ||
        sku.weave_category.toLowerCase().includes(searchLower) ||
        sku.zari_configuration.toLowerCase().includes(searchLower)
      )
    }
    
    if (filters.geographic_hub) {
      filtered = filtered.filter(sku => sku.geographic_hub === filters.geographic_hub)
    }
    if (filters.weave_category) {
      filtered = filtered.filter(sku => sku.weave_category === filters.weave_category)
    }
    if (filters.jacquard_capacity) {
      filtered = filtered.filter(sku => sku.jacquard_capacity === filters.jacquard_capacity)
    }
    if (filters.weight_category) {
      filtered = filtered.filter(sku => sku.weight_category_profile === filters.weight_category)
    }
    
    setFilteredSkus(filtered)
    setPage(0)
  }

  const handleChangePage = (event, newPage) => {
    setPage(newPage)
  }

  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(parseInt(event.target.value, 10))
    setPage(0)
  }

  const getWeightCategoryColor = (category) => {
    switch (category) {
      case 'Lightweight Dress Silk': return 'info'
      case 'Standard Mid-Weight Saree': return 'primary'
      case 'Heavy Bridal Brocade': return 'warning'
      case 'Ultra-Heavy Royal Heritage Pattu': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="xl" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            SKU Product Catalog
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
      </Box>

      {/* Filters */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Grid container spacing={2} alignItems="center">
          <Grid item xs={12} md={3}>
            <TextField
              fullWidth
              label="Search SKU"
              value={filters.search}
              onChange={(e) => setFilters({...filters, search: e.target.value})}
              InputProps={{
                endAdornment: <InputAdornment position="end">🔍</InputAdornment>
              }}
            />
          </Grid>
          <Grid item xs={12} md={2}>
            <FormControl fullWidth>
              <InputLabel>Geographic Hub</InputLabel>
              <Select
                value={filters.geographic_hub}
                label="Geographic Hub"
                onChange={(e) => setFilters({...filters, geographic_hub: e.target.value})}
              >
                <MenuItem value="">All</MenuItem>
                {filterOptions.geographic_hubs.map((hub) => (
                  <MenuItem key={hub} value={hub}>{hub}</MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={2}>
            <FormControl fullWidth>
              <InputLabel>Weave Category</InputLabel>
              <Select
                value={filters.weave_category}
                label="Weave Category"
                onChange={(e) => setFilters({...filters, weave_category: e.target.value})}
              >
                <MenuItem value="">All</MenuItem>
                {filterOptions.weave_categories.map((cat) => (
                  <MenuItem key={cat} value={cat}>{cat}</MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={2}>
            <FormControl fullWidth>
              <InputLabel>Jacquard Capacity</InputLabel>
              <Select
                value={filters.jacquard_capacity}
                label="Jacquard Capacity"
                onChange={(e) => setFilters({...filters, jacquard_capacity: e.target.value})}
              >
                <MenuItem value="">All</MenuItem>
                {filterOptions.jacquard_capacities.map((cap) => (
                  <MenuItem key={cap} value={cap}>{cap}</MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={2}>
            <FormControl fullWidth>
              <InputLabel>Weight Category</InputLabel>
              <Select
                value={filters.weight_category}
                label="Weight Category"
                onChange={(e) => setFilters({...filters, weight_category: e.target.value})}
              >
                <MenuItem value="">All</MenuItem>
                {filterOptions.weight_categories.map((cat) => (
                  <MenuItem key={cat} value={cat}>{cat}</MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={1}>
            <Button
              variant="outlined"
              fullWidth
              onClick={() => setFilters({
                search: '',
                geographic_hub: '',
                weave_category: '',
                jacquard_capacity: '',
                weight_category: ''
              })}
            >
              Reset
            </Button>
          </Grid>
        </Grid>
      </Paper>

      {/* Stats */}
      <Grid container spacing={3} mb={3}>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Total SKUs
              </Typography>
              <Typography variant="h4" component="div" color="primary">
                {skus.length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Filtered Results
              </Typography>
              <Typography variant="h4" component="div" color="secondary">
                {filteredSkus.length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Avg. MFG Cost
              </Typography>
              <Typography variant="h4" component="div" color="success.main">
                ₹{skus.length > 0 ? Math.round(skus.reduce((a, b) => a + b.total_mfg_cost_inr, 0) / skus.length).toLocaleString() : 0}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Avg. Selling Price
              </Typography>
              <Typography variant="h4" component="div" color="warning.main">
                ₹{skus.length > 0 ? Math.round(skus.reduce((a, b) => a + b.selling_price_inr, 0) / skus.length).toLocaleString() : 0}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* SKU Table */}
      <Paper>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>SKU Ref ID</TableCell>
                <TableCell>Geographic Hub</TableCell>
                <TableCell>Weave Category</TableCell>
                <TableCell>Jacquard</TableCell>
                <TableCell>Zari Config</TableCell>
                <TableCell>Total Weight</TableCell>
                <TableCell>MFG Cost</TableCell>
                <TableCell>Selling Price</TableCell>
                <TableCell>Weight Category</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {filteredSkus.slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage).map((sku) => (
                <TableRow key={sku.id} hover>
                  <TableCell>{sku.sku_ref_id}</TableCell>
                  <TableCell>{sku.geographic_hub}</TableCell>
                  <TableCell>{sku.weave_category}</TableCell>
                  <TableCell>{sku.jacquard_capacity}</TableCell>
                  <TableCell>{sku.zari_configuration}</TableCell>
                  <TableCell>{sku.total_saree_weight_g}g</TableCell>
                  <TableCell>₹{sku.total_mfg_cost_inr.toLocaleString()}</TableCell>
                  <TableCell>₹{sku.selling_price_inr.toLocaleString()}</TableCell>
                  <TableCell>
                    <Chip
                      label={sku.weight_category_profile}
                      color={getWeightCategoryColor(sku.weight_category_profile)}
                      size="small"
                    />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
        <TablePagination
          rowsPerPageOptions={[25, 50, 100]}
          component="div"
          count={filteredSkus.length}
          rowsPerPage={rowsPerPage}
          page={page}
          onPageChange={handleChangePage}
          onRowsPerPageChange={handleChangeRowsPerPage}
        />
      </Paper>
    </Container>
  )
}
