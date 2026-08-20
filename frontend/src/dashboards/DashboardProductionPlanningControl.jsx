import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip, TextField, Select, MenuItem, FormControl, InputLabel, Tabs, Tab, Alert, LinearProgress, IconButton, Tooltip } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'
import EnterprisePanel from '../components/EnterprisePanel'
import RefreshIcon from '@mui/icons-material/Refresh'
import AddIcon from '@mui/icons-material/Add'
import VisibilityIcon from '@mui/icons-material/Visibility'
import SyncIcon from '@mui/icons-material/Sync'
import NotificationsActiveIcon from '@mui/icons-material/NotificationsActive'
import TrendingUpIcon from '@mui/icons-material/TrendingUp'
import AssignmentIcon from '@mui/icons-material/Assignment'
import FactoriesIcon from '@mui/icons-material/Factories'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import WarningIcon from '@mui/icons-material/Warning'
import ErrorIcon from '@mui/icons-material/Error'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

export default function DashboardProductionPlanningControl() {
  const { user } = useAuth()
  const navigate = useNavigate()
  const { addNotification } = useDashboard()
  const [tab, setTab] = useState('dashboard')
  const [loading, setLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)

  const [summary, setSummary] = useState({
    pending_orders: 0, in_production_orders: 0, delivered_orders: 0,
    active_plans: 0, total_target_meters: 0, total_actual_meters: 0,
    avg_oee: 0, quality_pass_rate_pct: 0, active_production_lines: 0, allocated_resources: 0
  })
  const [recentLines, setRecentLines] = useState([])
  const [recentQuality, setRecentQuality] = useState([])
  const [recentAllocations, setRecentAllocations] = useState([])
  const [orders, setOrders] = useState([])
  const [plans, setPlans] = useState([])
  const [boms, setBoms] = useState([])
  const [trackingLogs, setTrackingLogs] = useState([])
  const [qualityChecks, setQualityChecks] = useState([])
  const [lines, setLines] = useState([])
  const [shifts, setShifts] = useState([])
  const [resources, setResources] = useState([])

  const factoryId = user?.factory_node_id || 'FACT-BLR-01'

  const fetchWithAuth = async (url) => {
    const token = localStorage.getItem('access_token')
    const response = await fetch(url, { headers: { 'Authorization': `Bearer ${token}` } })
    return response.json()
  }

  const fetchDashboardData = async () => {
    setRefreshing(true)
    try {
      const data = await fetchWithAuth(`${API_URL}/ppc/dashboard?factory_node_id=${encodeURIComponent(factoryId)}`)
      if (data.summary) setSummary(data.summary)
      if (data.recent_lines) setRecentLines(data.recent_lines)
      if (data.recent_quality) setRecentQuality(data.recent_quality)
      if (data.recent_allocations) setRecentAllocations(data.recent_allocations)
    } catch (error) {
      console.error('Failed to fetch dashboard:', error)
      addNotification('Failed to load PPC dashboard', 'error')
    } finally {
      setRefreshing(false)
    }
  }

  const fetchOrders = async () => {
    try {
      const data = await fetchWithAuth(`${API_URL}/ppc/orders?factory_node_id=${encodeURIComponent(factoryId)}&limit=50`)
      setOrders(data.orders || [])
    } catch (error) {
      console.error('Failed to fetch orders:', error)
    }
  }

  const fetchPlans = async () => {
    try {
      const data = await fetchWithAuth(`${API_URL}/ppc/plans?factory_node_id=${encodeURIComponent(factoryId)}&limit=50`)
      setPlans(data.plans || [])
    } catch (error) {
      console.error('Failed to fetch plans:', error)
    }
  }

  const fetchBoms = async () => {
    try {
      const data = await fetchWithAuth(`${API_URL}/ppc/bom?factory_node_id=${encodeURIComponent(factoryId)}&limit=50`)
      setBoms(data.boms || [])
    } catch (error) {
      console.error('Failed to fetch BOMs:', error)
    }
  }

  const fetchTracking = async () => {
    try {
      const data = await fetchWithAuth(`${API_URL}/ppc/tracking?factory_node_id=${encodeURIComponent(factoryId)}&limit=50`)
      setTrackingLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch tracking:', error)
    }
  }

  const fetchQuality = async () => {
    try {
      const data = await fetchWithAuth(`${API_URL}/ppc/quality-checks?factory_node_id=${encodeURIComponent(factoryId)}&limit=50`)
      setQualityChecks(data.checks || [])
    } catch (error) {
      console.error('Failed to fetch quality checks:', error)
    }
  }

  const fetchLines = async () => {
    try {
      const data = await fetchWithAuth(`${API_URL}/ppc/lines?factory_node_id=${encodeURIComponent(factoryId)}&limit=100`)
      setLines(data.lines || [])
    } catch (error) {
      console.error('Failed to fetch lines:', error)
    }
  }

  const fetchShifts = async () => {
    try {
      const data = await fetchWithAuth(`${API_URL}/ppc/shifts?factory_node_id=${encodeURIComponent(factoryId)}&limit=100`)
      setShifts(data.shifts || [])
    } catch (error) {
      console.error('Failed to fetch shifts:', error)
    }
  }

  const fetchResources = async () => {
    try {
      const data = await fetchWithAuth(`${API_URL}/ppc/resources?factory_node_id=${encodeURIComponent(factoryId)}&limit=100`)
      setResources(data.allocations || [])
    } catch (error) {
      console.error('Failed to fetch resources:', error)
    }
  }

  const refreshAll = () => {
    fetchDashboardData()
    fetchOrders()
    fetchPlans()
    fetchBoms()
    fetchTracking()
    fetchQuality()
    fetchLines()
    fetchShifts()
    fetchResources()
  }

  useEffect(() => {
    refreshAll()
  }, [])

  const getStatusColor = (status) => {
    switch (status) {
      case 'COMPLETED': case 'PASSED': case 'CERTIFIED': case 'DELIVERED': case 'RELEASED': return 'success'
      case 'IN_PROGRESS': case 'CONFIRMED': case 'ACTIVE': case 'ALLOCATED': case 'IN_USE': return 'info'
      case 'PENDING': case 'SCHEDULED': case 'PLANNED': return 'warning'
      case 'FAILED': case 'REJECT': case 'CANCELLED': case 'SHUTDOWN': case 'MAINTENANCE': return 'error'
      default: return 'default'
    }
  }

  const StatCard = ({ title, value, subtitle, color, icon }) => (
    <Card>
      <CardContent>
        <Box display="flex" justifyContent="space-between" alignItems="flex-start">
          <Box>
            <Typography variant="h6" gutterBottom color={color}>
              {title}
            </Typography>
            <Typography variant="h4" color={color}>
              {value}
            </Typography>
            {subtitle && (
              <Typography variant="body2" color="text.secondary">
                {subtitle}
              </Typography>
            )}
          </Box>
          <Box sx={{ color }}>{icon}</Box>
        </Box>
      </CardContent>
    </Card>
  )

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Production Planning & Control
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
        <Box>
          <Tooltip title="Refresh All Data">
            <IconButton onClick={refreshAll} disabled={refreshing}>
              <RefreshIcon />
            </IconButton>
          </Tooltip>
        </Box>
      </Box>

      {refreshing && <LinearProgress sx={{ mb: 2 }} />}

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="scrollable" scrollButtons="auto">
          <Tab label="Dashboard" value="dashboard" />
          <Tab label="Orders" value="orders" />
          <Tab label="Production Plans" value="plans" />
          <Tab label="BOM" value="bom" />
          <Tab label="Tracking" value="tracking" />
          <Tab label="Quality & Compliance" value="quality" />
          <Tab label="Lines & Shifts" value="lines" />
          <Tab label="Resources" value="resources" />
          <Tab label="Mobile" value="mobile" />
        </Tabs>
      </Paper>

      {tab === 'dashboard' && (
        <Grid container spacing={3}>
          <Grid item xs={12} sm={6} md={4}>
            <StatCard title="Pending Orders" value={summary.pending_orders} subtitle={`${summary.in_production_orders} in production`} color="primary.main" icon={<AssignmentIcon />} />
          </Grid>
          <Grid item xs={12} sm={6} md={4}>
            <StatCard title="Active Plans" value={summary.active_plans} subtitle="Currently running" color="info.main" icon={<TrendingUpIcon />} />
          </Grid>
          <Grid item xs={12} sm={6} md={4}>
            <StatCard title="Total Production" value={`${parseFloat(summary.total_actual_meters || 0).toFixed(1)}m`} subtitle={`Target: ${parseFloat(summary.total_target_meters || 0).toFixed(1)}m`} color="success.main" icon={<FactoriesIcon />} />
          </Grid>
          <Grid item xs={12} sm={6} md={4}>
            <StatCard title="Quality Pass Rate" value={`${summary.quality_pass_rate_pct}%`} subtitle="Passed / Total" color={summary.quality_pass_rate_pct > 90 ? 'success.main' : 'warning.main'} icon={<CheckCircleIcon />} />
          </Grid>
          <Grid item xs={12} sm={6} md={4}>
            <StatCard title="OEE Score" value={`${summary.avg_oee}%`} subtitle="Average OEE" color="secondary.main" icon={<TrendingUpIcon />} />
          </Grid>
          <Grid item xs={12} sm={6} md={4}>
            <StatCard title="Active Lines" value={summary.active_production_lines} subtitle={`${summary.allocated_resources} resources allocated`} color="warning.main" icon={<FactoriesIcon />} />
          </Grid>
        </Grid>
      )}

      {tab === 'orders' && (
        <Paper sx={{ p: 2 }}>
          <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h6">Orders ({orders.length})</Typography>
            <Button variant="contained" size="small" startIcon={<AddIcon />} onClick={() => addNotification('Order creation form would open here', 'info')}>
              New Order
            </Button>
          </Box>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Order ID</TableCell>
                  <TableCell>Type</TableCell>
                  <TableCell>Buyer</TableCell>
                  <TableCell>Qty</TableCell>
                  <TableCell>Delivery Date</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Priority</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {orders.map((order) => (
                  <TableRow key={order.id || order.order_id}>
                    <TableCell>{order.order_id}</TableCell>
                    <TableCell><Chip label={order.order_type} size="small" /></TableCell>
                    <TableCell>{order.buyer_name || order.buyer_id}</TableCell>
                    <TableCell>{order.quantity}</TableCell>
                    <TableCell>{order.delivery_date}</TableCell>
                    <TableCell><Chip label={order.status} size="small" color={getStatusColor(order.status)} /></TableCell>
                    <TableCell><Chip label={order.priority} size="small" variant="outlined" /></TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          {orders.length === 0 && <Alert severity="info" sx={{ mt: 2 }}>No orders found.</Alert>}
        </Paper>
      )}

      {tab === 'plans' && (
        <Paper sx={{ p: 2 }}>
          <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h6">Production Plans ({plans.length})</Typography>
            <Button variant="contained" size="small" startIcon={<AddIcon />} onClick={() => addNotification('Plan creation form would open here', 'info')}>
              New Plan
            </Button>
          </Box>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Plan ID</TableCell>
                  <TableCell>Name</TableCell>
                  <TableCell>Type</TableCell>
                  <TableCell>Duration</TableCell>
                  <TableCell>Target</TableCell>
                  <TableCell>Actual</TableCell>
                  <TableCell>OEE</TableCell>
                  <TableCell>Status</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {plans.map((plan) => (
                  <TableRow key={plan.id || plan.plan_id}>
                    <TableCell>{plan.plan_id}</TableCell>
                    <TableCell>{plan.plan_name}</TableCell>
                    <TableCell><Chip label={plan.plan_type} size="small" /></TableCell>
                    <TableCell>{plan.plan_start_date} to {plan.plan_end_date}</TableCell>
                    <TableCell>{plan.total_production_target}</TableCell>
                    <TableCell>{plan.total_production_actual || 0}</TableCell>
                    <TableCell>{plan.oee_score ? `${plan.oee_score}%` : '-'}</TableCell>
                    <TableCell><Chip label={plan.status} size="small" color={getStatusColor(plan.status)} /></TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          {plans.length === 0 && <Alert severity="info" sx={{ mt: 2 }}>No production plans found.</Alert>}
        </Paper>
      )}

      {tab === 'bom' && (
        <Paper sx={{ p: 2 }}>
          <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h6">Bill of Materials ({boms.length})</Typography>
            <Button variant="contained" size="small" startIcon={<AddIcon />} onClick={() => addNotification('BOM creation form would open here', 'info')}>
              New BOM
            </Button>
          </Box>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>BOM ID</TableCell>
                  <TableCell>SKU</TableCell>
                  <TableCell>Version</TableCell>
                  <TableCell>Cost/Unit</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Approved</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {boms.map((bom) => (
                  <TableRow key={bom.id || bom.bom_id}>
                    <TableCell>{bom.bom_id}</TableCell>
                    <TableCell>{bom.sku_name || bom.sku_id}</TableCell>
                    <TableCell>{bom.version}</TableCell>
                    <TableCell>{bom.total_cost_per_unit ? `₹${bom.total_cost_per_unit}` : '-'}</TableCell>
                    <TableCell><Chip label={bom.status} size="small" color={getStatusColor(bom.status)} /></TableCell>
                    <TableCell>{bom.approved_at ? 'Yes' : 'No'}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          {boms.length === 0 && <Alert severity="info" sx={{ mt: 2 }}>No BOMs found.</Alert>}
        </Paper>
      )}

      {tab === 'tracking' && (
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>Production Tracking Logs</Typography>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Tracking ID</TableCell>
                  <TableCell>Loom</TableCell>
                  <TableCell>Operation</TableCell>
                  <TableCell>Output (m)</TableCell>
                  <TableCell>Defects</TableCell>
                  <TableCell>Downtime</TableCell>
                  <TableCell>AI Anomaly</TableCell>
                  <TableCell>Recorded At</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {trackingLogs.map((log) => (
                  <TableRow key={log.id || log.tracking_id}>
                    <TableCell>{log.tracking_id}</TableCell>
                    <TableCell>{log.loom_id || '-'}</TableCell>
                    <TableCell>{log.operation_type}</TableCell>
                    <TableCell>{log.output_meters || 0}</TableCell>
                    <TableCell>{log.defect_count || 0}</TableCell>
                    <TableCell>{log.downtime_minutes || 0}</TableCell>
                    <TableCell>
                      <Chip label={log.ai_anomaly_flag ? 'FLAG' : 'OK'} size="small" color={log.ai_anomaly_flag ? 'error' : 'success'} />
                    </TableCell>
                    <TableCell>{new Date(log.recorded_at).toLocaleString()}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          {trackingLogs.length === 0 && <Alert severity="info" sx={{ mt: 2 }}>No tracking logs found.</Alert>}
        </Paper>
      )}

      {tab === 'quality' && (
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>Quality & Compliance Checks</Typography>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Check ID</TableCell>
                  <TableCell>Type</TableCell>
                  <TableCell>Defect</TableCell>
                  <TableCell>Severity</TableCell>
                  <TableCell>Grade</TableCell>
                  <TableCell>AI Score</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Certified</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {qualityChecks.map((check) => (
                  <TableRow key={check.id || check.check_id}>
                    <TableCell>{check.check_id}</TableCell>
                    <TableCell><Chip label={check.check_type} size="small" /></TableCell>
                    <TableCell>{check.defect_type || '-'}</TableCell>
                    <TableCell>
                      <Chip label={check.defect_severity || '-'} size="small" color={check.defect_severity === 'CRITICAL' ? 'error' : 'default'} />
                    </TableCell>
                    <TableCell>
                      <Chip label={check.grade || '-'} size="small" color={['A_PLUS', 'A'].includes(check.grade) ? 'success' : 'default'} />
                    </TableCell>
                    <TableCell>{check.ai_defect_score ? `${(check.ai_defect_score * 100).toFixed(1)}%` : '-'}</TableCell>
                    <TableCell><Chip label={check.status} size="small" color={getStatusColor(check.status)} /></TableCell>
                    <TableCell>{check.certified_at ? new Date(check.certified_at).toLocaleDateString() : '-'}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          {qualityChecks.length === 0 && <Alert severity="info" sx={{ mt: 2 }}>No quality checks found.</Alert>}
        </Paper>
      )}

      {tab === 'lines' && (
        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Production Lines ({lines.length})</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Line ID</TableCell>
                      <TableCell>Name</TableCell>
                      <TableCell>Type</TableCell>
                      <TableCell>Looms</TableCell>
                      <TableCell>Workers</TableCell>
                      <TableCell>OEE</TableCell>
                      <TableCell>Status</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {lines.map((line) => (
                      <TableRow key={line.id || line.line_id}>
                        <TableCell>{line.line_id}</TableCell>
                        <TableCell>{line.line_name}</TableCell>
                        <TableCell><Chip label={line.line_type} size="small" /></TableCell>
                        <TableCell>{line.total_looms}</TableCell>
                        <TableCell>{line.total_workers}</TableCell>
                        <TableCell>{line.oee_score ? `${line.oee_score}%` : '-'}</TableCell>
                        <TableCell><Chip label={line.status} size="small" color={getStatusColor(line.status)} /></TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
              {lines.length === 0 && <Alert severity="info" sx={{ mt: 2 }}>No production lines configured.</Alert>}
            </Paper>
          </Grid>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Shifts ({shifts.length})</Typography>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Shift ID</TableCell>
                      <TableCell>Line</TableCell>
                      <TableCell>Name</TableCell>
                      <TableCell>Type</TableCell>
                      <TableCell>Date</TableCell>
                      <TableCell>Workers</TableCell>
                      <TableCell>Status</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {shifts.map((shift) => (
                      <TableRow key={shift.id || shift.shift_id}>
                        <TableCell>{shift.shift_id}</TableCell>
                        <TableCell>{shift.line_id || '-'}</TableCell>
                        <TableCell>{shift.shift_name}</TableCell>
                        <TableCell><Chip label={shift.shift_type} size="small" /></TableCell>
                        <TableCell>{shift.date}</TableCell>
                        <TableCell>{shift.actual_workers_present || 0}/{shift.total_workers_scheduled || 0}</TableCell>
                        <TableCell><Chip label={shift.status} size="small" color={getStatusColor(shift.status)} /></TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
              {shifts.length === 0 && <Alert severity="info" sx={{ mt: 2 }}>No shifts scheduled.</Alert>}
            </Paper>
          </Grid>
        </Grid>
      )}

      {tab === 'resources' && (
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6" gutterBottom>Resource Allocation ({resources.length})</Typography>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Allocation ID</TableCell>
                  <TableCell>Resource Type</TableCell>
                  <TableCell>Resource ID</TableCell>
                  <TableCell>Qty</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Plan Line</TableCell>
                  <TableCell>Order</TableCell>
                  <TableCell>Allocated At</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {resources.map((res) => (
                  <TableRow key={res.id || res.allocation_id}>
                    <TableCell>{res.allocation_id}</TableCell>
                    <TableCell><Chip label={res.resource_type} size="small" /></TableCell>
                    <TableCell>{res.resource_id}</TableCell>
                    <TableCell>{res.allocated_qty} {res.unit_of_measure}</TableCell>
                    <TableCell>
                      <Chip label={res.status} size="small" color={getStatusColor(res.status)} />
                    </TableCell>
                    <TableCell>{res.line_no || '-'}</TableCell>
                    <TableCell>{res.order_id || '-'}</TableCell>
                    <TableCell>{new Date(res.allocated_at).toLocaleString()}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          {resources.length === 0 && <Alert severity="info" sx={{ mt: 2 }}>No resources allocated. Allocate looms, workers, or materials to production plan lines.</Alert>}
        </Paper>
      )}

      {tab === 'mobile' && (
        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Mobile Sync</Typography>
              <Typography variant="body2" color="text.secondary" gutterBottom>
                Real-time sync for Android & iOS apps. Offline-first with conflict resolution.
              </Typography>
              <Box sx={{ mt: 2 }}>
                <Button variant="contained" fullWidth sx={{ mb: 1 }} startIcon={<SyncIcon />} onClick={() => addNotification('Mobile sync API ready at /api/v1/ppc/mobile/sync', 'info')}>
                  Sync Now
                </Button>
                <Button variant="outlined" fullWidth onClick={() => addNotification('Mobile SDK documentation available', 'info')}>
                  View Mobile SDK Docs
                </Button>
              </Box>
            </Paper>
          </Grid>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Push Notifications</Typography>
              <Typography variant="body2" color="text.secondary" gutterBottom>
                Send push notifications to Android & iOS devices for order updates, quality alerts, and production milestones.
              </Typography>
              <Box sx={{ mt: 2 }}>
                <Button variant="contained" fullWidth color="secondary" startIcon={<NotificationsActiveIcon />} onClick={() => addNotification('Push notification API ready at /api/v1/ppc/mobile/push', 'info')}>
                  Test Push Notification
                </Button>
              </Box>
            </Paper>
          </Grid>
        </Grid>
      )}

      <EnterprisePanel userId={user?.id} factoryNodeId={user?.factory_node_id} />
    </Container>
  )
}
