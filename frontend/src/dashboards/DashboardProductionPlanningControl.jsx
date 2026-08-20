import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip, TextField, Select, MenuItem, FormControl, InputLabel, Tabs, Tab, Alert, LinearProgress } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useNavigate } from 'react-router-dom'
import { useDashboard } from '../contexts/DashboardContext'
import EnterprisePanel from '../components/EnterprisePanel'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

export default function DashboardProductionPlanningControl() {
  const { user } = useAuth()
  const navigate = useNavigate()
  const { addNotification } = useDashboard()
  const [tab, setTab] = useState('dashboard')

  const [stats, setStats] = useState({
    totalOrders: 0,
    pendingOrders: 0,
    activePlans: 0,
    totalProduction: 0,
    qualityPassRate: 0,
    oeeScore: 0
  })
  const [orders, setOrders] = useState([])
  const [plans, setPlans] = useState([])
  const [boms, setBoms] = useState([])
  const [trackingLogs, setTrackingLogs] = useState([])
  const [qualityChecks, setQualityChecks] = useState([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    fetchStats()
    fetchOrders()
    fetchPlans()
  }, [])

  const fetchStats = async () => {
    setLoading(true)
    try {
      const token = localStorage.getItem('access_token')
      const factoryId = user?.factory_node_id || 'FACT-BLR-01'

      const [ordersRes, plansRes, trackingRes, qualityRes] = await Promise.all([
        fetch(`${API_URL}/ppc/orders?factory_node_id=${encodeURIComponent(factoryId)}&limit=1000`, {
          headers: { 'Authorization': `Bearer ${token}` }
        }),
        fetch(`${API_URL}/ppc/plans?factory_node_id=${encodeURIComponent(factoryId)}&limit=1000`, {
          headers: { 'Authorization': `Bearer ${token}` }
        }),
        fetch(`${API_URL}/ppc/tracking?factory_node_id=${encodeURIComponent(factoryId)}&limit=1000`, {
          headers: { 'Authorization': `Bearer ${token}` }
        }),
        fetch(`${API_URL}/ppc/quality-checks?factory_node_id=${encodeURIComponent(factoryId)}&limit=1000`, {
          headers: { 'Authorization': `Bearer ${token}` }
        })
      ])

      const [ordersData, plansData, trackingData, qualityData] = await Promise.all([
        ordersRes.json(),
        plansRes.json(),
        trackingRes.json(),
        qualityRes.json()
      ])

      const ordersList = ordersData.orders || []
      const plansList = plansData.plans || []
      const trackingList = trackingData.logs || []
      const qualityList = qualityData.checks || []

      const totalProduction = trackingList.reduce((sum, t) => sum + (parseFloat(t.output_meters) || 0), 0)
      const passedChecks = qualityList.filter(q => q.status === 'PASSED' || q.status === 'CERTIFIED').length
      const qualityPassRate = qualityList.length > 0 ? (passedChecks / qualityList.length * 100) : 0
      const avgOee = plansList.length > 0 ? plansList.reduce((sum, p) => sum + (parseFloat(p.oee_score) || 0), 0) / plansList.length : 0

      setStats({
        totalOrders: ordersList.length,
        pendingOrders: ordersList.filter(o => o.status === 'PENDING').length,
        activePlans: plansList.filter(p => p.status === 'IN_PROGRESS').length,
        totalProduction: totalProduction.toFixed(2),
        qualityPassRate: qualityPassRate.toFixed(1),
        oeeScore: avgOee.toFixed(1)
      })

      setOrders(ordersList.slice(0, 10))
      setPlans(plansList.slice(0, 10))
      setTrackingLogs(trackingList.slice(0, 10))
      setQualityChecks(qualityList.slice(0, 10))
    } catch (error) {
      console.error('Failed to fetch PPC stats:', error)
      addNotification('Failed to load PPC dashboard data', 'error')
    } finally {
      setLoading(false)
    }
  }

  const fetchOrders = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const factoryId = user?.factory_node_id || 'FACT-BLR-01'
      const response = await fetch(`${API_URL}/ppc/orders?factory_node_id=${encodeURIComponent(factoryId)}&limit=50`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setOrders(data.orders || [])
    } catch (error) {
      console.error('Failed to fetch orders:', error)
    }
  }

  const fetchPlans = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const factoryId = user?.factory_node_id || 'FACT-BLR-01'
      const response = await fetch(`${API_URL}/ppc/plans?factory_node_id=${encodeURIComponent(factoryId)}&limit=50`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setPlans(data.plans || [])
    } catch (error) {
      console.error('Failed to fetch plans:', error)
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'COMPLETED':
      case 'PASSED':
      case 'CERTIFIED':
      case 'DELIVERED':
        return 'success'
      case 'IN_PROGRESS':
      case 'CONFIRMED':
      case 'ACTIVE':
        return 'info'
      case 'PENDING':
        return 'warning'
      case 'FAILED':
      case 'REJECT':
      case 'CANCELLED':
        return 'error'
      default:
        return 'default'
    }
  }

  const StatCard = ({ title, value, subtitle, color }) => (
    <Card>
      <CardContent>
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
      </Box>

      {loading && <LinearProgress sx={{ mb: 2 }} />}

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="scrollable" scrollButtons="auto">
          <Tab label="Dashboard" value="dashboard" />
          <Tab label="Orders" value="orders" />
          <Tab label="Production Plans" value="plans" />
          <Tab label="BOM" value="bom" />
          <Tab label="Tracking" value="tracking" />
          <Tab label="Quality & Compliance" value="quality" />
          <Tab label="Mobile Sync" value="mobile" />
        </Tabs>
      </Paper>

      {tab === 'dashboard' && (
        <Grid container spacing={3}>
          <Grid item xs={12} sm={6} md={4}>
            <StatCard title="Total Orders" value={stats.totalOrders} subtitle={`${stats.pendingOrders} pending`} color="primary" />
          </Grid>
          <Grid item xs={12} sm={6} md={4}>
            <StatCard title="Active Plans" value={stats.activePlans} subtitle="Currently running" color="info" />
          </Grid>
          <Grid item xs={12} sm={6} md={4}>
            <StatCard title="Total Production" value={`${stats.totalProduction}m`} subtitle="Meters produced" color="success" />
          </Grid>
          <Grid item xs={12} sm={6} md={4}>
            <StatCard title="Quality Pass Rate" value={`${stats.qualityPassRate}%`} subtitle="Passed / Total" color={stats.qualityPassRate > 90 ? 'success' : 'warning'} />
          </Grid>
          <Grid item xs={12} sm={6} md={4}>
            <StatCard title="OEE Score" value={`${stats.oeeScore}%`} subtitle="Average OEE" color="secondary" />
          </Grid>
        </Grid>
      )}

      {tab === 'orders' && (
        <Paper sx={{ p: 2 }}>
          <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h6">Orders</Typography>
            <Button variant="contained" size="small" onClick={() => alert('Create Order form would open here')}>
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
          {orders.length === 0 && <Alert severity="info" sx={{ mt: 2 }}>No orders found. Create your first order to get started.</Alert>}
        </Paper>
      )}

      {tab === 'plans' && (
        <Paper sx={{ p: 2 }}>
          <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h6">Production Plans</Typography>
            <Button variant="contained" size="small" onClick={() => alert('Create Plan form would open here')}>
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
          {plans.length === 0 && <Alert severity="info" sx={{ mt: 2 }}>No production plans found. Create your first plan to get started.</Alert>}
        </Paper>
      )}

      {tab === 'bom' && (
        <Paper sx={{ p: 2 }}>
          <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h6">Bill of Materials</Typography>
            <Button variant="contained" size="small" onClick={() => alert('Create BOM form would open here')}>
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
          {boms.length === 0 && <Alert severity="info" sx={{ mt: 2 }}>No BOMs found. Create your first BOM to get started.</Alert>}
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
                  <TableCell>Downtime (min)</TableCell>
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
          {trackingLogs.length === 0 && <Alert severity="info" sx={{ mt: 2 }}>No tracking logs found. Production tracking data will appear here.</Alert>}
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
                    <TableCell><Chip label={check.defect_severity || '-'} size="small" color={check.defect_severity === 'CRITICAL' ? 'error' : 'default'} /></TableCell>
                    <TableCell><Chip label={check.grade || '-'} size="small" color={check.grade === 'A_PLUS' || check.grade === 'A' ? 'success' : 'default'} /></TableCell>
                    <TableCell>{check.ai_defect_score ? `${(check.ai_defect_score * 100).toFixed(1)}%` : '-'}</TableCell>
                    <TableCell><Chip label={check.status} size="small" color={getStatusColor(check.status)} /></TableCell>
                    <TableCell>{check.certified_at ? new Date(check.certified_at).toLocaleDateString() : '-'}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          {qualityChecks.length === 0 && <Alert severity="info" sx={{ mt: 2 }}>No quality checks found. Quality data will appear here.</Alert>}
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
                <Button variant="contained" fullWidth sx={{ mb: 1 }} onClick={() => addNotification('Mobile sync API ready at /api/v1/ppc/mobile/sync', 'info')}>
                  Sync Now
                </Button>
                <Button variant="outlined" fullWidth onClick={() => alert('Mobile SDK documentation would open here')}>
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
                <Button variant="contained" fullWidth color="secondary" onClick={() => addNotification('Push notification API ready at /api/v1/ppc/mobile/push', 'info')}>
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
