import { Box, Paper, Typography, Grid, Card, CardContent, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Tab, Divider } from '@mui/material'
import { useState, useEffect } from 'react'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

export default function EnterprisePanel({ userId, factoryNodeId }) {
  const [tab, setTab] = useState('ai')
  const [aiLogs, setAiLogs] = useState([])
  const [notifications, setNotifications] = useState([])
  const [financeEntries, setFinanceEntries] = useState([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (userId) {
      fetchAiLogs()
      fetchNotifications()
      fetchFinanceEntries()
    }
  }, [userId])

  const fetchAiLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/enterprise/ai/inference-logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setAiLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch AI logs:', error)
    }
  }

  const fetchNotifications = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/enterprise/notifications`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setNotifications(data.notifications || [])
    } catch (error) {
      console.error('Failed to fetch notifications:', error)
    }
  }

  const fetchFinanceEntries = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/enterprise/finance/journal-entries`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setFinanceEntries(data.entries || [])
    } catch (error) {
      console.error('Failed to fetch finance entries:', error)
    }
  }

  const getPriorityColor = (priority) => {
    switch (priority) {
      case 'CRITICAL': return 'error'
      case 'HIGH': return 'warning'
      case 'NORMAL': return 'info'
      case 'LOW': return 'default'
      default: return 'default'
    }
  }

  const getAiStatusColor = (edgeDeployed) => {
    return edgeDeployed ? 'success' : 'primary'
  }

  return (
    <Paper sx={{ mt: 3, p: 2 }}>
      <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        🏢 Enterprise Integration Layer
        <Chip label="AI/ML + Finance + HR + Procurement + Supply Chain + IoT" size="small" color="secondary" />
      </Typography>
      <Divider sx={{ mb: 2 }} />

      <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="scrollable" scrollButtons="auto">
          <Tab label={`AI/ML Insights (${aiLogs.length})`} value="ai" />
          <Tab label={`Notifications (${notifications.length})`} value="notifications" />
          <Tab label={`Finance (${financeEntries.length})`} value="finance" />
          <Tab label="HR & Guild" value="hr" />
          <Tab label="Procurement" value="procurement" />
          <Tab label="Supply Chain" value="supply" />
          <Tab label="IoT & Telemetry" value="iot" />
        </Tabs>
      </Box>

      {/* AI/ML Tab */}
      {tab === 'ai' && (
        <Box>
          {aiLogs.length === 0 ? (
            <Typography color="text.secondary">No AI inference logs yet. AI services will appear here when triggered.</Typography>
          ) : (
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Service</TableCell>
                    <TableCell>Entity</TableCell>
                    <TableCell>Confidence</TableCell>
                    <TableCell>Latency (ms)</TableCell>
                    <TableCell>Deployment</TableCell>
                    <TableCell>Timestamp</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {aiLogs.map((log) => (
                    <TableRow key={log.id || log.inference_id}>
                      <TableCell><Chip label={log.service_name} size="small" color="secondary" /></TableCell>
                      <TableCell>{log.entity_type}: {log.entity_id}</TableCell>
                      <TableCell>
                        {log.confidence_score ? `${(log.confidence_score * 100).toFixed(1)}%` : 'N/A'}
                      </TableCell>
                      <TableCell>{log.inference_latency_ms || 'N/A'}</TableCell>
                      <TableCell>
                        <Chip label={log.edge_deployed ? 'Edge' : 'Cloud'} size="small" color={getAiStatusColor(log.edge_deployed)} />
                      </TableCell>
                      <TableCell>{new Date(log.created_at).toLocaleString()}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </Box>
      )}

      {/* Notifications Tab */}
      {tab === 'notifications' && (
        <Box>
          {notifications.length === 0 ? (
            <Typography color="text.secondary">No notifications yet.</Typography>
          ) : (
            notifications.slice(0, 20).map((notif) => (
              <Alert key={notif.id || notif.notification_id} severity={notif.priority === 'CRITICAL' ? 'error' : notif.priority === 'HIGH' ? 'warning' : 'info'} sx={{ mb: 1 }}>
                <Typography variant="subtitle2">{notif.title}</Typography>
                <Typography variant="body2">{notif.message}</Typography>
                <Typography variant="caption" color="text.secondary">
                  {notif.notification_type} | {notif.channel} | {new Date(notif.created_at).toLocaleString()}
                </Typography>
              </Alert>
            ))
          )}
        </Box>
      )}

      {/* Finance Tab */}
      {tab === 'finance' && (
        <Box>
          {financeEntries.length === 0 ? (
            <Typography color="text.secondary">No finance entries yet.</Typography>
          ) : (
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Journal ID</TableCell>
                    <TableCell>Type</TableCell>
                    <TableCell>Debit</TableCell>
                    <TableCell>Credit</TableCell>
                    <TableCell>Amount</TableCell>
                    <TableCell>Posted At</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {financeEntries.map((entry) => (
                    <TableRow key={entry.id || entry.journal_id}>
                      <TableCell><code>{entry.journal_id}</code></TableCell>
                      <TableCell><Chip label={entry.entry_type} size="small" /></TableCell>
                      <TableCell>{entry.debit_account}</TableCell>
                      <TableCell>{entry.credit_account}</TableCell>
                      <TableCell>₹{parseFloat(entry.amount).toLocaleString()}</TableCell>
                      <TableCell>{new Date(entry.posted_at).toLocaleString()}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </Box>
      )}

      {/* HR Tab */}
      {tab === 'hr' && (
        <Box>
          <Typography color="text.secondary">
            HR & Guild features: Weaver profiles, skill matrices, guild membership, payroll runs, and attendance tracking.
            Integrated with all manufacturing roles.
          </Typography>
        </Box>
      )}

      {/* Procurement Tab */}
      {tab === 'procurement' && (
        <Box>
          <Typography color="text.secondary">
            Procurement features: Purchase requisitions, purchase orders, invoice matching, supplier scoring, and PR automation.
            Integrated with Store Inventory Manager and Finance.
          </Typography>
        </Box>
      )}

      {/* Supply Chain Tab */}
      {tab === 'supply' && (
        <Box>
          <Typography color="text.secondary">
            Supply Chain features: B2B orders, dispatch manifests, inventory bins, movements, reconciliation batches, and shrinkage detection.
            Integrated with all material and dispatch roles.
          </Typography>
        </Box>
      )}

      {/* IoT Tab */}
      {tab === 'iot' && (
        <Box>
          <Typography color="text.secondary">
            IoT features: Device registry, loom telemetry, maintenance work orders, and edge AI model deployment.
            Integrated with all manufacturing and IoT Device Manager roles.
          </Typography>
        </Box>
      )}
    </Paper>
  )
}
