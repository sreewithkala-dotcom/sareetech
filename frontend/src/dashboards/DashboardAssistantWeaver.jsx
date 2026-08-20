import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, TextField, Select, MenuItem, FormControl, InputLabel, Chip, Alert, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

export default function DashboardAssistantWeaver() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [submitting, setSubmitting] = useState(false)
  const [logs, setLogs] = useState([])
  const [alarms, setAlarms] = useState([])
  const [validationResult, setValidationResult] = useState(null)
  const [form, setForm] = useState({
    active_loom_id: '',
    lead_weaver_id: '',
    shift_start_time: new Date().toISOString().slice(0, 16),
    shift_end_time: new Date(Date.now() + 28800000).toISOString().slice(0, 16),
    pirns_replaced_count: 0,
    logged_warp_breaks: 0,
    logged_weft_breaks: 0,
    weft_spool_lot_id: '',
    weft_feeder_position: 'Feeder 1 (Ground Silk)',
    yarn_tail_transfer_status: 'Spliced & Tail-Locked',
    zari_tension_disc_setting: 'Micro-Tension Active (Fine Zari)',
    warp_break_repair_count: 0,
    mending_knot_type: "Weaver's Micro-Knot (Short Tail)",
    dropper_rethread_verification: 'Threaded & Dropper Active',
    comber_board_cleaning_status: 'Cleaned / Compressed Air Blowout Done',
    shift_handover_readiness: 'READY_FOR_NEXT_SHIFT',
    assistant_weaver_approval_state: 'ACTIVE_LOGGING'
  })

  useEffect(() => {
    fetchLogs()
    fetchAlarms()
  }, [])

  const fetchLogs = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/assistant-weaver/logs`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setLogs(data.logs || [])
    } catch (error) {
      console.error('Failed to fetch logs:', error)
    }
  }

  const fetchAlarms = async () => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/assistant-weaver/loom-alarms`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setAlarms(data.alarms || [])
    } catch (error) {
      console.error('Failed to fetch alarms:', error)
    }
  }

  const handleSubmit = async () => {
    setSubmitting(true)
    setValidationResult(null)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/assistant-weaver/logs`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(form)
      })
      const data = await response.json()
      if (response.ok) {
        addNotification(`Shift log ${data.assistant_job_log_id} submitted`, 'success')
        setValidationResult(data)
        fetchLogs()
      } else {
        addNotification(data.error || 'Submission failed', 'error')
        setValidationResult(data)
      }
    } catch (error) {
      addNotification('Failed to submit shift log', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleApprove = async (logId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/assistant-weaver/logs/${logId}/approve`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ notes: 'Approved by lead weaver' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Shift log approved', 'success')
        fetchLogs()
      } else {
        addNotification(data.error || 'Approval failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to approve shift log', 'error')
    }
  }

  const handleReject = async (logId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/assistant-weaver/logs/${logId}/reject`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ reason: 'Rejected by lead weaver - unresolved issues' })
      })
      const data = await response.json()
      if (response.ok) {
        addNotification('Shift log rejected', 'warning')
        fetchLogs()
      } else {
        addNotification(data.error || 'Rejection failed', 'error')
      }
    } catch (error) {
      addNotification('Failed to reject shift log', 'error')
    }
  }

  const getApprovalColor = (state) => {
    switch (state) {
      case 'PASSED_SHIFT_AUDIT': return 'success'
      case 'REJECTED_UNRESOLVED_WARP_BREAKS': return 'error'
      case 'ACTIVE_LOGGING': return 'warning'
      default: return 'default'
    }
  }

  const getAlarmSeverity = (severity) => {
    switch (severity) {
      case 'CRITICAL': return 'error'
      case 'WARNING': return 'warning'
      default: return 'info'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Assistant Weaver — Floor Operations
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
      </Box>

      {validationResult && validationResult.validation_errors && validationResult.validation_errors.length > 0 && (
        <Alert severity="error" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            Guardrail Violations
          </Typography>
          {validationResult.validation_errors.map((err, idx) => (
            <Typography key={idx} variant="body2">
              • [{err.code}] {err.message}
            </Typography>
          ))}
        </Alert>
      )}

      {validationResult && !validationResult.validation_errors && (
        <Alert severity="success" sx={{ mb: 3 }}>
          Shift log submitted successfully. Log ID: {validationResult.assistant_job_log_id}
        </Alert>
      )}

      <Grid container spacing={3}>
        {/* Shift & Operator Linkage */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Shift & Operator Linkage
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Active Loom ID"
                  value={form.active_loom_id}
                  onChange={(e) => setForm({ ...form, active_loom_id: e.target.value })}
                  placeholder="LOOM-2401"
                  required
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Lead Weaver ID"
                  value={form.lead_weaver_id}
                  onChange={(e) => setForm({ ...form, lead_weaver_id: e.target.value })}
                  placeholder="Scan or enter lead weaver ID"
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Shift Start Time"
                  type="datetime-local"
                  value={form.shift_start_time}
                  onChange={(e) => setForm({ ...form, shift_start_time: e.target.value })}
                  InputLabelProps={{ shrink: true }}
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Shift End Time"
                  type="datetime-local"
                  value={form.shift_end_time}
                  onChange={(e) => setForm({ ...form, shift_end_time: e.target.value })}
                  InputLabelProps={{ shrink: true }}
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Pirns Replaced Count"
                  type="number"
                  value={form.pirns_replaced_count}
                  onChange={(e) => setForm({ ...form, pirns_replaced_count: parseInt(e.target.value) || 0 })}
                  inputProps={{ min: 0 }}
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Logged Warp Breaks"
                  type="number"
                  value={form.logged_warp_breaks}
                  onChange={(e) => setForm({ ...form, logged_warp_breaks: parseInt(e.target.value) || 0 })}
                  inputProps={{ min: 0 }}
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Logged Weft Breaks"
                  type="number"
                  value={form.logged_weft_breaks}
                  onChange={(e) => setForm({ ...form, logged_weft_breaks: parseInt(e.target.value) || 0 })}
                  inputProps={{ min: 0 }}
                />
              </Grid>
            </Grid>
          </Paper>
        </Grid>

        {/* Category A: Weft Material Creeling */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Category A: Weft Material Creeling & Replenishment
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Weft Spool Lot ID"
                  value={form.weft_spool_lot_id}
                  onChange={(e) => setForm({ ...form, weft_spool_lot_id: e.target.value })}
                  placeholder="Dye lot / batch code"
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <FormControl fullWidth>
                  <InputLabel>Weft Feeder Position</InputLabel>
                  <Select
                    value={form.weft_feeder_position}
                    label="Weft Feeder Position"
                    onChange={(e) => setForm({ ...form, weft_feeder_position: e.target.value })}
                  >
                    <MenuItem value="Feeder 1 (Ground Silk)">Feeder 1 (Ground Silk)</MenuItem>
                    <MenuItem value="Feeder 2 (Zari Extra Weft)">Feeder 2 (Zari Extra Weft)</MenuItem>
                    <MenuItem value="Feeder 3 (Contrast Border Silk)">Feeder 3 (Contrast Border Silk)</MenuItem>
                    <MenuItem value="Feeder 4 (Secondary Zari)">Feeder 4 (Secondary Zari)</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={4}>
                <FormControl fullWidth>
                  <InputLabel>Yarn Tail Transfer Status</InputLabel>
                  <Select
                    value={form.yarn_tail_transfer_status}
                    label="Yarn Tail Transfer Status"
                    onChange={(e) => setForm({ ...form, yarn_tail_transfer_status: e.target.value })}
                  >
                    <MenuItem value="Spliced & Tail-Locked">Spliced & Tail-Locked</MenuItem>
                    <MenuItem value="Single Spool (No Reserve)">Single Spool (No Reserve)</MenuItem>
                    <MenuItem value="Unverified / Loose Tail">Unverified / Loose Tail</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={4}>
                <FormControl fullWidth>
                  <InputLabel>Zari Tension Disc Setting</InputLabel>
                  <Select
                    value={form.zari_tension_disc_setting}
                    label="Zari Tension Disc Setting"
                    onChange={(e) => setForm({ ...form, zari_tension_disc_setting: e.target.value })}
                  >
                    <MenuItem value="Micro-Tension Active (Fine Zari)">Micro-Tension Active (Fine Zari)</MenuItem>
                    <MenuItem value="Standard Friction">Standard Friction</MenuItem>
                    <MenuItem value="Low-Tension Light">Low-Tension Light</MenuItem>
                    <MenuItem value="Bypassed">Bypassed</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Paper>
        </Grid>

        {/* Category B: Warp Repair & Maintenance */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Category B: Warp Repair & Maintenance Logging
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Warp Break Repair Count"
                  type="number"
                  value={form.warp_break_repair_count}
                  onChange={(e) => setForm({ ...form, warp_break_repair_count: parseInt(e.target.value) || 0 })}
                  inputProps={{ min: 0 }}
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <FormControl fullWidth>
                  <InputLabel>Mending Knot Type</InputLabel>
                  <Select
                    value={form.mending_knot_type}
                    label="Mending Knot Type"
                    onChange={(e) => setForm({ ...form, mending_knot_type: e.target.value })}
                  >
                    <MenuItem value="Weaver's Micro-Knot (Short Tail)">Weaver's Micro-Knot (Short Tail)</MenuItem>
                    <MenuItem value="Standard Overhand Knot">Standard Overhand Knot</MenuItem>
                    <MenuItem value="Spliced Loop">Spliced Loop</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={4}>
                <FormControl fullWidth>
                  <InputLabel>Dropper Rethread Verification</InputLabel>
                  <Select
                    value={form.dropper_rethread_verification}
                    label="Dropper Rethread Verification"
                    onChange={(e) => setForm({ ...form, dropper_rethread_verification: e.target.value })}
                  >
                    <MenuItem value="Threaded & Dropper Active">Threaded & Dropper Active</MenuItem>
                    <MenuItem value="Bypassed Dropper (Unsafe)">Bypassed Dropper (Unsafe)</MenuItem>
                    <MenuItem value="Missing Dropper">Missing Dropper</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={4}>
                <FormControl fullWidth>
                  <InputLabel>Comber Board Cleaning Status</InputLabel>
                  <Select
                    value={form.comber_board_cleaning_status}
                    label="Comber Board Cleaning Status"
                    onChange={(e) => setForm({ ...form, comber_board_cleaning_status: e.target.value })}
                  >
                    <MenuItem value="Cleaned / Compressed Air Blowout Done">Cleaned / Compressed Air Blowout Done</MenuItem>
                    <MenuItem value="Pending Cleaning">Pending Cleaning</MenuItem>
                    <MenuItem value="Heavy Fly Buildup">Heavy Fly Buildup</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Paper>
        </Grid>

        {/* Category C: Final Shift Status */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Category C: Final Shift Status & System Verification
            </Typography>
            <Grid container spacing={2}>
              <Grid item xs={12} md={6}>
                <FormControl fullWidth>
                  <InputLabel>Shift Handover Readiness</InputLabel>
                  <Select
                    value={form.shift_handover_readiness}
                    label="Shift Handover Readiness"
                    onChange={(e) => setForm({ ...form, shift_handover_readiness: e.target.value })}
                  >
                    <MenuItem value="READY_FOR_NEXT_SHIFT">READY_FOR_NEXT_SHIFT</MenuItem>
                    <MenuItem value="PENDING_WARP_BREAK_REPAIR">PENDING_WARP_BREAK_REPAIR</MenuItem>
                    <MenuItem value="LOW_WEFT_RESERVE_WARNING">LOW_WEFT_RESERVE_WARNING</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={6}>
                <FormControl fullWidth>
                  <InputLabel>Assistant Weaver Approval State</InputLabel>
                  <Select
                    value={form.assistant_weaver_approval_state}
                    label="Assistant Weaver Approval State"
                    onChange={(e) => setForm({ ...form, assistant_weaver_approval_state: e.target.value })}
                  >
                    <MenuItem value="ACTIVE_LOGGING">ACTIVE_LOGGING</MenuItem>
                    <MenuItem value="PASSED_SHIFT_AUDIT">PASSED_SHIFT_AUDIT</MenuItem>
                    <MenuItem value="REJECTED_UNRESOLVED_WARP_BREAKS">REJECTED_UNRESOLVED_WARP_BREAKS</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12}>
                <Button
                  variant="contained"
                  size="large"
                  onClick={handleSubmit}
                  disabled={submitting}
                  fullWidth
                >
                  {submitting ? 'Submitting...' : 'Submit Shift Log'}
                </Button>
              </Grid>
            </Grid>
          </Paper>
        </Grid>

        {/* Loom Breakage Alarms */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Loom Friction & Breakage Alarms
            </Typography>
            {alarms.length === 0 ? (
              <Typography color="text.secondary">No active alarms</Typography>
            ) : (
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Loom ID</TableCell>
                      <TableCell>Alarm Type</TableCell>
                      <TableCell>Rate/Hr</TableCell>
                      <TableCell>Severity</TableCell>
                      <TableCell>Message</TableCell>
                      <TableCell>Created</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {alarms.map((alarm) => (
                      <TableRow key={alarm.id}>
                        <TableCell>{alarm.loom_id}</TableCell>
                        <TableCell>{alarm.alarm_type}</TableCell>
                        <TableCell>{alarm.breakage_rate_per_hour}</TableCell>
                        <TableCell>
                          <Chip label={alarm.severity} color={getAlarmSeverity(alarm.severity)} size="small" />
                        </TableCell>
                        <TableCell>{alarm.message}</TableCell>
                        <TableCell>{new Date(alarm.created_at).toLocaleString()}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
          </Paper>
        </Grid>

        {/* Recent Shift Logs */}
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Recent Shift Logs
            </Typography>
            {logs.length === 0 ? (
              <Typography color="text.secondary">No shift logs yet</Typography>
            ) : (
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Log ID</TableCell>
                      <TableCell>Loom ID</TableCell>
                      <TableCell>Shift Start</TableCell>
                      <TableCell>Pirns</TableCell>
                      <TableCell>Warp Breaks</TableCell>
                      <TableCell>Weft Breaks</TableCell>
                      <TableCell>Status</TableCell>
                      <TableCell>Actions</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {logs.map((log) => (
                      <TableRow key={log.id}>
                        <TableCell>{log.assistant_job_log_id}</TableCell>
                        <TableCell>{log.active_loom_id}</TableCell>
                        <TableCell>{new Date(log.shift_start_time).toLocaleString()}</TableCell>
                        <TableCell>{log.pirns_replaced_count}</TableCell>
                        <TableCell>{log.logged_warp_breaks}</TableCell>
                        <TableCell>{log.logged_weft_breaks}</TableCell>
                        <TableCell>
                          <Chip label={log.approval_state} color={getApprovalColor(log.approval_state)} size="small" />
                        </TableCell>
                        <TableCell>
                          {log.approval_state === 'ACTIVE_LOGGING' && (
                            <>
                              <Button size="small" variant="outlined" onClick={() => handleApprove(log.id)}>
                                Approve
                              </Button>
                              <Button size="small" variant="outlined" color="error" onClick={() => handleReject(log.id)} sx={{ ml: 1 }}>
                                Reject
                              </Button>
                            </>
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
          </Paper>
        </Grid>
      </Grid>
    </Container>
  )
}
