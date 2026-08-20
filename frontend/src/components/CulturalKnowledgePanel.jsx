import { useState, useEffect } from 'react'
import { Box, Paper, Typography, Grid, Card, CardContent, Chip, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, TextField, InputAdornment, IconButton, Tabs, Tab, FormControl, InputLabel, Select, MenuItem } from '@mui/material'
import { Search as SearchIcon } from '@mui/icons-material'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

export default function CulturalKnowledgePanel({ factoryNodeId }) {
  const [tab, setTab] = useState('concepts')
  const [facets, setFacets] = useState([])
  const [states, setStates] = useState([])
  const [concepts, setConcepts] = useState([])
  const [benchmarks, setBenchmarks] = useState([])
  const [selectedFacet, setSelectedFacet] = useState('')
  const [selectedState, setSelectedState] = useState('')
  const [searchQuery, setSearchQuery] = useState('')
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (tab === 'facets') fetchFacets()
    else if (tab === 'states') fetchStates()
    else if (tab === 'concepts') fetchConcepts()
    else if (tab === 'benchmarks') fetchBenchmarks()
  }, [tab])

  useEffect(() => {
    if (tab === 'concepts') fetchConcepts()
  }, [selectedFacet, selectedState])

  const fetchFacets = async () => {
    setLoading(true)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/enterprise/diwali/facets?factory_node_id=${encodeURIComponent(factoryNodeId || 'FACT-BLR-01')}`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setFacets(data.facets || [])
    } catch (error) {
      console.error('Failed to fetch facets:', error)
    } finally {
      setLoading(false)
    }
  }

  const fetchStates = async () => {
    setLoading(true)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/enterprise/diwali/states?factory_node_id=${encodeURIComponent(factoryNodeId || 'FACT-BLR-01')}`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setStates(data.states || [])
    } catch (error) {
      console.error('Failed to fetch states:', error)
    } finally {
      setLoading(false)
    }
  }

  const fetchConcepts = async () => {
    setLoading(true)
    try {
      const token = localStorage.getItem('access_token')
      const params = new URLSearchParams({
        factory_node_id: factoryNodeId || 'FACT-BLR-01',
        limit: '100'
      })
      if (selectedFacet) params.append('facet_id', selectedFacet)
      if (selectedState) params.append('state_id', selectedState)
      if (searchQuery.trim()) params.append('q', searchQuery.trim())

      const response = await fetch(`${API_URL}/enterprise/diwali/concepts?${params.toString()}`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setConcepts(data.concepts || [])
    } catch (error) {
      console.error('Failed to fetch concepts:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleSearch = () => {
    fetchConcepts()
  }

  const fetchBenchmarks = async () => {
    setLoading(true)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/enterprise/culture/benchmark-sources?limit=100`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setBenchmarks(data.sources || [])
    } catch (error) {
      console.error('Failed to fetch benchmarks:', error)
    } finally {
      setLoading(false)
    }
  }

  return (
    <Paper sx={{ p: 2, mt: 2 }}>
      <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="scrollable" scrollButtons="auto">
          <Tab label={`Concepts (${concepts.length})`} value="concepts" />
          <Tab label={`Facets (${facets.length})`} value="facets" />
          <Tab label={`States (${states.length})`} value="states" />
          <Tab label={`Benchmarks (${benchmarks.length})`} value="benchmarks" />
        </Tabs>
      </Box>

      {tab === 'concepts' && (
        <Box>
          <Box sx={{ display: 'flex', gap: 1, mb: 2, flexWrap: 'wrap' }}>
            <FormControl size="small" sx={{ minWidth: 200 }}>
              <InputLabel>Facet</InputLabel>
              <Select
                value={selectedFacet}
                label="Facet"
                onChange={(e) => setSelectedFacet(e.target.value)}
              >
                <MenuItem value="">All Facets</MenuItem>
                {facets.map((facet) => (
                  <MenuItem key={facet.facet_id} value={facet.facet_id}>{facet.facet_name}</MenuItem>
                ))}
              </Select>
            </FormControl>
            <FormControl size="small" sx={{ minWidth: 200 }}>
              <InputLabel>State / Sub-region</InputLabel>
              <Select
                value={selectedState}
                label="State / Sub-region"
                onChange={(e) => setSelectedState(e.target.value)}
              >
                <MenuItem value="">All States</MenuItem>
                {states.map((state) => (
                  <MenuItem key={state.state_id} value={state.state_id}>{state.state_name}</MenuItem>
                ))}
              </Select>
            </FormControl>
            <TextField
              size="small"
              label="Search concepts..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
              sx={{ flex: 1, minWidth: 200 }}
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    <IconButton onClick={handleSearch} size="small">
                      <SearchIcon />
                    </IconButton>
                  </InputAdornment>
                )
              }}
            />
          </Box>

          {loading ? (
            <Typography>Loading cultural concepts...</Typography>
          ) : concepts.length === 0 ? (
            <Typography color="text.secondary">No concepts found. Use the DIWALI indexer to populate the database.</Typography>
          ) : (
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Concept</TableCell>
                    <TableCell>Facet</TableCell>
                    <TableCell>State / Region</TableCell>
                    <TableCell>Language</TableCell>
                    <TableCell>AI Confidence</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {concepts.slice(0, 50).map((c) => (
                    <TableRow key={c.concept_id}>
                      <TableCell>
                        <Typography variant="body2" fontWeight="medium">{c.concept}</Typography>
                        {c.description && (
                          <Typography variant="caption" color="text.secondary" display="block">
                            {c.description.slice(0, 120)}{c.description.length > 120 ? '...' : ''}
                          </Typography>
                        )}
                      </TableCell>
                      <TableCell>{c.facet_name || '-'}</TableCell>
                      <TableCell>{c.state_name || '-'} {c.region ? `(${c.region})` : ''}</TableCell>
                      <TableCell><Chip label={c.language_code} size="small" /></TableCell>
                      <TableCell>
                        {c.ai_confidence_score ? `${(c.ai_confidence_score * 100).toFixed(1)}%` : '-'}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </Box>
      )}

      {tab === 'facets' && (
        <Box>
          {loading ? (
            <Typography>Loading facets...</Typography>
          ) : facets.length === 0 ? (
            <Typography color="text.secondary">No facets indexed yet.</Typography>
          ) : (
            <Grid container spacing={2}>
              {facets.map((facet) => (
                <Grid item xs={12} sm={6} md={4} key={facet.facet_id}>
                  <Card variant="outlined">
                    <CardContent>
                      <Typography variant="subtitle1" gutterBottom>{facet.facet_name}</Typography>
                      <Typography variant="body2" color="text.secondary" gutterBottom>
                        {facet.facet_description || 'No description'}
                      </Typography>
                      <Chip label={facet.is_active ? 'Active' : 'Inactive'} size="small" color={facet.is_active ? 'success' : 'default'} />
                    </CardContent>
                  </Card>
                </Grid>
              ))}
            </Grid>
          )}
        </Box>
      )}

      {tab === 'states' && (
        <Box>
          {loading ? (
            <Typography>Loading states...</Typography>
          ) : states.length === 0 ? (
            <Typography color="text.secondary">No states indexed yet.</Typography>
          ) : (
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>State</TableCell>
                    <TableCell>Region</TableCell>
                    <TableCell>Language</TableCell>
                    <TableCell>Silk Type</TableCell>
                    <TableCell>Zari Type</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {states.map((state) => (
                    <TableRow key={state.state_id}>
                      <TableCell>{state.state_name}</TableCell>
                      <TableCell>{state.region || '-'}</TableCell>
                      <TableCell>{state.primary_language} {state.primary_dialect ? `(${state.primary_dialect})` : ''}</TableCell>
                      <TableCell>{state.dominant_silk_type || '-'}</TableCell>
                      <TableCell>{state.dominant_zari_type || '-'}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </Box>
      )}

      {tab === 'benchmarks' && (
        <Box>
          {loading ? (
            <Typography>Loading benchmark sources...</Typography>
          ) : benchmarks.length === 0 ? (
            <Typography color="text.secondary">No benchmark sources indexed yet.</Typography>
          ) : (
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Title</TableCell>
                    <TableCell>Year</TableCell>
                    <TableCell>Type</TableCell>
                    <TableCell>Culture Scope</TableCell>
                    <TableCell>Venue</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {benchmarks.map((b) => (
                    <TableRow key={b.source_id}>
                      <TableCell>
                        <Typography variant="body2" fontWeight="medium">
                          {b.url ? (
                            <a href={b.url} target="_blank" rel="noopener noreferrer">{b.title}</a>
                          ) : (
                            b.title
                          )}
                        </Typography>
                        {b.authors?.length > 0 && (
                          <Typography variant="caption" color="text.secondary" display="block">
                            {b.authors.slice(0, 3).join(', ')}{b.authors.length > 3 ? ' et al.' : ''}
                          </Typography>
                        )}
                      </TableCell>
                      <TableCell>{b.year || '-'}</TableCell>
                      <TableCell><Chip label={b.benchmark_type} size="small" /></TableCell>
                      <TableCell>{b.culture_scope || '-'}</TableCell>
                      <TableCell>{b.venue || '-'}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </Box>
      )}
    </Paper>
  )
}
