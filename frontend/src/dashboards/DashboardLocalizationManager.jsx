import { Container, Typography, Box, Paper, Grid, Card, CardContent, Button, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Chip, FormControl, InputLabel, Select, MenuItem, Tabs, Tab, TextField, InputAdornment, IconButton } from '@mui/material'
import { useState, useEffect } from 'react'
import { useAuth } from '../contexts/AuthContext'
import { useDashboard } from '../contexts/DashboardContext'
import EnterprisePanel from '../components/EnterprisePanel'
import CulturalKnowledgePanel from '../components/CulturalKnowledgePanel'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

export default function DashboardLocalizationManager() {
  const { user } = useAuth()
  const { addNotification } = useDashboard()
  const [tab, setTab] = useState('translations')
  const [selectedLanguage, setSelectedLanguage] = useState('te-IN')

  const languages = [
    { code: 'te-IN', name: 'Telugu', native: 'తెలుగు', speakers: '82M' },
    { code: 'ta-IN', name: 'Tamil', native: 'தமிழ்', speakers: '75M' },
    { code: 'kn-IN', name: 'Kannada', native: 'ಕನ್ನಡ', speakers: '43M' },
    { code: 'hi-IN', name: 'Hindi', native: 'हिन्दी', speakers: '520M' },
    { code: 'bn-IN', name: 'Bengali', native: 'বাংলা', speakers: '300M' },
  ]

  const translations = [
    { key: 'auth.login', te: 'లాగిన్', ta: 'உள்நுழைய', kn: 'ಲಾಗಿನ್', hi: 'लॉगिन', bn: 'লগইন' },
    { key: 'dashboard.input_queue', te: 'ఇన్‌పుట్ క్యూ', ta: 'இன்புட் க்யூ', kn: 'ಇನ್‌ಪುಟ್ ಕ್ಯೂ', hi: 'इनपुट क्यू', bn: 'ইনপুট কিউ' },
    { key: 'scanner.input_scan', te: 'ఇన్‌పుట్ స్కాన్', ta: 'இன்புட் ஸ்கேன்', kn: 'ಇನ್‌ಪುಟ್ ಸ್ಕ್ಯಾನ್', hi: 'इनपुट स्कैन', bn: 'ইনপুট স্ক্যান' },
    { key: 'scanner.output_scan', te: 'ఆఉట్‌పుట్ స్కాన్', ta: 'ஆட்புட் ஸ்கேன்', kn: 'ಆಟ್‌ಪುಟ್ ಸ್ಕ್ಯಾನ್', hi: 'आउटपुट स्कैन', bn: 'আউটপুট স্ক্যান' },
    { key: 'lot.status_pending', te: 'పెండింగ్', ta: 'பெண்டிங்', kn: 'ಪೆಂಡಿಂಗ್', hi: 'पेंडिंग', bn: 'পেন্ডিং' },
    { key: 'lot.status_certified', te: 'సర్టిఫైడ్', ta: 'சர்டிஃபைட்', kn: 'ಸರ್ಟಿಫೈಡ್', hi: 'सर्टिफाइड', bn: 'সার্টিফাইড' },
  ]

  const ttsRequests = [
    { id: 'TTS-001', text: 'Your design is ready to download', language: 'te-IN', status: 'COMPLETED' },
    { id: 'TTS-002', text: 'Payment of ₹1,200 has been processed', language: 'ta-IN', status: 'COMPLETED' },
    { id: 'TTS-003', text: 'Loom maintenance required', language: 'kn-IN', status: 'PROCESSING' },
  ]

  const getStatusColor = (status) => {
    switch (status) {
      case 'COMPLETED': return 'success'
      case 'PROCESSING': return 'warning'
      case 'FAILED': return 'error'
      default: return 'default'
    }
  }

  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <div>
          <Typography variant="h4" component="h1" gutterBottom>
            Localization Management
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Factory: {user?.factory_node_id} | Operator: {user?.full_name}
          </Typography>
        </div>
      </Box>

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="scrollable" scrollButtons="auto">
          <Tab label="Translation Keys" value="translations" />
          <Tab label="TTS Requests" value="tts" />
          <Tab label="DIWALI Cultural Knowledge" value="diwali" />
        </Tabs>
      </Paper>

      {tab === 'translations' && (
        <Grid container spacing={3}>
          <Grid item xs={12} md={4}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>
                Supported Languages
              </Typography>
              <Box display="flex" flexDirection="column" gap={2}>
                {languages.map((lang) => (
                  <Card key={lang.code} variant="outlined">
                    <CardContent>
                      <Typography variant="subtitle1">{lang.name}</Typography>
                      <Typography variant="body2" color="text.secondary">
                        {lang.native} | {lang.speakers} speakers
                      </Typography>
                      <Chip
                        label={lang.code === selectedLanguage ? 'Active' : 'Inactive'}
                        color={lang.code === selectedLanguage ? 'success' : 'default'}
                        size="small"
                        sx={{ mt: 1 }}
                      />
                    </CardContent>
                  </Card>
                ))}
              </Box>
            </Paper>
          </Grid>

          <Grid item xs={12} md={8}>
            <Paper sx={{ p: 2 }}>
              <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
                <Typography variant="h6">
                  Translation Keys
                </Typography>
                <FormControl size="small" sx={{ minWidth: 120 }}>
                  <InputLabel>Language</InputLabel>
                  <Select
                    value={selectedLanguage}
                    label="Language"
                    onChange={(e) => setSelectedLanguage(e.target.value)}
                  >
                    {languages.map((lang) => (
                      <MenuItem key={lang.code} value={lang.code}>
                        {lang.name}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Box>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Key</TableCell>
                      <TableCell>Translation</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {translations.map((t) => (
                      <TableRow key={t.key}>
                        <TableCell>{t.key}</TableCell>
                        <TableCell>{t[selectedLanguage.split('-')[0].toLowerCase()] || t.hi}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            </Paper>
          </Grid>
        </Grid>
      )}

      {tab === 'tts' && (
        <Grid item xs={12}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              TTS Generation Requests
            </Typography>
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Request ID</TableCell>
                    <TableCell>Text</TableCell>
                    <TableCell>Language</TableCell>
                    <TableCell>Status</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {ttsRequests.map((req) => (
                    <TableRow key={req.id}>
                      <TableCell>{req.id}</TableCell>
                      <TableCell>{req.text}</TableCell>
                      <TableCell>{req.language}</TableCell>
                      <TableCell>
                        <Chip
                          label={req.status}
                          color={getStatusColor(req.status)}
                          size="small"
                        />
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>
        </Grid>
      )}

      {tab === 'diwali' && (
        <Box>
          <Paper sx={{ p: 2, mb: 2 }}>
            <Typography variant="h6" gutterBottom>
              DIWALI Cultural Knowledge Base
            </Typography>
            <Typography variant="body2" color="text.secondary" gutterBottom>
              Dataset: nlip/DIWALI — 8,817 cultural concepts across 36 sub-regions and 17 facets.
              Use this for culturally grounded design, SKU, and localization decisions.
            </Typography>
          </Paper>
          <CulturalKnowledgePanel factoryNodeId={user?.factory_node_id} />
        </Box>
      )}

      <EnterprisePanel userId={user?.id} factoryNodeId={user?.factory_node_id} />
    </Container>
  )
}
