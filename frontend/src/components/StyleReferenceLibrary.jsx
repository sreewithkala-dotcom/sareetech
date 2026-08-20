import { useState, useEffect } from 'react'
import { Box, Paper, Typography, Grid, Card, CardContent, CardMedia, Button, Chip, TextField, InputAdornment, Tabs, Tab, Alert, IconButton } from '@mui/material'
import { Search as SearchIcon, Add as AddIcon, Link as LinkIcon, StarBorder as StarBorderIcon } from '@mui/icons-material'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5003/api/v1'

export default function StyleReferenceLibrary({ user, factoryNodeId, skuOptions = [], onMapSku }) {
  const [images, setImages] = useState([])
  const [selectedImage, setSelectedImage] = useState(null)
  const [imageTags, setImageTags] = useState([])
  const [tab, setTab] = useState('library')
  const [searchQuery, setSearchQuery] = useState('')
  const [loading, setLoading] = useState(false)
  const [mappingSkuId, setMappingSkuId] = useState('')
  const [mappingReason, setMappingReason] = useState('')
  const [similarityScore, setSimilarityScore] = useState('')
  const [tagType, setTagType] = useState('')
  const [tagValue, setTagValue] = useState('')

  useEffect(() => {
    fetchLibrary()
  }, [])

  const fetchLibrary = async () => {
    setLoading(true)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/enterprise/style-references?limit=50`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setImages(data.images || [])
    } catch (error) {
      console.error('Failed to fetch style references:', error)
    } finally {
      setLoading(false)
    }
  }

  const fetchImageTags = async (imageId) => {
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/enterprise/style-references/${imageId}/tags`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setImageTags(data.tags || [])
    } catch (error) {
      console.error('Failed to fetch tags:', error)
    }
  }

  const handleImageClick = (image) => {
    setSelectedImage(image)
    fetchImageTags(image.image_id)
    setTab('details')
  }

  const handleSearch = async () => {
    if (!searchQuery.trim()) return
    setLoading(true)
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/enterprise/style-references/search?tag_value=${encodeURIComponent(searchQuery)}&limit=50`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      const data = await response.json()
      if (response.ok) setImages(data.results || [])
    } catch (error) {
      console.error('Search failed:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleAddTag = async () => {
    if (!selectedImage || !tagType || !tagValue) return
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/enterprise/style-references/${selectedImage.image_id}/tags`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          factory_node_id: factoryNodeId || 'FACT-BLR-01',
          tag_type: tagType,
          tag_value: tagValue
        })
      })
      if (response.ok) {
        fetchImageTags(selectedImage.image_id)
        setTagType('')
        setTagValue('')
      }
    } catch (error) {
      console.error('Failed to add tag:', error)
    }
  }

  const handleMapSku = async () => {
    if (!selectedImage || !mappingSkuId) return
    try {
      const token = localStorage.getItem('access_token')
      const response = await fetch(`${API_URL}/enterprise/style-references/${selectedImage.image_id}/map-sku`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          sku_id: mappingSkuId,
          factory_node_id: factoryNodeId || 'FACT-BLR-01',
          similarity_score: similarityScore ? parseFloat(similarityScore) : null,
          match_reason: mappingReason
        })
      })
      const data = await response.json()
      if (response.ok) {
        alert('SKU mapped successfully')
        setMappingSkuId('')
        setMappingReason('')
        setSimilarityScore('')
      } else {
        alert(data.error?.message || 'Failed to map SKU')
      }
    } catch (error) {
      console.error('Failed to map SKU:', error)
    }
  }

  const tagTypeOptions = ['REGION', 'OCCASION', 'COLOR_FAMILY', 'PATTERN', 'FABRIC', 'ZARI_TYPE', 'DESIGN_CODE', 'CUSTOM']

  return (
    <Paper sx={{ mt: 3, p: 2 }}>
      <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        🎨 Style Reference Library
        <Chip label="NIFT Dataset" size="small" color="secondary" />
        <Chip label={`${images.length} images`} size="small" variant="outlined" />
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        Curated reference images from NIFT for design inspiration, SKU matching, and pattern similarity.
      </Typography>

      <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}>
        <Tabs value={tab} onChange={(e, v) => setTab(v)} variant="scrollable" scrollButtons="auto">
          <Tab label="Library" value="library" />
          <Tab label={`Details${selectedImage ? ' ✓' : ''}`} value="details" disabled={!selectedImage} />
          <Tab label="Search" value="search" />
        </Tabs>
      </Box>

      {tab === 'library' && (
        <Box>
          {loading ? (
            <Typography>Loading style references...</Typography>
          ) : images.length === 0 ? (
            <Alert severity="info">No style reference images indexed yet. Use the indexer script to populate from the NIFT dataset.</Alert>
          ) : (
            <Grid container spacing={2}>
              {images.map((img) => (
                <Grid item xs={12} sm={6} md={4} lg={3} key={img.id || img.image_id}>
                  <Card
                    sx={{ cursor: 'pointer', border: selectedImage?.image_id === img.image_id ? 2 : 0, borderColor: 'primary.main' }}
                    onClick={() => handleImageClick(img)}
                  >
                    <CardMedia
                      component="img"
                      height="180"
                      image={img.image_url}
                      alt={`Style reference ${img.image_id}`}
                      sx={{ objectFit: 'cover' }}
                    />
                    <CardContent sx={{ p: 1 }}>
                      <Typography variant="caption" display="block" noWrap>
                        {img.image_id}
                      </Typography>
                      <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', mt: 0.5 }}>
                        {img.ai_style_tag && (
                          <Chip label={img.ai_style_tag} size="small" color="secondary" />
                        )}
                        {img.dominant_colors?.length > 0 && (
                          <Box sx={{ display: 'flex', gap: 0.25 }}>
                            {img.dominant_colors.slice(0, 3).map((color, idx) => (
                              <Box
                                key={idx}
                                sx={{
                                  width: 16,
                                  height: 16,
                                  borderRadius: '50%',
                                  backgroundColor: color,
                                  border: '1px solid #ddd'
                                }}
                                title={color}
                              />
                            ))}
                          </Box>
                        )}
                      </Box>
                    </CardContent>
                  </Card>
                </Grid>
              ))}
            </Grid>
          )}
        </Box>
      )}

      {tab === 'details' && selectedImage && (
        <Box>
          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <Paper variant="outlined" sx={{ p: 2 }}>
                <img
                  src={selectedImage.image_url}
                  alt={selectedImage.image_id}
                  style={{ width: '100%', maxHeight: 400, objectFit: 'contain' }}
                />
              </Paper>
            </Grid>
            <Grid item xs={12} md={6}>
              <Typography variant="subtitle1" gutterBottom>
                {selectedImage.image_id}
              </Typography>
              <Typography variant="body2" color="text.secondary" gutterBottom>
                Source: {selectedImage.source_dataset} | Split: {selectedImage.hf_split} | Row: {selectedImage.hf_row_index}
              </Typography>
              <Typography variant="body2" color="text.secondary" gutterBottom>
                Dimensions: {selectedImage.image_width} × {selectedImage.image_height}
              </Typography>

              <Typography variant="subtitle2" sx={{ mt: 2, mb: 1 }}>
                AI Style Tag
              </Typography>
              {selectedImage.ai_style_tag ? (
                <Chip label={selectedImage.ai_style_tag} color="secondary" />
              ) : (
                <Typography variant="body2" color="text.secondary">No AI tag yet</Typography>
              )}

              <Typography variant="subtitle2" sx={{ mt: 2, mb: 1 }}>
                Tags
              </Typography>
              {imageTags.length === 0 ? (
                <Typography variant="body2" color="text.secondary">No tags yet</Typography>
              ) : (
                <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap', mb: 2 }}>
                  {imageTags.map((tag) => (
                    <Chip
                      key={tag.id || `${tag.tag_type}-${tag.tag_value}`}
                      label={`${tag.tag_type}: ${tag.tag_value}`}
                      size="small"
                      color={tag.ai_generated ? 'primary' : 'default'}
                    />
                  ))}
                </Box>
              )}

              <Typography variant="subtitle2" sx={{ mt: 2, mb: 1 }}>
                Add Tag
              </Typography>
              <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
                <TextField
                  select
                  size="small"
                  label="Tag Type"
                  value={tagType}
                  onChange={(e) => setTagType(e.target.value)}
                  sx={{ minWidth: 150 }}
                  SelectProps={{ native: true }}
                >
                  <option value="">Select type</option>
                  {tagTypeOptions.map((type) => (
                    <option key={type} value={type}>{type}</option>
                  ))}
                </TextField>
                <TextField
                  size="small"
                  label="Tag Value"
                  value={tagValue}
                  onChange={(e) => setTagValue(e.target.value)}
                  sx={{ flex: 1 }}
                />
                <Button variant="contained" size="small" onClick={handleAddTag} disabled={!tagType || !tagValue}>
                  <AddIcon />
                </Button>
              </Box>

              <Typography variant="subtitle2" sx={{ mt: 2, mb: 1 }}>
                Map to SKU
              </Typography>
              <Box sx={{ display: 'flex', gap: 1, mb: 1 }}>
                <TextField
                  select
                  size="small"
                  label="SKU"
                  value={mappingSkuId}
                  onChange={(e) => setMappingSkuId(e.target.value)}
                  sx={{ minWidth: 200 }}
                  SelectProps={{ native: true }}
                >
                  <option value="">Select SKU</option>
                  {skuOptions.map((sku) => (
                    <option key={sku.sku_id} value={sku.sku_id}>{sku.sku_id} - {sku.sku_name}</option>
                  ))}
                </TextField>
                <TextField
                  size="small"
                  label="Similarity"
                  type="number"
                  value={similarityScore}
                  onChange={(e) => setSimilarityScore(e.target.value)}
                  inputProps={{ min: 0, max: 1, step: 0.01 }}
                  sx={{ width: 100 }}
                />
              </Box>
              <TextField
                size="small"
                label="Match Reason"
                value={mappingReason}
                onChange={(e) => setMappingReason(e.target.value)}
                fullWidth
                sx={{ mb: 1 }}
              />
              <Button
                variant="outlined"
                size="small"
                startIcon={<LinkIcon />}
                onClick={handleMapSku}
                disabled={!mappingSkuId}
              >
                Map to SKU
              </Button>
            </Grid>
          </Grid>
        </Box>
      )}

      {tab === 'search' && (
        <Box>
          <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
            <TextField
              size="small"
              label="Search by tag value..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
              sx={{ flex: 1 }}
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
            <Button variant="contained" onClick={handleSearch}>Search</Button>
          </Box>
          {images.length === 0 ? (
            <Typography color="text.secondary">No results found.</Typography>
          ) : (
            <Grid container spacing={2}>
              {images.map((img) => (
                <Grid item xs={12} sm={6} md={4} lg={3} key={img.id || img.image_id}>
                  <Card sx={{ cursor: 'pointer' }} onClick={() => handleImageClick(img)}>
                    <CardMedia
                      component="img"
                      height="180"
                      image={img.image_url}
                      alt={`Style reference ${img.image_id}`}
                      sx={{ objectFit: 'cover' }}
                    />
                    <CardContent sx={{ p: 1 }}>
                      <Typography variant="caption" display="block" noWrap>
                        {img.image_id}
                      </Typography>
                      {img.tag_value && (
                        <Chip label={`${img.tag_type}: ${img.tag_value}`} size="small" />
                      )}
                    </CardContent>
                  </Card>
                </Grid>
              ))}
            </Grid>
          )}
        </Box>
      )}
    </Paper>
  )
}
