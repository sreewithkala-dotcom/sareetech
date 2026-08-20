-- ============================================================
-- Migration: 033_saree_style_reference_library.sql
-- Integration for HuggingFace dataset:
-- shrimantasatpati/Saree-NIFT-Style
--
-- Adds:
-- - saree_style_reference_images
-- - saree_style_reference_tags
-- - Extends design_files / sku_catalog with reference linkage
-- ============================================================

-- ------------------------------------------------------------
-- 1. STYLE REFERENCE IMAGES
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS saree_style_reference_images (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    image_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    source_dataset VARCHAR(200) NOT NULL DEFAULT 'shrimantasatpati/Saree-NIFT-Style',
    hf_split VARCHAR(50) DEFAULT 'train',
    hf_row_index INTEGER,
    image_url VARCHAR(500) NOT NULL,
    image_width INTEGER,
    image_height INTEGER,
    storage_path VARCHAR(500),
    mime_type VARCHAR(50) DEFAULT 'image/png',
    file_size_bytes BIGINT,
    dominant_colors JSONB DEFAULT '[]'::jsonb,
    pattern_hash VARCHAR(100),
    ai_embedding_vector JSONB DEFAULT '[]'::jsonb,
    ai_style_tag VARCHAR(100),
    ai_confidence_score DECIMAL(5,4),
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_saree_style_reference_images_factory ON saree_style_reference_images(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_saree_style_reference_images_source ON saree_style_reference_images(source_dataset);
CREATE INDEX IF NOT EXISTS idx_saree_style_reference_images_style_tag ON saree_style_reference_images(ai_style_tag);

-- ------------------------------------------------------------
-- 2. STYLE REFERENCE TAGS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS saree_style_reference_tags (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    image_id UUID REFERENCES saree_style_reference_images(id) ON DELETE CASCADE,
    factory_node_id VARCHAR(50) NOT NULL,
    tag_type VARCHAR(50) NOT NULL CHECK (tag_type IN ('REGION', 'OCCASION', 'COLOR_FAMILY', 'PATTERN', 'FABRIC', 'ZARI_TYPE', 'DESIGN_CODE', 'CUSTOM')),
    tag_value VARCHAR(200) NOT NULL,
    confidence_score DECIMAL(5,4),
    ai_generated BOOLEAN DEFAULT TRUE,
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_saree_style_reference_tags_image ON saree_style_reference_tags(image_id);
CREATE INDEX IF NOT EXISTS idx_saree_style_reference_tags_type ON saree_style_reference_tags(tag_type);
CREATE INDEX IF NOT EXISTS idx_saree_style_reference_tags_value ON saree_style_reference_tags(tag_value);

-- ------------------------------------------------------------
-- 3. STYLE REFERENCE -> SKU MAPPING
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS saree_style_reference_sku_mapping (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    image_id UUID REFERENCES saree_style_reference_images(id) ON DELETE CASCADE,
    sku_id VARCHAR(100) REFERENCES sku_catalog(sku_id) ON DELETE CASCADE,
    factory_node_id VARCHAR(50) NOT NULL,
    similarity_score DECIMAL(5,4),
    match_reason VARCHAR(500),
    mapped_by UUID REFERENCES users(id),
    mapped_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb,
    UNIQUE(image_id, sku_id)
);
CREATE INDEX IF NOT EXISTS idx_saree_style_reference_sku_mapping_image ON saree_style_reference_sku_mapping(image_id);
CREATE INDEX IF NOT EXISTS idx_saree_style_reference_sku_mapping_sku ON saree_style_reference_sku_mapping(sku_id);

-- ------------------------------------------------------------
-- 4. EXTEND design_files with reference linkage
-- ------------------------------------------------------------
ALTER TABLE design_files
ADD COLUMN IF NOT EXISTS reference_image_id UUID REFERENCES saree_style_reference_images(id),
ADD COLUMN IF NOT EXISTS ai_style_similarity_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_pattern_match_flag BOOLEAN DEFAULT FALSE;

-- ------------------------------------------------------------
-- 5. EXTEND sku_catalog with style metadata
-- ------------------------------------------------------------
ALTER TABLE sku_catalog
ADD COLUMN IF NOT EXISTS style_reference_image_id UUID REFERENCES saree_style_reference_images(id),
ADD COLUMN IF NOT EXISTS dominant_color_palette JSONB DEFAULT '[]'::jsonb,
ADD COLUMN IF NOT EXISTS nift_style_tag VARCHAR(100),
ADD COLUMN IF NOT EXISTS ai_style_match_score DECIMAL(5,4);

-- ------------------------------------------------------------
-- 6. TRIGGERS
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_saree_style_reference_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_saree_style_reference_images_updated_at ON saree_style_reference_images;
CREATE TRIGGER trigger_update_saree_style_reference_images_updated_at
    BEFORE UPDATE ON saree_style_reference_images
    FOR EACH ROW EXECUTE FUNCTION update_saree_style_reference_timestamp();

-- ------------------------------------------------------------
-- 7. PERMISSIONS
-- ------------------------------------------------------------
UPDATE roles
SET permitted_operations = permitted_operations || '["style-reference:read","style-reference:write","style-reference:tag"]'
WHERE role_id IN (
    'ROLE-GRAPH-DRAFTER',
    'ROLE-DESIGN-GENERATOR',
    'ROLE-SKU-MANAGER',
    'ROLE-SUP-LOOM-FLOOR-SUPERVISOR',
    'ROLE-MASTER-WEAVER',
    'ROLE-SYSTEM-ADMIN'
);
