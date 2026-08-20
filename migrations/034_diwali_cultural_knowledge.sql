-- ============================================================
-- Migration: 034_diwali_cultural_knowledge.sql
-- Dataset: nlip/DIWALI
--
-- Adds culturally grounded Indian cultural concepts:
-- - diwali_cultural_facets
-- - diwali_cultural_states
-- - diwali_cultural_concepts
-- - diwali_concept_design_mappings
-- - diwali_concept_sku_mappings
-- - diwali_concept_translation_cache
-- ============================================================

-- ------------------------------------------------------------
-- 1. CULTURAL FACETS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS diwali_cultural_facets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    facet_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    facet_name VARCHAR(200) NOT NULL,
    facet_description TEXT,
    sort_order INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_diwali_cultural_facets_factory ON diwali_cultural_facets(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_diwali_cultural_facets_name ON diwali_cultural_facets(facet_name);

-- ------------------------------------------------------------
-- 2. CULTURAL STATES (sub-regions)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS diwali_cultural_states (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    state_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    state_name VARCHAR(200) NOT NULL,
    region VARCHAR(200),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    primary_language VARCHAR(50),
    primary_dialect VARCHAR(50),
    festival_calendar JSONB DEFAULT '[]'::jsonb,
    dominant_silk_type VARCHAR(50),
    dominant_zari_type VARCHAR(50),
    typical_color_palette JSONB DEFAULT '[]'::jsonb,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_diwali_cultural_states_factory ON diwali_cultural_states(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_diwali_cultural_states_name ON diwali_cultural_states(state_name);
CREATE INDEX IF NOT EXISTS idx_diwali_cultural_states_region ON diwali_cultural_states(region);

-- ------------------------------------------------------------
-- 3. CULTURAL CONCEPTS (core DIWALI dataset)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS diwali_cultural_concepts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    concept_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    facet_id UUID REFERENCES diwali_cultural_facets(id),
    state_id UUID REFERENCES diwali_cultural_states(id),
    concept VARCHAR(500) NOT NULL,
    description TEXT,
    source VARCHAR(500),
    hf_row_index INTEGER,
    ai_localized_concept TEXT,
    ai_localized_description TEXT,
    ai_confidence_score DECIMAL(5,4),
    language_code VARCHAR(10) DEFAULT 'en' CHECK (language_code IN ('en', 'te', 'ta', 'kn', 'hi', 'bn', 'mr', 'gu', 'pa', 'ml')),
    dialect_code VARCHAR(20),
    usage_context JSONB DEFAULT '[]'::jsonb,
    related_concepts JSONB DEFAULT '[]'::jsonb,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_diwali_cultural_concepts_factory ON diwali_cultural_concepts(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_diwali_cultural_concepts_facet ON diwali_cultural_concepts(facet_id);
CREATE INDEX IF NOT EXISTS idx_diwali_cultural_concepts_state ON diwali_cultural_concepts(state_id);
CREATE INDEX IF NOT EXISTS idx_diwali_cultural_concepts_concept ON diwali_cultural_concepts(concept);
CREATE INDEX IF NOT EXISTS idx_diwali_cultural_concepts_language ON diwali_cultural_concepts(language_code);
CREATE INDEX IF NOT EXISTS idx_diwali_cultural_concepts_hf_row ON diwali_cultural_concepts(hf_row_index);

-- ------------------------------------------------------------
-- 4. CONCEPT -> DESIGN MAPPINGS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS diwali_concept_design_mappings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    concept_id UUID REFERENCES diwali_cultural_concepts(id) ON DELETE CASCADE,
    design_id UUID REFERENCES design_files(id),
    factory_node_id VARCHAR(50) NOT NULL,
    relevance_score DECIMAL(5,4),
    mapping_type VARCHAR(50) DEFAULT 'THEMATIC' CHECK (mapping_type IN ('THEMATIC', 'COLOR', 'MOTIF', 'REGIONAL', 'FESTIVAL')),
    mapped_by UUID REFERENCES users(id),
    mapped_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb,
    UNIQUE(concept_id, design_id)
);
CREATE INDEX IF NOT EXISTS idx_diwali_concept_design_mappings_concept ON diwali_concept_design_mappings(concept_id);
CREATE INDEX IF NOT EXISTS idx_diwali_concept_design_mappings_design ON diwali_concept_design_mappings(design_id);

-- ------------------------------------------------------------
-- 5. CONCEPT -> SKU MAPPINGS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS diwali_concept_sku_mappings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    concept_id UUID REFERENCES diwali_cultural_concepts(id) ON DELETE CASCADE,
    sku_id VARCHAR(100) REFERENCES sku_catalog(sku_id) ON DELETE CASCADE,
    factory_node_id VARCHAR(50) NOT NULL,
    relevance_score DECIMAL(5,4),
    mapping_type VARCHAR(50) DEFAULT 'THEMATIC' CHECK (mapping_type IN ('THEMATIC', 'COLOR', 'MOTIF', 'REGIONAL', 'FESTIVAL')),
    mapped_by UUID REFERENCES users(id),
    mapped_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb,
    UNIQUE(concept_id, sku_id)
);
CREATE INDEX IF NOT EXISTS idx_diwali_concept_sku_mappings_concept ON diwali_concept_sku_mappings(concept_id);
CREATE INDEX IF NOT EXISTS idx_diwali_concept_sku_mappings_sku ON diwali_concept_sku_mappings(sku_id);

-- ------------------------------------------------------------
-- 6. TRANSLATION CACHE (for TTS, UI localization)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS diwali_concept_translation_cache (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    concept_id UUID REFERENCES diwali_cultural_concepts(id) ON DELETE CASCADE,
    source_language_code VARCHAR(10) NOT NULL,
    target_language_code VARCHAR(10) NOT NULL,
    translated_concept TEXT NOT NULL,
    translated_description TEXT,
    translation_quality_score DECIMAL(5,2),
    ai_translated BOOLEAN DEFAULT TRUE,
    human_reviewed BOOLEAN DEFAULT FALSE,
    reviewed_by UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(concept_id, source_language_code, target_language_code)
);
CREATE INDEX IF NOT EXISTS idx_diwali_concept_translation_cache_concept ON diwali_concept_translation_cache(concept_id);
CREATE INDEX IF NOT EXISTS idx_diwali_concept_translation_cache_lang ON diwali_concept_translation_cache(target_language_code);

-- ------------------------------------------------------------
-- 7. EXTEND style reference images with cultural linkage
-- ------------------------------------------------------------
ALTER TABLE saree_style_reference_images
ADD COLUMN IF NOT EXISTS diwali_facet_id UUID REFERENCES diwali_cultural_facets(id),
ADD COLUMN IF NOT EXISTS diwali_state_id UUID REFERENCES diwali_cultural_states(id),
ADD COLUMN IF NOT EXISTS diwali_concept_id UUID REFERENCES diwali_cultural_concepts(id),
ADD COLUMN IF NOT EXISTS cultural_relevance_score DECIMAL(5,4);

-- ------------------------------------------------------------
-- 8. TRIGGERS
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_diwali_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_diwali_cultural_facets_updated_at ON diwali_cultural_facets;
CREATE TRIGGER trigger_update_diwali_cultural_facets_updated_at
    BEFORE UPDATE ON diwali_cultural_facets
    FOR EACH ROW EXECUTE FUNCTION update_diwali_timestamp();

DROP TRIGGER IF EXISTS trigger_update_diwali_cultural_states_updated_at ON diwali_cultural_states;
CREATE TRIGGER trigger_update_diwali_cultural_states_updated_at
    BEFORE UPDATE ON diwali_cultural_states
    FOR EACH ROW EXECUTE FUNCTION update_diwali_timestamp();

DROP TRIGGER IF EXISTS trigger_update_diwali_cultural_concepts_updated_at ON diwali_cultural_concepts;
CREATE TRIGGER trigger_update_diwali_cultural_concepts_updated_at
    BEFORE UPDATE ON diwali_cultural_concepts
    FOR EACH ROW EXECUTE FUNCTION update_diwali_timestamp();

DROP TRIGGER IF EXISTS trigger_update_diwali_concept_translation_cache_updated_at ON diwali_concept_translation_cache;
CREATE TRIGGER trigger_update_diwali_concept_translation_cache_updated_at
    BEFORE UPDATE ON diwali_concept_translation_cache
    FOR EACH ROW EXECUTE FUNCTION update_diwali_timestamp();

-- ------------------------------------------------------------
-- 9. PERMISSIONS
-- ------------------------------------------------------------
UPDATE roles
SET permitted_operations = permitted_operations || '["culture:read","culture:write","culture:search","culture:translate"]'
WHERE role_id IN (
    'ROLE-LOCALIZATION-MANAGER',
    'ROLE-SUP-LOOM-FLOOR-SUPERVISOR',
    'ROLE-MASTER-WEAVER',
    'ROLE-SKU-MANAGER',
    'ROLE-GRAPH-DRAFTER',
    'ROLE-DESIGN-GENERATOR',
    'ROLE-SYSTEM-ADMIN'
);
