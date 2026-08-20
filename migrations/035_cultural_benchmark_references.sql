-- ============================================================
-- Migration: 035_cultural_benchmark_references.sql
-- Cultural alignment / bias benchmark papers and datasets
-- recommended alongside DIWALI.
--
-- Tables:
-- - cultural_benchmark_sources
-- - diwali_concept_benchmark_links
-- - llm_cultural_evaluation_logs
-- ============================================================

-- ------------------------------------------------------------
-- 1. BENCHMARK SOURCES
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cultural_benchmark_sources (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    title VARCHAR(500) NOT NULL,
    authors JSONB DEFAULT '[]'::jsonb,
    year INTEGER,
    venue VARCHAR(200),
    url VARCHAR(500),
    doi VARCHAR(200),
    abstract TEXT,
    benchmark_type VARCHAR(100) CHECK (benchmark_type IN ('DATASET', 'PAPER', 'FRAMEWORK', 'MODEL', 'EVALUATION')),
    culture_scope VARCHAR(200),
    language_scope VARCHAR(200),
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_cultural_benchmark_sources_factory ON cultural_benchmark_sources(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_cultural_benchmark_sources_type ON cultural_benchmark_sources(benchmark_type);
CREATE INDEX IF NOT EXISTS idx_cultural_benchmark_sources_year ON cultural_benchmark_sources(year);

-- ------------------------------------------------------------
-- 2. DIWALI CONCEPT <-> BENCHMARK LINKS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS diwali_concept_benchmark_links (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    concept_id UUID REFERENCES diwali_cultural_concepts(id) ON DELETE CASCADE,
    benchmark_source_id UUID REFERENCES cultural_benchmark_sources(id) ON DELETE CASCADE,
    factory_node_id VARCHAR(50) NOT NULL,
    link_type VARCHAR(50) DEFAULT 'RELATED' CHECK (link_type IN ('RELATED', 'EVALUATED_ON', 'CITED_IN', 'EXTENDS', 'CONTAINS')),
    relevance_score DECIMAL(5,4),
    notes TEXT,
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(concept_id, benchmark_source_id)
);
CREATE INDEX IF NOT EXISTS idx_diwali_concept_benchmark_links_concept ON diwali_concept_benchmark_links(concept_id);
CREATE INDEX IF NOT EXISTS idx_diwali_concept_benchmark_links_benchmark ON diwali_concept_benchmark_links(benchmark_source_id);

-- ------------------------------------------------------------
-- 3. LLM CULTURAL EVALUATION LOGS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS llm_cultural_evaluation_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    evaluation_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    llm_model VARCHAR(100) NOT NULL,
    benchmark_source_id UUID REFERENCES cultural_benchmark_sources(id),
    evaluation_type VARCHAR(100) NOT NULL CHECK (evaluation_type IN ('CULTURAL_ADAPTATION', 'BIAS_DETECTION', 'ALIGNMENT', 'KNOWLEDGE_PROBE', 'SAFETY')),
    input_text TEXT,
    output_text TEXT,
    csi_coverage_score DECIMAL(5,4),
    cultural_alignment_score DECIMAL(5,4),
    bias_score DECIMAL(5,4),
    llm_judge_score DECIMAL(5,4),
    human_evaluation_score DECIMAL(5,4),
    evaluated_by UUID REFERENCES users(id),
    evaluated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_llm_cultural_evaluation_logs_factory ON llm_cultural_evaluation_logs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_llm_cultural_evaluation_logs_model ON llm_cultural_evaluation_logs(llm_model);
CREATE INDEX IF NOT EXISTS idx_llm_cultural_evaluation_logs_type ON llm_cultural_evaluation_logs(evaluation_type);
CREATE INDEX IF NOT EXISTS idx_llm_cultural_evaluation_logs_benchmark ON llm_cultural_evaluation_logs(benchmark_source_id);

-- ------------------------------------------------------------
-- 4. TRIGGERS
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_cultural_benchmark_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_cultural_benchmark_sources_updated_at ON cultural_benchmark_sources;
CREATE TRIGGER trigger_update_cultural_benchmark_sources_updated_at
    BEFORE UPDATE ON cultural_benchmark_sources
    FOR EACH ROW EXECUTE FUNCTION update_cultural_benchmark_timestamp();

-- ------------------------------------------------------------
-- 5. PERMISSIONS
-- ------------------------------------------------------------
UPDATE roles
SET permitted_operations = permitted_operations || '["culture:benchmark:read","culture:benchmark:write","culture:evaluation:read","culture:evaluation:write"]'
WHERE role_id IN (
    'ROLE-LOCALIZATION-MANAGER',
    'ROLE-SUP-LOOM-FLOOR-SUPERVISOR',
    'ROLE-MASTER-WEAVER',
    'ROLE-SKU-MANAGER',
    'ROLE-GRAPH-DRAFTER',
    'ROLE-DESIGN-GENERATOR',
    'ROLE-SYSTEM-ADMIN'
);
