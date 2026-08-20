-- ============================================================
-- Migration: 019_graph_drafter.sql
-- Graph Drafter (2400 Hook) module
-- Jacquard graph design, hook mapping, weave structure assignment,
-- color separation, and card-punch coding.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE GRAPH-DRAFTER ROLE
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","design:write","design:approve","design:export","design:certificate:read","sales:forecast:read"]',
    description = 'Graph Drafter (2400 Hook): pixel-by-pixel grid translation, hook allocation, weave structure assignment, color separation/card-punch coding, technical feasibility auditing, CAD export'
WHERE role_id = 'ROLE-GRAPH-DRAFTER';

-- ------------------------------------------------------------
-- 2. DESIGN MASTER / GRAPH DRAFTS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS design_masters (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    design_master_id VARCHAR(100) UNIQUE NOT NULL,
    graph_iteration_v DECIMAL(3,1) DEFAULT 1.0,
    drafter_employee_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    status VARCHAR(50) DEFAULT 'DRAFT' CHECK (status IN (
        'DRAFT', 'PENDING_FLOAT_CHECK', 'APPROVED_FOR_PUNCHING', 'REJECTED_FLOAT_EXCEEDED', 'ARCHIVED'
    )),
    
    -- Jacquard Architecture & Hook Mapping
    total_hook_capacity INTEGER DEFAULT 2400 CHECK (total_hook_capacity IN (600, 1200, 1536, 2400)),
    top_border_hooks INTEGER DEFAULT 0 CHECK (top_border_hooks >= 0),
    bottom_border_hooks INTEGER DEFAULT 0 CHECK (bottom_border_hooks >= 0),
    body_motif_hooks INTEGER DEFAULT 0 CHECK (body_motif_hooks >= 0),
    selvedge_hooks INTEGER DEFAULT 0 CHECK (selvedge_hooks >= 0),
    total_allocated_hooks INTEGER GENERATED ALWAYS AS (
        COALESCE(top_border_hooks, 0) + COALESCE(bottom_border_hooks, 0) + 
        COALESCE(body_motif_hooks, 0) + COALESCE(selvedge_hooks, 0)
    ) STORED,
    
    -- Technical Grid File Specifications
    grid_width_pixels INTEGER DEFAULT 2400 CHECK (grid_width_pixels > 0),
    grid_height_picks INTEGER DEFAULT 0 CHECK (grid_height_picks >= 0),
    maximum_float_length INTEGER DEFAULT 7 CHECK (maximum_float_length IN (3, 5, 7)),
    digital_cad_file_upload TEXT,
    
    -- Category A: Design Metadata & Resolution Control
    target_hook_capacity INTEGER DEFAULT 2400 CHECK (target_hook_capacity IN (600, 1200, 1536, 2400)),
    hook_allocation_profile VARCHAR(100) DEFAULT '2250_DESIGN_150_BORDER_SELVAGE' CHECK (hook_allocation_profile IN (
        '2250_DESIGN_150_BORDER_SELVAGE',
        '2304_DESIGN_96_BORDER_SELVAGE',
        '2160_DESIGN_240_DOUBLE_BORDER',
        'CUSTOM_ALLOCATION'
    )),
    warp_ends_per_inch_epi INTEGER CHECK (warp_ends_per_inch_epi > 0),
    weft_picks_per_inch_ppi INTEGER CHECK (weft_picks_per_inch_ppi > 0),
    graph_aspect_ratio DECIMAL(5,3) GENERATED ALWAYS AS (
        CASE 
            WHEN weft_picks_per_inch_ppi > 0 
            THEN ROUND(CAST(warp_ends_per_inch_epi AS DECIMAL) / CAST(weft_picks_per_inch_ppi AS DECIMAL), 3)
            ELSE NULL 
        END
    ) STORED,
    
    -- Category B: Weave Rules & Structural Selection
    ground_weave_structure VARCHAR(100) DEFAULT '16_END_SHADED_SATIN' CHECK (ground_weave_structure IN (
        '16_END_SHADED_SATIN',
        '8_END_WARP_SATIN',
        '5_END_WEFT_TWILL',
        '1_1_PLAIN_WEAVE',
        'DAMASK_INTERLOCK'
    )),
    zari_binding_weave_type VARCHAR(100) DEFAULT '8_END_SATIN_INTERLOCK' CHECK (zari_binding_weave_type IN (
        '8_END_SATIN_INTERLOCK',
        '5_END_TWILL_CATCH',
        'PLAIN_WEAVE_GROUND',
        '3_1_BROKEN_TWILL'
    )),
    border_binding_technique VARCHAR(100) DEFAULT 'MICRO_STEP_CATCHING' CHECK (border_binding_technique IN (
        'MICRO_STEP_CATCHING',
        'STANDARD_1_PIXEL_CATCH',
        'INTERLOCKING_SATIN_BORDER',
        'SAWTOOTH_EDGE_LOCK'
    )),
    shading_technique VARCHAR(100) DEFAULT 'MULTI_LEVEL_SHADED_SATIN' CHECK (shading_technique IN (
        'MULTI_LEVEL_SHADED_SATIN',
        'DITHERED_POINT_PAPER',
        'CROSS_HATCH_HATCHING',
        'SOLID_FILL'
    )),
    
    -- Category C: Audit Rules, System Output & Workflow Status
    max_float_enforcement_rule VARCHAR(100) DEFAULT 'STRICT_WARP_LE_4_WEFT_LE_5' CHECK (max_float_enforcement_rule IN (
        'STRICT_WARP_LE_4_WEFT_LE_5',
        'STANDARD_WARP_LE_7_WEFT_LE_8',
        'CUSTOM_LIMIT',
        'DISABLED_RAW_IMPORT'
    )),
    max_warp_float_ends INTEGER DEFAULT 4 CHECK (max_warp_float_ends >= 0),
    max_weft_float_picks INTEGER DEFAULT 5 CHECK (max_weft_float_picks >= 0),
    selvage_hook_count INTEGER DEFAULT 0 CHECK (selvage_hook_count >= 0),
    cad_output_format VARCHAR(20) DEFAULT 'JC5' CHECK (cad_output_format IN (
        'JC5', '.EP', '.DAT', '.BMP'
    )),
    design_approval_state VARCHAR(50) DEFAULT 'DRAFT' CHECK (design_approval_state IN (
        'DRAFT', 'PENDING_FLOAT_CHECK', 'APPROVED_FOR_PUNCHING', 'REJECTED_FLOAT_EXCEEDED', 'ARCHIVED'
    )),
    
    -- Automated Validation
    validation_errors JSONB DEFAULT '[]'::jsonb,
    validation_warnings JSONB DEFAULT '[]'::jsonb,
    auto_assigned_routing VARCHAR(50),
    
    -- Certification
    certificate_hash VARCHAR(255),
    qr_tag_id VARCHAR(100) UNIQUE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_design_masters_code ON design_masters(design_master_id);
CREATE INDEX IF NOT EXISTS idx_design_masters_factory ON design_masters(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_design_masters_status ON design_masters(status);
CREATE INDEX IF NOT EXISTS idx_design_masters_approval ON design_masters(design_approval_state);

-- ------------------------------------------------------------
-- 3. DESIGN ITERATIONS / VERSIONS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS design_iterations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    design_master_id UUID NOT NULL REFERENCES design_masters(id) ON DELETE CASCADE,
    iteration_v DECIMAL(3,1) NOT NULL,
    drafter_employee_id UUID REFERENCES users(id),
    change_reason TEXT,
    grid_width_pixels INTEGER,
    grid_height_picks INTEGER,
    hook_allocation_profile VARCHAR(100),
    ground_weave_structure VARCHAR(100),
    max_float_enforcement_rule VARCHAR(100),
    max_warp_float_ends INTEGER,
    max_weft_float_picks INTEGER,
    validation_errors JSONB DEFAULT '[]'::jsonb,
    validation_warnings JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_design_iterations_master ON design_iterations(design_master_id);

-- ------------------------------------------------------------
-- 4. DESIGN CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS design_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    design_master_id UUID NOT NULL REFERENCES design_masters(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    design_master_id_ref VARCHAR(100) NOT NULL,
    graph_iteration_v DECIMAL(3,1),
    total_hook_capacity INTEGER,
    hook_allocation_profile VARCHAR(100),
    grid_width_pixels INTEGER,
    grid_height_picks INTEGER,
    ground_weave_structure VARCHAR(100),
    zari_binding_weave_type VARCHAR(100),
    border_binding_technique VARCHAR(100),
    shading_technique VARCHAR(100),
    max_float_enforcement_rule VARCHAR(100),
    max_warp_float_ends INTEGER,
    max_weft_float_picks INTEGER,
    cad_output_format VARCHAR(20),
    auto_assigned_routing VARCHAR(50) NOT NULL,
    operator_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    certification_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'REVOKED', 'EXPIRED', 'PENDING'
    )),
    certified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_design_certificates_design ON design_certificates(design_master_id);
CREATE INDEX IF NOT EXISTS idx_design_certificates_hash ON design_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_design_certificates_qr ON design_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 5. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_design_plan (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    forecast_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    source_system VARCHAR(50) DEFAULT 'SALES_TEAM_API',
    forecast_period_days INTEGER DEFAULT 30,
    material_plan JSONB DEFAULT '[]'::jsonb,
    upcoming_lots JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sales_forecast_design_factory ON sales_forecast_design_plan(factory_node_id);

-- ------------------------------------------------------------
-- 6. TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_design_master()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1: 2400 Hook Float Limit Guardrail
    -- IF target_hook_capacity = 2400 AND (max_warp_float_ends > 4 OR max_float_enforcement_rule = DISABLED_RAW_IMPORT)
    -- → BLOCK: REJECT_DESIGN_FILE (EXCESSIVE_FLOAT_LENGTH_RISKS_WARP_SNAG)
    IF NEW.target_hook_capacity = 2400 THEN
        IF NEW.max_warp_float_ends > 4 OR NEW.max_float_enforcement_rule = 'DISABLED_RAW_IMPORT' THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'REJECT_DESIGN_FILE',
                    'message', 'Float length exceeds 4 ends for 2400 Hook. Excessive float length risks warp snag.',
                    'severity', 'BLOCK'
                ));
        END IF;
        
        -- Rule 3: Hook Allocation Exceeded
        -- IF (design_hooks_used + selvage_hook_count) > 2400
        -- → BLOCK: CAD_EXPORT_FAILED (EXCEEDS_2400_PHYSICAL_HOOK_LIMIT)
        IF NEW.total_allocated_hooks > 2400 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'CAD_EXPORT_FAILED',
                    'message', format('Total allocated hooks %s exceeds 2400 physical hook limit.', NEW.total_allocated_hooks),
                    'severity', 'BLOCK'
                ));
        END IF;
        
        -- 2400 Hook specific defaults
        IF NEW.max_warp_float_ends IS NULL THEN
            NEW.max_warp_float_ends := 4;
        END IF;
        IF NEW.max_weft_float_picks IS NULL THEN
            NEW.max_weft_float_picks := 5;
        END IF;
    END IF;
    
    -- Rule 2: Aspect Ratio Math Check
    -- IF graph_aspect_ratio ≠ (warp_ends_per_inch_epi / weft_picks_per_inch_ppi)
    -- → TRIGGER WARNING: MOTIF_DISTORTION_DETECTED
    IF NEW.graph_aspect_ratio IS NOT NULL AND NEW.warp_ends_per_inch_epi IS NOT NULL AND NEW.weft_picks_per_inch_ppi IS NOT NULL THEN
        DECLARE
            expected_ratio DECIMAL(5,3);
        BEGIN
            expected_ratio := ROUND(CAST(NEW.warp_ends_per_inch_epi AS DECIMAL) / CAST(NEW.weft_picks_per_inch_ppi AS DECIMAL), 3);
            IF ABS(NEW.graph_aspect_ratio - expected_ratio) > 0.01 THEN
                NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                    jsonb_build_array(jsonb_build_object(
                        'code', 'MOTIF_DISTORTION_DETECTED',
                        'message', format('Graph aspect ratio %s does not match EPI/PPI ratio %s. Check pixel scale.', NEW.graph_aspect_ratio, expected_ratio),
                        'severity', 'WARNING'
                    ));
            END IF;
        END;
    END IF;
    
    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'CAD_EXPORT_FAILED';
        ELSIF NEW.target_hook_capacity = 2400 
              AND NEW.max_warp_float_ends <= 4
              AND NEW.total_allocated_hooks <= 2400
              AND NEW.design_approval_state = 'APPROVED_FOR_PUNCHING' THEN
            NEW.auto_assigned_routing := 'JACQUARD_2400_READY';
        ELSIF NEW.target_hook_capacity = 1536 
              AND NEW.max_warp_float_ends <= 7
              AND NEW.total_allocated_hooks <= 1536 THEN
            NEW.auto_assigned_routing := 'JACQUARD_1536_READY';
        ELSE
            NEW.auto_assigned_routing := 'PENDING_REVIEW';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_design_master ON design_masters;

CREATE TRIGGER trigger_validate_design_master
    BEFORE INSERT OR UPDATE ON design_masters
    FOR EACH ROW EXECUTE FUNCTION validate_design_master();

-- ------------------------------------------------------------
-- 7. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_design_masters_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_design_masters_updated_at ON design_masters;

CREATE TRIGGER trigger_update_design_masters_updated_at
    BEFORE UPDATE ON design_masters
    FOR EACH ROW EXECUTE FUNCTION update_design_masters_updated_at();

-- ------------------------------------------------------------
-- 8. UPDATE PIRN-WINDERS ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:weaving","pirn:job:create","pirn:job:read","pirn:job:write","pirn:certificate:read","design:read","sales:forecast:read"]',
    description = 'Pirn Winder: precision taper control, density monitoring, knot formatting, shade/lot segregation, doffing/inspection, pirn certification; provides pre-process material to Graph Drafter/Weaving Master'
WHERE role_id = 'ROLE-PIRN-WINDERS';
