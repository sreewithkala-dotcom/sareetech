-- ============================================================
-- Migration: 028_qa_dyeing_inspector.sql
-- QA Dyeing Inspector module: post-dye inspection, color
-- consistency verification, fastness testing, defect
-- classification, certification, and B2B order linkage.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE ROLES: Expand permissions
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","quality:read","quality:write","dyeing:inspect","skein-dye:read","master-colorist:read","sales:forecast:read","b2b:order:read"]',
    description = 'QA Dyeing Inspector: post-dye color consistency verification, Delta-E measurement, color fastness testing, defect classification, dyeing certification, and B2B order linkage'
WHERE role_id = 'ROLE-QA-DYEING-INSPECTOR';

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","weaving:read","weaving:supervise","weaving:inspect","assistant:read","master-weaver:read","petni-master:read","warp-joiner:read","harness-setter:read","sales:forecast:read","environment:read","environment:write","oee:read","oee:write","quality:read","quality:write","dyeing:read","dyeing:write"]',
    description = 'SUP Loom Floor Supervisor (Weaving Shed Supervisor / Jacquard Floor Lead): oversees production, mechanical efficiency, quality compliance, and shift coordination across the weaving floor; monitors OEE, environmental controls, workflow handovers, first saree approval, root cause audits, batch release authorization, quality inspection clearance, and dyeing quality verification'
WHERE role_id = 'ROLE-SUP-LOOM-FLOOR-SUPERVISOR';

-- ------------------------------------------------------------
-- 2. QA DYEING INSPECTOR LOGS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS qa_dyeing_inspector_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    inspection_id VARCHAR(100) UNIQUE NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    saree_serial_barcode VARCHAR(100),
    factory_node_id VARCHAR(50) NOT NULL,
    inspector_employee_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    quality_inspector_log_id UUID REFERENCES quality_inspector_logs(id),
    sup_supervisor_log_id UUID REFERENCES sup_loom_floor_supervisor_logs(id),
    skein_dye_batch_id UUID,
    master_colorist_recipe_id UUID,
    inspection_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Category A: Dye Batch Identification & Color Reference
    dye_batch_ref VARCHAR(100),
    color_code VARCHAR(50),
    color_name VARCHAR(100),
    dyeing_process_type VARCHAR(50) DEFAULT 'SKEIN_DYE' CHECK (dyeing_process_type IN (
        'SKEIN_DYE',
        'HANK_DYE',
        'PIECE_DYE',
        'YARN_DYE'
    )),

    -- Category B: Color Consistency Verification
    color_delta_e DECIMAL(6,3),
    color_fastness_grade VARCHAR(50) DEFAULT 'GRADE_A_EXCELLENT' CHECK (color_fastness_grade IN (
        'GRADE_A_EXCELLENT',
        'GRADE_B_GOOD',
        'GRADE_C_ACCEPTABLE',
        'GRADE_D_POOR'
    )),
    shade_variation_detected BOOLEAN DEFAULT FALSE,
    dye_penetration_uniform BOOLEAN DEFAULT TRUE,
    metamerism_risk VARCHAR(50) DEFAULT 'LOW' CHECK (metamerism_risk IN (
        'LOW',
        'MEDIUM',
        'HIGH'
    )),

    -- Category C: Defect Classification & Batch Disposition
    primary_dye_defect_code VARCHAR(50) DEFAULT 'DEFECT_NONE_CLEAN_BATCH' CHECK (primary_dye_defect_code IN (
        'DEFECT_NONE_CLEAN_BATCH',
        'SHADE_VARIATION_LOT',
        'DYE_STREAK_MARK',
        'UNEVEN_PENETRATION',
        'COLOR_FASTNESS_FAIL',
        'METAMERISM_DETECTED'
    )),
    batch_clearance_status VARCHAR(50) DEFAULT 'CLEARED_FOR_FINISHING' CHECK (batch_clearance_status IN (
        'CLEARED_FOR_FINISHING',
        'HOLD_RE_DYE_REQUIRED',
        'REJECTED_SCRAP_BATCH'
    )),
    qa_dyeing_approval_state VARCHAR(50) DEFAULT 'INSPECTION_IN_PROGRESS' CHECK (qa_dyeing_approval_state IN (
        'INSPECTION_IN_PROGRESS',
        'PASSED_CLEARED_FOR_FINISHING',
        'HOLD_SECOND_AUDIT_REQUIRED',
        'REJECTED_RETURN_TO_DYEING'
    )),

    -- Piece-Rate Financial Penalty Trigger
    piece_rate_penalty_applied BOOLEAN DEFAULT FALSE,
    piece_rate_penalty_percent DECIMAL(5,2) DEFAULT 0.0,
    piece_rate_release_status VARCHAR(50) DEFAULT 'PENDING_RELEASE' CHECK (piece_rate_release_status IN (
        'PENDING_RELEASE',
        'RELEASED_FULL',
        'RELEASED_PARTIAL',
        'FROZEN_PENDING_RE_DYE'
    )),

    -- B2B Order Matcher
    b2b_order_matched BOOLEAN DEFAULT FALSE,
    b2b_order_status VARCHAR(50) DEFAULT 'PENDING_MATCH' CHECK (b2b_order_status IN (
        'PENDING_MATCH',
        'MATCHED_READY_FOR_PACKING',
        'SHIPPED',
        'HOLD_QA_REVIEW'
    )),

    -- Validation metadata
    validation_errors JSONB DEFAULT '[]'::jsonb,
    validation_warnings JSONB DEFAULT '[]'::jsonb,
    auto_assigned_routing VARCHAR(50),
    certificate_hash VARCHAR(255) UNIQUE,
    qr_tag_id VARCHAR(100) UNIQUE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_qa_dyeing_inspector_logs_batch ON qa_dyeing_inspector_logs(batch_id);
CREATE INDEX IF NOT EXISTS idx_qa_dyeing_inspector_logs_inspector ON qa_dyeing_inspector_logs(inspector_employee_id);
CREATE INDEX IF NOT EXISTS idx_qa_dyeing_inspector_logs_factory ON qa_dyeing_inspector_logs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_qa_dyeing_inspector_logs_quality ON qa_dyeing_inspector_logs(quality_inspector_log_id);
CREATE INDEX IF NOT EXISTS idx_qa_dyeing_inspector_logs_approval ON qa_dyeing_inspector_logs(qa_dyeing_approval_state);

-- ------------------------------------------------------------
-- 3. QA DYEING INSPECTOR CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS qa_dyeing_inspector_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    qa_dyeing_inspector_log_id UUID NOT NULL REFERENCES qa_dyeing_inspector_logs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    inspection_id VARCHAR(100) NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    saree_serial_barcode VARCHAR(100),
    color_code VARCHAR(50),
    color_name VARCHAR(100),
    dyeing_process_type VARCHAR(50),
    color_delta_e DECIMAL(6,3),
    color_fastness_grade VARCHAR(50),
    shade_variation_detected BOOLEAN,
    dye_penetration_uniform BOOLEAN,
    metamerism_risk VARCHAR(50),
    primary_dye_defect_code VARCHAR(50),
    batch_clearance_status VARCHAR(50),
    qa_dyeing_approval_state VARCHAR(50),
    piece_rate_penalty_applied BOOLEAN,
    piece_rate_penalty_percent DECIMAL(5,2),
    piece_rate_release_status VARCHAR(50),
    b2b_order_matched BOOLEAN,
    b2b_order_status VARCHAR(50),
    inspector_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    certification_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'REVOKED', 'EXPIRED', 'PENDING'
    )),
    certified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_qa_dyeing_inspector_certificates_log ON qa_dyeing_inspector_certificates(qa_dyeing_inspector_log_id);
CREATE INDEX IF NOT EXISTS idx_qa_dyeing_inspector_certificates_hash ON qa_dyeing_inspector_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_qa_dyeing_inspector_certificates_qr ON qa_dyeing_inspector_certificates(qr_tag_id);
CREATE INDEX IF NOT EXISTS idx_qa_dyeing_inspector_certificates_batch ON qa_dyeing_inspector_certificates(batch_id);

-- ------------------------------------------------------------
-- 4. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_qa_dyeing_inspector_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_qa_dyeing_inspector_plan_factory ON sales_forecast_qa_dyeing_inspector_plan(factory_node_id);

-- ------------------------------------------------------------
-- 5. TRIGGERS: Validation and automation
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_qa_dyeing_inspector_log()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1 (Color Consistency Guardrail)
    -- IF color_delta_e > 1.0
    -- → BLOCK: COLOR_VARIANCE_FAIL
    IF NEW.color_delta_e IS NOT NULL AND NEW.color_delta_e > 1.0 THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'COLOR_VARIANCE_FAIL',
                'message', 'COLOR_DELTA_E_EXCEEDS_ACCEPTABLE_THRESHOLD_OF_1.0',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Rule 2 (Color Fastness Gate)
    -- IF color_fastness_grade IN (GRADE_C_ACCEPTABLE, GRADE_D_POOR)
    -- → BLOCK: FASTNESS_FAIL
    IF NEW.color_fastness_grade IN ('GRADE_C_ACCEPTABLE', 'GRADE_D_POOR') THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'FASTNESS_FAIL',
                'message', 'COLOR_FASTNESS_GRADE_BELOW_ACCEPTABLE_THRESHOLD',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Rule 3 (Dye Defect Escalation)
    -- IF primary_dye_defect_code IN (SHADE_VARIATION_LOT, DYE_STREAK_MARK, UNEVEN_PENETRATION)
    -- → TRIGGER: AUTO_ALERT_DYEING_SUPERVISOR
    IF NEW.primary_dye_defect_code IN ('SHADE_VARIATION_LOT', 'DYE_STREAK_MARK', 'UNEVEN_PENETRATION') THEN
        NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'AUTO_ALERT_DYEING_SUPERVISOR',
                'message', 'DYE_DEFECT_DETECTED_REQUIRES_SUPERVISOR_REVIEW',
                'severity', 'WARNING'
            ));
    END IF;

    -- Rule 4 (Batch Clearance Authorization Gate)
    -- IF qa_dyeing_approval_state NOT EQUAL TO PASSED_CLEARED_FOR_FINISHING
    -- → BLOCK: DENY_FINISHING_TRANSFER
    IF NEW.qa_dyeing_approval_state IS NOT NULL AND NEW.qa_dyeing_approval_state != 'PASSED_CLEARED_FOR_FINISHING' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_FINISHING_TRANSFER',
                'message', 'DYEING_INSPECTION_NOT_CLEARED',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Piece-Rate Financial Penalty Trigger
    IF NEW.primary_dye_defect_code = 'DEFECT_NONE_CLEAN_BATCH' AND NEW.qa_dyeing_approval_state = 'PASSED_CLEARED_FOR_FINISHING' THEN
        NEW.piece_rate_release_status := 'RELEASED_FULL';
        NEW.piece_rate_penalty_applied := FALSE;
        NEW.piece_rate_penalty_percent := 0.0;
    ELSIF NEW.primary_dye_defect_code IN ('SHADE_VARIATION_LOT', 'DYE_STREAK_MARK') THEN
        NEW.piece_rate_release_status := 'RELEASED_PARTIAL';
        NEW.piece_rate_penalty_applied := TRUE;
        NEW.piece_rate_penalty_percent := 15.0;
    ELSIF NEW.primary_dye_defect_code IN ('UNEVEN_PENETRATION', 'COLOR_FASTNESS_FAIL', 'METAMERISM_DETECTED') THEN
        NEW.piece_rate_release_status := 'FROZEN_PENDING_RE_DYE';
        NEW.piece_rate_penalty_applied := TRUE;
        NEW.piece_rate_penalty_percent := 100.0;
    END IF;

    -- B2B Order Matcher
    IF NEW.qa_dyeing_approval_state = 'PASSED_CLEARED_FOR_FINISHING' AND NEW.primary_dye_defect_code = 'DEFECT_NONE_CLEAN_BATCH' THEN
        NEW.b2b_order_matched := TRUE;
        NEW.b2b_order_status := 'MATCHED_READY_FOR_PACKING';
    END IF;

    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'DYEING_QC_HOLD';
        ELSIF NEW.qa_dyeing_approval_state = 'PASSED_CLEARED_FOR_FINISHING' THEN
            NEW.auto_assigned_routing := 'READY_FOR_FINISHING';
        ELSE
            NEW.auto_assigned_routing := 'INSPECTION_IN_PROGRESS';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_qa_dyeing_inspector_log ON qa_dyeing_inspector_logs;

CREATE TRIGGER trigger_validate_qa_dyeing_inspector_log
    BEFORE INSERT OR UPDATE ON qa_dyeing_inspector_logs
    FOR EACH ROW EXECUTE FUNCTION validate_qa_dyeing_inspector_log();

-- ------------------------------------------------------------
-- 6. TRIGGER: Auto-generate certificate on clearance
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION generate_qa_dyeing_inspector_certificate()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.qa_dyeing_approval_state = 'PASSED_CLEARED_FOR_FINISHING'
       AND OLD.qa_dyeing_approval_state IS DISTINCT FROM NEW.qa_dyeing_approval_state THEN
        NEW.certificate_hash := generate_certificate_hash(NEW.id, NEW.inspection_id);
        NEW.qr_tag_id := 'QDI-' || NEW.inspection_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_generate_qa_dyeing_inspector_certificate ON qa_dyeing_inspector_logs;

CREATE TRIGGER trigger_generate_qa_dyeing_inspector_certificate
    BEFORE UPDATE OF qa_dyeing_approval_state ON qa_dyeing_inspector_logs
    FOR EACH ROW EXECUTE FUNCTION generate_qa_dyeing_inspector_certificate();

-- ------------------------------------------------------------
-- 7. TRIGGER: Update timestamps
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_qa_dyeing_inspector_logs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_qa_dyeing_inspector_logs_updated_at ON qa_dyeing_inspector_logs;

CREATE TRIGGER trigger_update_qa_dyeing_inspector_logs_updated_at
    BEFORE UPDATE ON qa_dyeing_inspector_logs
    FOR EACH ROW EXECUTE FUNCTION update_qa_dyeing_inspector_logs_updated_at();
