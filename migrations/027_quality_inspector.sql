-- ============================================================
-- Migration: 027_quality_inspector.sql
-- Quality Inspector (Fabric QA Specialist / On-Loom & Off-Loom Inspector)
-- module: finished saree inspection, dimensional auditing, defect
-- classification, commercial grading, piece-rate penalty trigger,
-- B2B order auto-matcher, and quality certification.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE ROLES: Expand permissions
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","weaving:read","weaving:inspect","supervisor:read","master-weaver:read","assistant:read","sales:forecast:read","b2b:order:read","b2b:order:write"]',
    description = 'Quality Inspector (Fabric QA Specialist / On-Loom & Off-Loom Inspector): 100% illuminated light-table inspection, dimensional compliance verification, Zari quality audit, defect classification, final commercial grading, piece-rate penalty trigger, B2B order auto-matcher, and quality certification; serves as final technical gatekeeper before saree release to B2B clients'
WHERE role_id = 'ROLE-QUALITY-INSPECTOR';

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","weaving:read","weaving:supervise","weaving:inspect","assistant:read","master-weaver:read","petni-master:read","warp-joiner:read","harness-setter:read","sales:forecast:read","environment:read","environment:write","oee:read","oee:write","quality:read","quality:write"]',
    description = 'SUP Loom Floor Supervisor (Weaving Shed Supervisor / Jacquard Floor Lead): oversees production, mechanical efficiency, quality compliance, and shift coordination across the weaving floor; monitors OEE, environmental controls, workflow handovers, first saree approval, root cause audits, batch release authorization, and quality inspection clearance'
WHERE role_id = 'ROLE-SUP-LOOM-FLOOR-SUPERVISOR';

-- ------------------------------------------------------------
-- 2. QUALITY INSPECTOR LOGS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS quality_inspector_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    inspection_id VARCHAR(100) UNIQUE NOT NULL,
    saree_serial_barcode VARCHAR(100) UNIQUE NOT NULL,
    loom_id_ref VARCHAR(100),
    factory_node_id VARCHAR(50) NOT NULL,
    inspector_employee_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    sup_supervisor_log_id UUID REFERENCES sup_loom_floor_supervisor_logs(id),
    assistant_weaver_job_log_id UUID REFERENCES assistant_weaver_job_logs(id),
    master_weaver_job_id UUID REFERENCES master_weaver_jobs(id),
    petni_master_job_id UUID REFERENCES petni_master_jobs(id),
    warp_joining_job_id UUID REFERENCES warp_joining_jobs(id),
    harness_setup_log_id UUID REFERENCES harness_setup_logs(id),
    inspection_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Category A: Inspection Setup & Material Metadata
    inspection_table_type VARCHAR(50) DEFAULT 'ILLUMINATED_LED_DUAL_SURFACE_TABLE' CHECK (inspection_table_type IN (
        'ILLUMINATED_LED_DUAL_SURFACE_TABLE',
        'STANDARD_OVERHEAD_LIGHT_TABLE',
        'MANUAL_FLAT_BENCH'
    )),
    fabric_weight_grams_per_sqm DECIMAL(6,2),
    pick_density_measured_ppi DECIMAL(6,2),
    warp_density_measured_epi DECIMAL(6,2),

    -- Category B: Defect Classification & Dimensional Audit
    primary_fabric_defect_code VARCHAR(50) DEFAULT 'DEFECT_NONE_CLEAN_PIECE' CHECK (primary_fabric_defect_code IN (
        'DEFECT_NONE_CLEAN_PIECE',
        'HOOK_MISLIFT_PATTERN_ERROR',
        'ZARI_FLOAT_TENSION_FAULT',
        'REED_MARK_GAP',
        'WEFT_PICK_BAR_DENSITY_VAR',
        'WARP_END_BREAK_MEND_MARK'
    )),
    pallu_length_measured_cm DECIMAL(6,2),
    total_saree_length_measured_meters DECIMAL(6,2),
    border_width_symmetry_offset_mm DECIMAL(6,2),
    zari_tarnish_visual_check VARCHAR(50) DEFAULT 'PASSED_FULL_LUSTER' CHECK (zari_tarnish_visual_check IN (
        'PASSED_FULL_LUSTER',
        'WARNING_MINOR_DISCOLORATION',
        'REJECTED_OXIDIZED_ZARI'
    )),

    -- Dimensional Metrics (Physical Measurements)
    actual_body_length_meters DECIMAL(6,2),
    actual_blouse_length_meters DECIMAL(6,2),
    actual_width_inches DECIMAL(6,2),
    total_finished_weight_grams INTEGER,

    -- Defect Checklist Toggles (Binary & Counts)
    warp_break_streaks_count INTEGER DEFAULT 0,
    weft_barriness_detected BOOLEAN DEFAULT FALSE,
    zari_tarnishing_present BOOLEAN DEFAULT FALSE,
    has_oil_grease_stains BOOLEAN DEFAULT FALSE,
    loose_zari_floats_count INTEGER DEFAULT 0,

    -- Category C: Quality Grading & Final Batch Disposition
    final_fabric_quality_grade VARCHAR(50) DEFAULT 'GRADE_A_EXPORT_PREMIUM' CHECK (final_fabric_quality_grade IN (
        'GRADE_A_EXPORT_PREMIUM',
        'GRADE_B_DOMESTIC_MINOR_DEFECT',
        'GRADE_C_RESERVE_DISCOUNT',
        'REJECTED_SCRAP'
    )),
    qa_inspector_approval_state VARCHAR(50) DEFAULT 'INSPECTION_IN_PROGRESS' CHECK (qa_inspector_approval_state IN (
        'INSPECTION_IN_PROGRESS',
        'PASSED_CLEARED_FOR_PACKING',
        'HOLD_SECOND_AUDIT_REQUIRED',
        'REJECTED_RETURN_TO_SUPERVISOR'
    )),

    -- Piece-Rate Financial Penalty Trigger
    piece_rate_penalty_applied BOOLEAN DEFAULT FALSE,
    piece_rate_penalty_percent DECIMAL(5,2) DEFAULT 0.0,
    piece_rate_release_status VARCHAR(50) DEFAULT 'PENDING_RELEASE' CHECK (piece_rate_release_status IN (
        'PENDING_RELEASE',
        'RELEASED_FULL',
        'RELEASED_PARTIAL',
        'FROZEN_PENDING_MENDING'
    )),

    -- B2B Order Matcher
    b2b_order_matched BOOLEAN DEFAULT FALSE,
    b2b_order_id UUID,
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

CREATE INDEX IF NOT EXISTS idx_quality_inspector_logs_serial ON quality_inspector_logs(saree_serial_barcode);
CREATE INDEX IF NOT EXISTS idx_quality_inspector_logs_inspector ON quality_inspector_logs(inspector_employee_id);
CREATE INDEX IF NOT EXISTS idx_quality_inspector_logs_factory ON quality_inspector_logs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_quality_inspector_logs_supervisor ON quality_inspector_logs(sup_supervisor_log_id);
CREATE INDEX IF NOT EXISTS idx_quality_inspector_logs_grade ON quality_inspector_logs(final_fabric_quality_grade);
CREATE INDEX IF NOT EXISTS idx_quality_inspector_logs_b2b ON quality_inspector_logs(b2b_order_status);

-- ------------------------------------------------------------
-- 3. QUALITY INSPECTOR CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS quality_inspector_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    quality_inspector_log_id UUID NOT NULL REFERENCES quality_inspector_logs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    inspection_id VARCHAR(100) NOT NULL,
    saree_serial_barcode VARCHAR(100) NOT NULL,
    loom_id_ref VARCHAR(100),
    sup_supervisor_log_id UUID REFERENCES sup_loom_floor_supervisor_logs(id),
    assistant_weaver_job_log_id UUID REFERENCES assistant_weaver_job_logs(id),
    master_weaver_job_id UUID REFERENCES master_weaver_jobs(id),
    actual_body_length_meters DECIMAL(6,2),
    actual_blouse_length_meters DECIMAL(6,2),
    actual_width_inches DECIMAL(6,2),
    total_finished_weight_grams INTEGER,
    warp_break_streaks_count INTEGER,
    weft_barriness_detected BOOLEAN,
    zari_tarnishing_present BOOLEAN,
    has_oil_grease_stains BOOLEAN,
    loose_zari_floats_count INTEGER,
    primary_fabric_defect_code VARCHAR(50),
    pallu_length_measured_cm DECIMAL(6,2),
    total_saree_length_measured_meters DECIMAL(6,2),
    border_width_symmetry_offset_mm DECIMAL(6,2),
    zari_tarnish_visual_check VARCHAR(50),
    final_fabric_quality_grade VARCHAR(50),
    qa_inspector_approval_state VARCHAR(50),
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

CREATE INDEX IF NOT EXISTS idx_quality_inspector_certificates_log ON quality_inspector_certificates(quality_inspector_log_id);
CREATE INDEX IF NOT EXISTS idx_quality_inspector_certificates_hash ON quality_inspector_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_quality_inspector_certificates_qr ON quality_inspector_certificates(qr_tag_id);
CREATE INDEX IF NOT EXISTS idx_quality_inspector_certificates_serial ON quality_inspector_certificates(saree_serial_barcode);

-- ------------------------------------------------------------
-- 4. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_quality_inspector_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_quality_inspector_plan_factory ON sales_forecast_quality_inspector_plan(factory_node_id);

-- ------------------------------------------------------------
-- 5. TRIGGERS: Validation and automation
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_quality_inspector_log()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1 (Zero Zari Looping Protection Guardrail)
    -- IF primary_fabric_defect_code = ZARI_FLOAT_TENSION_FAULT
    -- → BLOCK: GRADE_A_DISQUALIFICATION
    -- → SET final_fabric_quality_grade = GRADE_B_DOMESTIC_MINOR_DEFECT
    IF NEW.primary_fabric_defect_code = 'ZARI_FLOAT_TENSION_FAULT' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'GRADE_A_DISQUALIFICATION',
                'message', 'LOOSE_ZARI_FLOATS_CANNOT_BE_GRADED_PREMIUM',
                'severity', 'BLOCK'
            ));
        NEW.final_fabric_quality_grade := 'GRADE_B_DOMESTIC_MINOR_DEFECT';
    END IF;

    -- Rule 2 (Dimensional Compliance Gate)
    -- IF total_saree_length_measured_meters < 6.25 OR total_saree_length_measured_meters > 6.35
    -- → BLOCK: REJECT_OUT_OF_SPEC_LENGTH
    -- → SET qa_inspector_approval_state = HOLD_SECOND_AUDIT_REQUIRED
    IF NEW.total_saree_length_measured_meters IS NOT NULL THEN
        IF NEW.total_saree_length_measured_meters < 6.25 OR NEW.total_saree_length_measured_meters > 6.35 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'REJECT_OUT_OF_SPEC_LENGTH',
                    'message', 'MUST_BE_WITHIN_6.28m_TO_6.32m_RANGE',
                    'severity', 'BLOCK'
                ));
            NEW.qa_inspector_approval_state := 'HOLD_SECOND_AUDIT_REQUIRED';
        END IF;
    END IF;

    -- Rule 3 (Hook Mislift Escalation Rule)
    -- IF primary_fabric_defect_code = HOOK_MISLIFT_PATTERN_ERROR
    -- → TRIGGER: AUTO_ALERT_LOOM_SUPERVISOR
    -- → SET qa_inspector_approval_state = REJECTED_RETURN_TO_SUPERVISOR
    IF NEW.primary_fabric_defect_code = 'HOOK_MISLIFT_PATTERN_ERROR' THEN
        NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'AUTO_ALERT_LOOM_SUPERVISOR',
                'message', 'CHECK_SOLENOID_BOARD_ON_LOOM_LINE',
                'severity', 'WARNING'
            ));
        NEW.qa_inspector_approval_state := 'REJECTED_RETURN_TO_SUPERVISOR';
    END IF;

    -- Rule 4 (Packing Warehouse Transfer Authorization Gate)
    -- IF qa_inspector_approval_state NOT EQUAL TO PASSED_CLEARED_FOR_PACKING
    -- → BLOCK: DENY_FINISHED_GOODS_WAREHOUSE_TRANSFER
    IF NEW.qa_inspector_approval_state IS NOT NULL AND NEW.qa_inspector_approval_state != 'PASSED_CLEARED_FOR_PACKING' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_FINISHED_GOODS_WAREHOUSE_TRANSFER',
                'message', 'INSPECTION_NOT_CLEARED_FOR_PACKING',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Piece-Rate Financial Penalty Trigger
    IF NEW.final_fabric_quality_grade IS NOT NULL THEN
        IF NEW.final_fabric_quality_grade = 'GRADE_A_EXPORT_PREMIUM' THEN
            NEW.piece_rate_release_status := 'RELEASED_FULL';
            NEW.piece_rate_penalty_applied := FALSE;
            NEW.piece_rate_penalty_percent := 0.0;
        ELSIF NEW.final_fabric_quality_grade = 'GRADE_B_DOMESTIC_MINOR_DEFECT' THEN
            NEW.piece_rate_release_status := 'RELEASED_PARTIAL';
            NEW.piece_rate_penalty_applied := TRUE;
            NEW.piece_rate_penalty_percent := 20.0;
        ELSIF NEW.final_fabric_quality_grade IN ('GRADE_C_RESERVE_DISCOUNT', 'REJECTED_SCRAP') THEN
            NEW.piece_rate_release_status := 'FROZEN_PENDING_MENDING';
            NEW.piece_rate_penalty_applied := TRUE;
            NEW.piece_rate_penalty_percent := 100.0;
        END IF;
    END IF;

    -- Automatic B2B Order Matcher
    IF NEW.final_fabric_quality_grade = 'GRADE_A_EXPORT_PREMIUM' AND NEW.qa_inspector_approval_state = 'PASSED_CLEARED_FOR_PACKING' THEN
        NEW.b2b_order_matched := TRUE;
        NEW.b2b_order_status := 'MATCHED_READY_FOR_PACKING';
    END IF;

    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'QUALITY_QC_HOLD';
        ELSIF NEW.qa_inspector_approval_state = 'PASSED_CLEARED_FOR_PACKING' THEN
            NEW.auto_assigned_routing := 'READY_FOR_PACKING';
        ELSIF NEW.qa_inspector_approval_state = 'HOLD_SECOND_AUDIT_REQUIRED' THEN
            NEW.auto_assigned_routing := 'SECOND_AUDIT_HOLD';
        ELSE
            NEW.auto_assigned_routing := 'INSPECTION_IN_PROGRESS';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_quality_inspector_log ON quality_inspector_logs;

CREATE TRIGGER trigger_validate_quality_inspector_log
    BEFORE INSERT OR UPDATE ON quality_inspector_logs
    FOR EACH ROW EXECUTE FUNCTION validate_quality_inspector_log();

-- ------------------------------------------------------------
-- 6. TRIGGER: Auto-generate certificate on clearance
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION generate_quality_inspector_certificate()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.qa_inspector_approval_state = 'PASSED_CLEARED_FOR_PACKING'
       AND OLD.qa_inspector_approval_state IS DISTINCT FROM NEW.qa_inspector_approval_state THEN
        NEW.certificate_hash := generate_certificate_hash(NEW.id, NEW.inspection_id);
        NEW.qr_tag_id := 'QI-' || NEW.inspection_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_generate_quality_inspector_certificate ON quality_inspector_logs;

CREATE TRIGGER trigger_generate_quality_inspector_certificate
    BEFORE UPDATE OF qa_inspector_approval_state ON quality_inspector_logs
    FOR EACH ROW EXECUTE FUNCTION generate_quality_inspector_certificate();

-- ------------------------------------------------------------
-- 7. TRIGGER: Update timestamps
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_quality_inspector_logs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_quality_inspector_logs_updated_at ON quality_inspector_logs;

CREATE TRIGGER trigger_update_quality_inspector_logs_updated_at
    BEFORE UPDATE ON quality_inspector_logs
    FOR EACH ROW EXECUTE FUNCTION update_quality_inspector_logs_updated_at();
