-- ============================================================
-- Migration: 024_petni_master.sql
-- Petni Master (Warp Pulling & Reed Denting Specialist) module
-- Petni warp transition joint, reed denting, dropper pinning,
-- lease verification, border alignment, and loom activation.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE PETNI-MASTER ROLE
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","petni:create","petni:read","petni:write","petni:certificate:read","sales:forecast:read"]',
    description = 'Petni Master (Warp Pulling & Reed Denting Specialist): Petni warp transition joint, reed denting, dropper pinning, lease verification, border alignment, structural integrity checks, loom activation release, Petni certification'
WHERE role_id = 'ROLE-PETNI-MASTER';

-- ------------------------------------------------------------
-- 2. PETNI MASTER JOBS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS petni_master_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    petni_job_card_id VARCHAR(100) UNIQUE NOT NULL,
    loom_number_id VARCHAR(100) NOT NULL,
    production_lot_id UUID REFERENCES production_lots(id),
    design_master_id UUID REFERENCES design_masters(id),
    warp_joining_job_id UUID REFERENCES warp_joining_jobs(id),
    warp_beam_production_log_id UUID REFERENCES warp_beam_production_logs(id),
    harness_setup_log_id UUID REFERENCES harness_setup_logs(id),
    card_puncher_job_id UUID REFERENCES programming_jobs(id),
    pirn_winding_job_id UUID REFERENCES pirn_winding_jobs(id),
    bobbin_winder_job_card_id UUID REFERENCES winding_job_cards(id),
    factory_node_id VARCHAR(50) NOT NULL,
    master_petni_employee_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'PETNI_IN_PROGRESS' CHECK (status IN (
        'PETNI_IN_PROGRESS', 'PASSED_APPROVED_FOR_FIRST_PICK', 'REJECTED_CROSSED_ENDS',
        'REJECTED_REED_MISDENT', 'LOOM_ACTIVE_PRODUCTION', 'PETNI_SETUP_HOLD', 'ARCHIVED'
    )),

    -- Category A: Petni & Denting Setup Configuration
    warp_set_id VARCHAR(100),
    petni_transition_method VARCHAR(50) DEFAULT 'COMB_TENSIONED_PETNI_KNOTTING' CHECK (petni_transition_method IN (
        'COMB_TENSIONED_PETNI_KNOTTING',
        'STANDARD_MANUAL_HAND_PETNI',
        'ADHESIVE_TAPE_FUSION_JOINT',
        'DIRECT_LOOM_DRAW_IN'
    )),
    reed_denting_draft_pattern VARCHAR(50) DEFAULT '4_ENDS_DENT_144_EPI' CHECK (reed_denting_draft_pattern IN (
        '4_ENDS_DENT_144_EPI',
        '3_ENDS_DENT_120_EPI',
        '2_ENDS_DENT_STANDARD',
        'VARIABLE_BORDER_BODY_DRAFT'
    )),
    dropper_wire_specification VARCHAR(50) DEFAULT '0_3G_ULTRA_LIGHTWEIGHT_CLOSED_O_WIRE' CHECK (dropper_wire_specification IN (
        '0_3G_ULTRA_LIGHTWEIGHT_CLOSED_O_WIRE',
        '0_5G_OPEN_U_WIRE',
        '0_7G_STANDARD_HEAVY_WIRE'
    )),
    lease_order_verification VARCHAR(50) DEFAULT '1X1_STRICT_LEASE_LOCK' CHECK (lease_order_verification IN (
        '1X1_STRICT_LEASE_LOCK',
        '2X2_GROUP_LEASE_LOCK',
        'UNVERIFIED_LEASE'
    )),
    contrast_type VARCHAR(50) DEFAULT 'SIDE_BORDERS_ONLY' CHECK (contrast_type IN (
        'SIDE_BORDERS_ONLY',
        'PALLU_END_PIECE_ONLY',
        'FULL_BODY_BORDER_PALLU_THREE_SHUTTLE_KORVAI'
    )),
    body_silk_lot_no VARCHAR(100),
    contrast_silk_lot_no VARCHAR(100),

    -- Category B: Quality Inspection & Alignment Audit
    crossed_ends_count INTEGER DEFAULT 0,
    reed_mark_laser_inspection VARCHAR(50) DEFAULT 'PASSED_UNIFORM_DENTING' CHECK (reed_mark_laser_inspection IN (
        'PASSED_UNIFORM_DENTING',
        'WARNING_MINOR_DENT_SPACING_VAR',
        'FAILED_MISDENTED_REED'
    )),
    petni_joint_tension_variance_grams DECIMAL(6,2),
    border_channel_offset_mm DECIMAL(6,2),

    -- Category C: Final Verification & System Status
    dropper_pinning_completion_status VARCHAR(50) DEFAULT '100_PERCENT_DROPPERS_PINNED_AND_TESTED' CHECK (dropper_pinning_completion_status IN (
        '100_PERCENT_DROPPERS_PINNED_AND_TESTED',
        'PARTIAL_DROPPERS_MISSING',
        'ELECTRICAL_SHORT_DETECTED'
    )),
    petni_master_approval_state VARCHAR(50) DEFAULT 'PETNI_IN_PROGRESS' CHECK (petni_master_approval_state IN (
        'PETNI_IN_PROGRESS',
        'PASSED_APPROVED_FOR_FIRST_PICK',
        'REJECTED_CROSSED_ENDS',
        'REJECTED_REED_MISDENT'
    )),

    -- Logistics & Resource Mapping
    saree_production_order_ref VARCHAR(100),

    -- Time and Processing Metrics
    petni_start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    petni_end_time TIMESTAMP WITH TIME ZONE,
    total_threads_spliced_count INTEGER,
    joint_clearance_status VARCHAR(50) DEFAULT 'PASSED_TENSION_TEST' CHECK (joint_clearance_status IN (
        'PASSED_TENSION_TEST',
        'REQUIRED_RE_SPLICING',
        'FAILED_WARP_REALIGNMENT_NEEDED'
    )),

    -- Contrast Inventory Depletion
    contrast_yarn_weight_consumed_kg DECIMAL(10,3),

    -- Automated Validation
    validation_errors JSONB DEFAULT '[]'::jsonb,
    validation_warnings JSONB DEFAULT '[]'::jsonb,
    auto_assigned_routing VARCHAR(50),

    -- Certification
    certificate_hash VARCHAR(255),
    qr_tag_id VARCHAR(100) UNIQUE,
    certification_data JSONB DEFAULT '{}'::jsonb,

    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_petni_master_jobs_card ON petni_master_jobs(petni_job_card_id);
CREATE INDEX IF NOT EXISTS idx_petni_master_jobs_loom ON petni_master_jobs(loom_number_id);
CREATE INDEX IF NOT EXISTS idx_petni_master_jobs_factory ON petni_master_jobs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_petni_master_jobs_status ON petni_master_jobs(status);
CREATE INDEX IF NOT EXISTS idx_petni_master_jobs_approval ON petni_master_jobs(petni_master_approval_state);

-- ------------------------------------------------------------
-- 3. PETNI MASTER CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS petni_master_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    petni_master_job_id UUID NOT NULL REFERENCES petni_master_jobs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    petni_job_card_id VARCHAR(100) NOT NULL,
    loom_number_id VARCHAR(100) NOT NULL,
    warp_set_id VARCHAR(100),
    petni_transition_method VARCHAR(50),
    reed_denting_draft_pattern VARCHAR(50),
    dropper_wire_specification VARCHAR(50),
    lease_order_verification VARCHAR(50),
    contrast_type VARCHAR(50),
    body_silk_lot_no VARCHAR(100),
    contrast_silk_lot_no VARCHAR(100),
    crossed_ends_count INTEGER,
    reed_mark_laser_inspection VARCHAR(50),
    petni_joint_tension_variance_grams DECIMAL(6,2),
    border_channel_offset_mm DECIMAL(6,2),
    dropper_pinning_completion_status VARCHAR(50),
    petni_master_approval_state VARCHAR(50),
    total_threads_spliced_count INTEGER,
    joint_clearance_status VARCHAR(50),
    contrast_yarn_weight_consumed_kg DECIMAL(10,3),
    auto_assigned_routing VARCHAR(50) NOT NULL,
    master_petni_employee_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    certification_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'REVOKED', 'EXPIRED', 'PENDING'
    )),
    certified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_petni_master_certificates_job ON petni_master_certificates(petni_master_job_id);
CREATE INDEX IF NOT EXISTS idx_petni_master_certificates_hash ON petni_master_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_petni_master_certificates_qr ON petni_master_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 4. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_petni_master_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_petni_master_plan_factory ON sales_forecast_petni_master_plan(factory_node_id);

-- ------------------------------------------------------------
-- 5. TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_petni_master_job()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1 (Zero Crossed Ends Guardrail)
    -- IF crossed_ends_count > 0
    -- → BLOCK: CROSSED_ENDS_DETECTED → SET petni_master_approval_state = REJECTED_CROSSED_ENDS
    IF NEW.crossed_ends_count IS NOT NULL AND NEW.crossed_ends_count > 0 THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'CROSSED_ENDS_DETECTED',
                'message', 'Must resolve all crossed ends before weaving.',
                'severity', 'BLOCK'
            ));
        NEW.petni_master_approval_state := 'REJECTED_CROSSED_ENDS';
    END IF;

    -- Rule 2 (Dropper Wire Weight Compliance Gate)
    -- IF reed_denting_draft_pattern = 4 Ends / Dent (144 EPI) AND dropper_wire_specification = 0.7g Standard Heavy Wire
    -- → BLOCK: INVALID_DROPPER_WEIGHT
    IF NEW.reed_denting_draft_pattern = '4_ENDS_DENT_144_EPI' THEN
        IF NEW.dropper_wire_specification = '0_7G_STANDARD_HEAVY_WIRE' THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'INVALID_DROPPER_WEIGHT',
                    'message', 'Heavy droppers will damage fine silk warp.',
                    'severity', 'BLOCK'
                ));
        END IF;
    END IF;

    -- Rule 3 (Reed Denting Quality Gate)
    -- IF reed_mark_laser_inspection = FAILED_MISDENTED_REED
    -- → BLOCK: CANNOT_CLEAR_LOOM
    IF NEW.reed_mark_laser_inspection = 'FAILED_MISDENTED_REED' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'CANNOT_CLEAR_LOOM',
                'message', 'Misdented reed will cause permanent fabric streaks.',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Rule 4 (First-Pick Production Authorization Gate)
    -- IF petni_master_approval_state NOT EQUAL TO PASSED_APPROVED_FOR_FIRST_PICK
    -- → BLOCK: DENY_LOOM_WEAVER_START_SIGNAL
    IF NEW.petni_master_approval_state != 'PASSED_APPROVED_FOR_FIRST_PICK' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_LOOM_WEAVER_START_SIGNAL',
                'message', 'Petni master approval state must be PASSED_APPROVED_FOR_FIRST_PICK before loom weaving start.',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Contrast Inventory Depletion (Multi-Lot Balancing)
    -- Deduct contrast yarn weight from inventory
    IF NEW.contrast_yarn_weight_consumed_kg IS NOT NULL AND NEW.contrast_yarn_weight_consumed_kg > 0 THEN
        NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'CONTRAST_INVENTORY_DEPLETED',
                'message', format('Contrast yarn weight of %s kg has been allocated to loom asset.', NEW.contrast_yarn_weight_consumed_kg),
                'severity', 'INFO'
            ));
    END IF;

    -- Production State Transition: IF joint_clearance_status = PASSED_TENSION_TEST
    -- → SET status = LOOM_ACTIVE_PRODUCTION
    IF NEW.joint_clearance_status = 'PASSED_TENSION_TEST' THEN
        NEW.status := 'LOOM_ACTIVE_PRODUCTION';
    END IF;

    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'PETNI_QC_HOLD';
        ELSIF NEW.petni_master_approval_state = 'PASSED_APPROVED_FOR_FIRST_PICK' THEN
            NEW.auto_assigned_routing := 'READY_FOR_WEAVING';
        ELSIF NEW.joint_clearance_status = 'PASSED_TENSION_TEST' THEN
            NEW.auto_assigned_routing := 'LOOM_ACTIVE_PRODUCTION';
        ELSE
            NEW.auto_assigned_routing := 'PETNI_IN_PROGRESS';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_petni_master_job ON petni_master_jobs;

CREATE TRIGGER trigger_validate_petni_master_job
    BEFORE INSERT OR UPDATE ON petni_master_jobs
    FOR EACH ROW EXECUTE FUNCTION validate_petni_master_job();

-- ------------------------------------------------------------
-- 6. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_petni_master_jobs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_petni_master_jobs_updated_at ON petni_master_jobs;

CREATE TRIGGER trigger_update_petni_master_jobs_updated_at
    BEFORE UPDATE ON petni_master_jobs
    FOR EACH ROW EXECUTE FUNCTION update_petni_master_jobs_updated_at();

-- ------------------------------------------------------------
-- 7. UPDATE WARP-JOINER ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","warp_join:create","warp_join:read","warp_join:write","warp_join:certificate:read","petni:read","sales:forecast:read"]',
    description = 'Warp Joiner (Tie-in Master / Knotting Specialist): warp sheet alignment, precision end-to-end knotting, adhesive application, drawing-through, lease correction, loom activation release, warp joining certification; provides pre-process warp joining to Petni Master'
WHERE role_id = 'ROLE-WARP-JOINER';
