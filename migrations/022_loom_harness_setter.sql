-- ============================================================
-- Migration: 022_loom_harness_setter.sql
-- Loom Harness Setter (Harness Building Master) module
-- Harness cord assembly, comber board positioning, mail eye
-- leveling, lingo weight integration, shed alignment, and
-- mechanical synchronization with Jacquard head.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE LOOM-HARNESS-SETTER ROLE
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","harness:create","harness:read","harness:write","harness:certificate:read","sales:forecast:read"]',
    description = 'Loom Harness Setter (Harness Building Master): harness cord assembly, comber board positioning, mail eye leveling, lingo weight integration, shed alignment, mechanical synchronization, preventive maintenance tracking, harness certification'
WHERE role_id = 'ROLE-LOOM-HARNESS-SETTER';

-- ------------------------------------------------------------
-- 2. HARNESS SETUP LOGS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS harness_setup_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    harness_setup_job_id VARCHAR(100) UNIQUE NOT NULL,
    loom_hardware_id VARCHAR(100) NOT NULL,
    production_lot_id UUID REFERENCES production_lots(id),
    design_master_id UUID REFERENCES design_masters(id),
    warp_beam_production_log_id UUID REFERENCES warp_beam_production_logs(id),
    card_puncher_job_id UUID REFERENCES programming_jobs(id),
    pirn_winding_job_id UUID REFERENCES pirn_winding_jobs(id),
    bobbin_winder_job_card_id UUID REFERENCES winding_job_cards(id),
    factory_node_id VARCHAR(50) NOT NULL,
    setter_employee_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'SETUP_IN_PROGRESS' CHECK (status IN (
        'SETUP_IN_PROGRESS', 'PASSED_APPROVED_FOR_GAITING', 'REJECTED_LEVELING_ERROR',
        'REJECTED_LINGO_WEIGHT_MISMATCH', 'VACANT_AVAILABLE_FOR_WEAVING', 'UNDER_REPAIR_MAINTENANCE', 'ARCHIVED'
    )),

    -- Category A: Harness Structure & Mechanical Layout
    jacquard_capacity_type VARCHAR(50) DEFAULT '2400_HOOK' CHECK (jacquard_capacity_type IN (
        '2400_HOOK',
        '1536_HOOK',
        '1200_HOOK',
        '600_HOOK'
    )),
    comber_board_density_epi VARCHAR(50) DEFAULT '144_EPI' CHECK (comber_board_density_epi IN (
        '144_EPI',
        '120_EPI',
        '112_EPI',
        '96_EPI'
    )),
    harness_cord_material VARCHAR(50) DEFAULT 'NOMEX_CORE_LOW_STRETCH_SYNTHETIC' CHECK (harness_cord_material IN (
        'NOMEX_CORE_LOW_STRETCH_SYNTHETIC',
        'STANDARD_BRAIDED_POLYESTER',
        'KEVLAR_REINFORCED_CORE'
    )),
    lingo_weight_per_cord_grams DECIMAL(6,2),
    harness_tie_up_profile VARCHAR(50) DEFAULT 'STRAIGHT_TIE' CHECK (harness_tie_up_profile IN (
        'STRAIGHT_TIE',
        'REPEAT_TIE',
        'BORDER_BODY_POINT_TIE'
    )),
    total_active_harness_cords INTEGER,
    reed_count_density INTEGER,

    -- Category B: Shed Geometry & Alignment Audit
    mail_eye_leveling_status VARCHAR(50) DEFAULT 'LASER_VERIFIED_PLUS_MINUS_0_5MM' CHECK (mail_eye_leveling_status IN (
        'LASER_VERIFIED_PLUS_MINUS_0_5MM',
        'MANUAL_GAUGE_PLUS_MINUS_1_0MM',
        'UNVERIFIED_OUT_OF_ALIGNMENT'
    )),
    shed_opening_height_mm DECIMAL(6,2),
    harness_drop_angle_status VARCHAR(50) DEFAULT 'STRAIGHT_DROP_LE_8_DEG' CHECK (harness_drop_angle_status IN (
        'STRAIGHT_DROP_LE_8_DEG',
        'STANDARD_DROP_9_12_DEG',
        'STEEP_DROP_GT_12_DEG'
    )),
    antistatic_harness_lubrication VARCHAR(50) DEFAULT 'DRY_PTFE_LUBRICANT_APPLIED' CHECK (antistatic_harness_lubrication IN (
        'DRY_PTFE_LUBRICANT_APPLIED',
        'SILICONE_SPRAY_APPLIED',
        'NONE'
    )),
    comber_board_clearance_mm INTEGER,

    -- Category C: Final Verification & System Status
    dry_run_full_lift_test VARCHAR(50) DEFAULT 'PASSED_100_PERCENT_HOOK_CLEARANCE' CHECK (dry_run_full_lift_test IN (
        'PASSED_100_PERCENT_HOOK_CLEARANCE',
        'FAILED_SLUG_HESITATION',
        'FAILED_MISALIGNED_MAIL_EYES'
    )),
    harness_setup_approval_state VARCHAR(50) DEFAULT 'SETUP_IN_PROGRESS' CHECK (harness_setup_approval_state IN (
        'SETUP_IN_PROGRESS',
        'PASSED_APPROVED_FOR_GAITING',
        'REJECTED_LEVELING_ERROR',
        'REJECTED_LINGO_WEIGHT_MISMATCH'
    )),

    -- Maintenance, Consumables, and Lifecycle Fields
    cord_material_batch_no VARCHAR(100),
    setup_start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    setup_end_time TIMESTAMP WITH TIME ZONE,
    total_assembly_hours DECIMAL(6,2),
    accumulated_picks_on_harness BIGINT DEFAULT 0,
    preventive_maintenance_flag BOOLEAN DEFAULT FALSE,
    preventive_maintenance_task TEXT,

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

CREATE INDEX IF NOT EXISTS idx_harness_setup_logs_job ON harness_setup_logs(harness_setup_job_id);
CREATE INDEX IF NOT EXISTS idx_harness_setup_logs_loom ON harness_setup_logs(loom_hardware_id);
CREATE INDEX IF NOT EXISTS idx_harness_setup_logs_factory ON harness_setup_logs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_harness_setup_logs_status ON harness_setup_logs(status);
CREATE INDEX IF NOT EXISTS idx_harness_setup_logs_approval ON harness_setup_logs(harness_setup_approval_state);

-- ------------------------------------------------------------
-- 3. HARNESS CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS harness_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    harness_setup_log_id UUID NOT NULL REFERENCES harness_setup_logs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    harness_setup_job_id VARCHAR(100) NOT NULL,
    loom_hardware_id VARCHAR(100) NOT NULL,
    jacquard_capacity_type VARCHAR(50),
    comber_board_density_epi VARCHAR(50),
    harness_cord_material VARCHAR(50),
    lingo_weight_per_cord_grams DECIMAL(6,2),
    harness_tie_up_profile VARCHAR(50),
    total_active_harness_cords INTEGER,
    mail_eye_leveling_status VARCHAR(50),
    shed_opening_height_mm DECIMAL(6,2),
    harness_drop_angle_status VARCHAR(50),
    antistatic_harness_lubrication VARCHAR(50),
    dry_run_full_lift_test VARCHAR(50),
    harness_setup_approval_state VARCHAR(50),
    accumulated_picks_on_harness BIGINT,
    preventive_maintenance_flag BOOLEAN,
    auto_assigned_routing VARCHAR(50) NOT NULL,
    setter_employee_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    certification_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'REVOKED', 'EXPIRED', 'PENDING'
    )),
    certified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_harness_certificates_log ON harness_certificates(harness_setup_log_id);
CREATE INDEX IF NOT EXISTS idx_harness_certificates_hash ON harness_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_harness_certificates_qr ON harness_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 4. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_harness_setup_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_harness_setup_plan_factory ON sales_forecast_harness_setup_plan(factory_node_id);

-- ------------------------------------------------------------
-- 5. TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_harness_setup_log()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1 (2400 Hook Shed Height Protection Guardrail)
    -- IF jacquard_capacity_type = 2400 Hook AND shed_opening_height_mm > 48.0
    -- → BLOCK: EXCESSIVE_SHED_HEIGHT_WARNING
    IF NEW.jacquard_capacity_type = '2400_HOOK' THEN
        IF NEW.shed_opening_height_mm > 48.0 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'EXCESSIVE_SHED_HEIGHT_WARNING',
                    'message', '2400 Hook shed opening exceeds 48.0mm. Overstretch risk for high-density silk warp.',
                    'severity', 'BLOCK'
                ));
        END IF;
    END IF;

    -- Rule 2 (Lingo Weight Compliance Gate)
    -- IF jacquard_capacity_type = 2400 Hook AND (lingo_weight_per_cord_grams > 23.0 OR lingo_weight_per_cord_grams < 17.0)
    -- → BLOCK: INVALID_LINGO_WEIGHT
    IF NEW.jacquard_capacity_type = '2400_HOOK' THEN
        IF NEW.lingo_weight_per_cord_grams IS NOT NULL THEN
            IF NEW.lingo_weight_per_cord_grams > 23.0 OR NEW.lingo_weight_per_cord_grams < 17.0 THEN
                NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                    jsonb_build_array(jsonb_build_object(
                        'code', 'INVALID_LINGO_WEIGHT',
                        'message', '2400 Hook lingo weight must be between 18 and 22 grams.',
                        'severity', 'BLOCK'
                    ));
            END IF;
        END IF;
    END IF;

    -- Rule 3 (Mail Eye Alignment Clearance Gate)
    -- IF mail_eye_leveling_status = Unverified / Out of Alignment
    -- → BLOCK: CANNOT_CLEAR_HARNESS_FOR_WARP_GAITING → SET harness_setup_approval_state = REJECTED_LEVELING_ERROR
    IF NEW.mail_eye_leveling_status = 'UNVERIFIED_OUT_OF_ALIGNMENT' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'CANNOT_CLEAR_HARNESS_FOR_WARP_GAITING',
                'message', 'Risk of dirty shed and loom traps.',
                'severity', 'BLOCK'
            ));
        NEW.harness_setup_approval_state := 'REJECTED_LEVELING_ERROR';
    END IF;

    -- Rule 4 (Warp Drawing-In Clearance Authorization)
    -- IF harness_setup_approval_state NOT EQUAL TO PASSED_APPROVED_FOR_GAITING
    -- → BLOCK: DENY_WARP_BEAM_GAITING_WORK_ORDER
    IF NEW.harness_setup_approval_state != 'PASSED_APPROVED_FOR_GAITING' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_WARP_BEAM_GAITING_WORK_ORDER',
                'message', 'Harness setup approval state must be PASSED_APPROVED_FOR_GAITING before warp beam gaiting.',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Rule 5 (2400 Hook Capacity Logic Gate)
    -- IF jacquard_capacity_type = 2400 Hook AND total_active_harness_cords < 2400
    -- → BLOCK: LOOM_CAPABILITY_MISMATCH
    IF NEW.jacquard_capacity_type = '2400_HOOK' THEN
        IF NEW.total_active_harness_cords IS NOT NULL AND NEW.total_active_harness_cords < 2400 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'LOOM_CAPABILITY_MISMATCH',
                    'message', 'Selected loom lacks the required physical harness hooks for 2400 Hook design.',
                    'severity', 'BLOCK'
                ));
        END IF;
    END IF;

    -- Production State Transition: IF dry_run_full_lift_test = PASSED_100%_HOOK_CLEARANCE
    -- → SET status = VACANT_AVAILABLE_FOR_WEAVING
    IF NEW.dry_run_full_lift_test = 'PASSED_100_PERCENT_HOOK_CLEARANCE' THEN
        NEW.status := 'VACANT_AVAILABLE_FOR_WEAVING';
    END IF;

    -- Preventive Maintenance Tracker: IF accumulated_picks_on_harness >= 10,000,000
    -- → SET preventive_maintenance_flag = TRUE AND SET preventive_maintenance_task
    IF NEW.accumulated_picks_on_harness IS NOT NULL AND NEW.accumulated_picks_on_harness >= 10000000 THEN
        NEW.preventive_maintenance_flag := TRUE;
        NEW.preventive_maintenance_task := 'Schedule Tension Spring and Cord Friction Inspection on ' || COALESCE(NEW.loom_hardware_id, 'Unknown Loom');
    END IF;

    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'HARNESS_QC_HOLD';
        ELSIF NEW.harness_setup_approval_state = 'PASSED_APPROVED_FOR_GAITING' THEN
            NEW.auto_assigned_routing := 'READY_FOR_WARP_GAITING';
        ELSIF NEW.preventive_maintenance_flag = TRUE THEN
            NEW.auto_assigned_routing := 'PENDING_MAINTENANCE';
        ELSE
            NEW.auto_assigned_routing := 'SETUP_IN_PROGRESS';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_harness_setup_log ON harness_setup_logs;

CREATE TRIGGER trigger_validate_harness_setup_log
    BEFORE INSERT OR UPDATE ON harness_setup_logs
    FOR EACH ROW EXECUTE FUNCTION validate_harness_setup_log();

-- ------------------------------------------------------------
-- 6. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_harness_setup_logs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_harness_setup_logs_updated_at ON harness_setup_logs;

CREATE TRIGGER trigger_update_harness_setup_logs_updated_at
    BEFORE UPDATE ON harness_setup_logs
    FOR EACH ROW EXECUTE FUNCTION update_harness_setup_logs_updated_at();

-- ------------------------------------------------------------
-- 7. UPDATE WARP-BEAM-PREPARATION ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","beam:create","beam:read","beam:write","beam:certificate:read","harness:read","sales:forecast:read"]',
    description = 'Warp Beam Preparation Specialist (80 Saree Length): sectional warping, creel tension synchronization, beaming-off, mass balance accounting, lease insertion, loom mounting authorization, warp beam certification; provides pre-process warp beam to Loom Harness Setter'
WHERE role_id = 'ROLE-WARP-BEAM-PREPARATION';
