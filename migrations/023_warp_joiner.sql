-- ============================================================
-- Migration: 023_warp_joiner.sql
-- Warp Joiner (Tie-in Master / Knotting Specialist) module
-- Warp sheet alignment, precision end-to-end knotting,
-- adhesive application, drawing-through, lease correction,
-- and loom activation release.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE WARP-JOINER ROLE
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","warp_join:create","warp_join:read","warp_join:write","warp_join:certificate:read","sales:forecast:read"]',
    description = 'Warp Joiner (Tie-in Master / Knotting Specialist): warp sheet alignment, precision end-to-end knotting, adhesive application, drawing-through, lease correction, loom activation release, warp joining certification'
WHERE role_id = 'ROLE-WARP-JOINER';

-- ------------------------------------------------------------
-- 2. WARP JOINING JOBS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS warp_joining_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    joining_job_card_id VARCHAR(100) UNIQUE NOT NULL,
    loom_number_id VARCHAR(100) NOT NULL,
    production_lot_id UUID REFERENCES production_lots(id),
    design_master_id UUID REFERENCES design_masters(id),
    harness_setup_log_id UUID REFERENCES harness_setup_logs(id),
    warp_beam_production_log_id UUID REFERENCES warp_beam_production_logs(id),
    card_puncher_job_id UUID REFERENCES programming_jobs(id),
    pirn_winding_job_id UUID REFERENCES pirn_winding_jobs(id),
    bobbin_winder_job_card_id UUID REFERENCES winding_job_cards(id),
    factory_node_id VARCHAR(50) NOT NULL,
    warp_joiner_employee_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'TYING_IN_PROGRESS' CHECK (status IN (
        'TYING_IN_PROGRESS', 'PASSED_READY_FOR_WEAVER_START', 'REJECTED_HIGH_KNOT_FAILURE',
        'REJECTED_MISALIGNED_LEASE', 'LOOM_ACTIVE_PRODUCTION', 'LOOM_DOWNTIME_GATE', 'ARCHIVED'
    )),

    -- Category A: Warp Setup & Tying Machine Configuration
    warp_set_id VARCHAR(100),
    tying_machine_model VARCHAR(50) DEFAULT 'STAUBLI_TOPMATIC' CHECK (tying_machine_model IN (
        'STAUBLI_TOPMATIC',
        'KNOTEX_AS_3',
        'GROZ_BECKERT_KNOTMASTER',
        'MANUAL_HAND_TYING'
    )),
    separation_needle_type VARCHAR(50) DEFAULT 'ULTRA_FINE_SILK_NEEDLE_LE_0_3MM' CHECK (separation_needle_type IN (
        'ULTRA_FINE_SILK_NEEDLE_LE_0_3MM',
        'FINE_NEEDLE_0_5MM',
        'STANDARD_NEEDLE_0_8MM'
    )),
    target_tying_speed_kpm INTEGER,
    knot_type_selection VARCHAR(50) DEFAULT 'DOUBLE_LOOP_MICRO_KNOT' CHECK (knot_type_selection IN (
        'DOUBLE_LOOP_MICRO_KNOT',
        'STANDARD_SINGLE_OVERHAND',
        'FLAT_SECURITY_KNOT'
    )),

    -- Category B: Quality Inspection & Defect Audit
    knot_tail_length_mm DECIMAL(6,2),
    double_end_detection_status VARCHAR(50) DEFAULT 'ZERO_DOUBLE_ENDS_DETECTED' CHECK (double_end_detection_status IN (
        'ZERO_DOUBLE_ENDS_DETECTED',
        'MINOR_DOUBLE_ENDS_CORRECTED',
        'FAULTY_SEPARATION_ABORT'
    )),
    manual_repair_knot_count INTEGER DEFAULT 0,
    knot_pull_through_mode VARCHAR(50) DEFAULT 'MANUAL_HAND_CRANK_CREEP_PULL' CHECK (knot_pull_through_mode IN (
        'MANUAL_HAND_CRANK_CREEP_PULL',
        'ELECTRONIC_SLOW_INCHING_5_PERCENT_SPEED',
        'FULL_SPEED_PULL_UNSAFE'
    )),

    -- Category C: Final Verification & System Status
    knot_pull_through_status VARCHAR(50) DEFAULT 'PASSED_100_PERCENT_KNOTS_CLEARED' CHECK (knot_pull_through_status IN (
        'PASSED_100_PERCENT_KNOTS_CLEARED',
        'FAILED_KNOT_SNAG_IN_MAIL_EYE',
        'FAILED_THREAD_BREAKAGE'
    )),
    warp_joiner_approval_state VARCHAR(50) DEFAULT 'TYING_IN_PROGRESS' CHECK (warp_joiner_approval_state IN (
        'TYING_IN_PROGRESS',
        'PASSED_READY_FOR_WEAVER_START',
        'REJECTED_HIGH_KNOT_FAILURE',
        'REJECTED_MISALIGNED_LEASE'
    )),

    -- Logistics & Resource Mapping
    new_warp_beam_lot_no VARCHAR(100),

    -- Time, Efficiency, and Quality Metrics
    start_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    end_timestamp TIMESTAMP WITH TIME ZONE,
    total_ends_to_join INTEGER,
    missed_ends_count INTEGER DEFAULT 0,
    total_joining_hours DECIMAL(6,2),
    loom_idle_duration_hours DECIMAL(6,2),
    loom_idle_variance_alert BOOLEAN DEFAULT FALSE,

    -- Piecemeal Wage Calculation
    knots_completed_count INTEGER DEFAULT 0,
    wage_per_hundred_knots DECIMAL(10,2),
    calculated_wage_payout DECIMAL(10,2),

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

CREATE INDEX IF NOT EXISTS idx_warp_joining_jobs_card ON warp_joining_jobs(joining_job_card_id);
CREATE INDEX IF NOT EXISTS idx_warp_joining_jobs_loom ON warp_joining_jobs(loom_number_id);
CREATE INDEX IF NOT EXISTS idx_warp_joining_jobs_factory ON warp_joining_jobs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_warp_joining_jobs_status ON warp_joining_jobs(status);
CREATE INDEX IF NOT EXISTS idx_warp_joining_jobs_approval ON warp_joining_jobs(warp_joiner_approval_state);

-- ------------------------------------------------------------
-- 3. WARP JOINING CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS warp_joining_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    warp_joining_job_id UUID NOT NULL REFERENCES warp_joining_jobs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    joining_job_card_id VARCHAR(100) NOT NULL,
    loom_number_id VARCHAR(100) NOT NULL,
    warp_set_id VARCHAR(100),
    tying_machine_model VARCHAR(50),
    separation_needle_type VARCHAR(50),
    target_tying_speed_kpm INTEGER,
    knot_type_selection VARCHAR(50),
    knot_tail_length_mm DECIMAL(6,2),
    double_end_detection_status VARCHAR(50),
    manual_repair_knot_count INTEGER,
    knot_pull_through_mode VARCHAR(50),
    knot_pull_through_status VARCHAR(50),
    warp_joiner_approval_state VARCHAR(50),
    total_ends_to_join INTEGER,
    missed_ends_count INTEGER,
    total_joining_hours DECIMAL(6,2),
    loom_idle_duration_hours DECIMAL(6,2),
    loom_idle_variance_alert BOOLEAN,
    knots_completed_count INTEGER,
    calculated_wage_payout DECIMAL(10,2),
    auto_assigned_routing VARCHAR(50) NOT NULL,
    warp_joiner_employee_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    certification_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'REVOKED', 'EXPIRED', 'PENDING'
    )),
    certified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_warp_joining_certificates_job ON warp_joining_certificates(warp_joining_job_id);
CREATE INDEX IF NOT EXISTS idx_warp_joining_certificates_hash ON warp_joining_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_warp_joining_certificates_qr ON warp_joining_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 4. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_warp_joiner_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_warp_joiner_plan_factory ON sales_forecast_warp_joiner_plan(factory_node_id);

-- ------------------------------------------------------------
-- 5. TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_warp_joining_job()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1 (2400 Hook Tying Speed Protection Guardrail)
    -- IF separation_needle_type = Ultra-Fine Silk Needle (<=0.3mm) AND target_tying_speed_kpm > 250
    -- → BLOCK: EXCESSIVE_TYING_SPEED_WARNING
    IF NEW.separation_needle_type = 'ULTRA_FINE_SILK_NEEDLE_LE_0_3MM' THEN
        IF NEW.target_tying_speed_kpm IS NOT NULL AND NEW.target_tying_speed_kpm > 250 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'EXCESSIVE_TYING_SPEED_WARNING',
                    'message', 'Ultra-fine silk needle requires tying speed <= 250 KPM. High speed will split fine silk filaments.',
                    'severity', 'BLOCK'
                ));
        END IF;
    END IF;

    -- Rule 2 (Knot Tail Clearance Gate)
    -- IF knot_tail_length_mm > 1.8
    -- → BLOCK: REJECT_LONG_KNOT_TAILS → SET warp_joiner_approval_state = REJECTED_HIGH_KNOT_FAILURE
    IF NEW.knot_tail_length_mm IS NOT NULL AND NEW.knot_tail_length_mm > 1.8 THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'REJECT_LONG_KNOT_TAILS',
                'message', 'Knot tails will tangle in 2400 hook mail eyes.',
                'severity', 'BLOCK'
            ));
        NEW.warp_joiner_approval_state := 'REJECTED_HIGH_KNOT_FAILURE';
    END IF;

    -- Rule 3 (Safe Pull-Through Enforcement)
    -- IF knot_pull_through_mode = Full Speed Pull (Unsafe)
    -- → BLOCK: INVALID_PULL_THROUGH_MODE
    IF NEW.knot_pull_through_mode = 'FULL_SPEED_PULL_UNSAFE' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'INVALID_PULL_THROUGH_MODE',
                'message', 'Manual creep or slow inching is mandatory for safe pull-through.',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Rule 4 (Loom Weaver Handover Gate)
    -- IF warp_joiner_approval_state NOT EQUAL TO PASSED_READY_FOR_WEAVER_START
    -- → BLOCK: DENY_LOOM_WEAVING_PRODUCTION_START
    IF NEW.warp_joiner_approval_state != 'PASSED_READY_FOR_WEAVER_START' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_LOOM_WEAVING_PRODUCTION_START',
                'message', 'Warp joiner approval state must be PASSED_READY_FOR_WEAVER_START before loom weaving production start.',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Loom Downtime Financial Audit
    -- IF loom_idle_duration_hours > 6.0
    -- → SET loom_idle_variance_alert = TRUE
    IF NEW.loom_idle_duration_hours IS NOT NULL AND NEW.loom_idle_duration_hours > 6.0 THEN
        NEW.loom_idle_variance_alert := TRUE;
    END IF;

    -- Piecemeal Wage Calculation Script
    -- wage_per_hundred_knots * (knots_completed_count / 100)
    IF NEW.knots_completed_count IS NOT NULL AND NEW.wage_per_hundred_knots IS NOT NULL THEN
        NEW.calculated_wage_payout := NEW.wage_per_hundred_knots * (NEW.knots_completed_count / 100.0);
    END IF;

    -- Production State Transition: IF knot_pull_through_status = PASSED_100%_KNOTS_CLEARED
    -- → SET status = LOOM_ACTIVE_PRODUCTION
    IF NEW.knot_pull_through_status = 'PASSED_100_PERCENT_KNOTS_CLEARED' THEN
        NEW.status := 'LOOM_ACTIVE_PRODUCTION';
    END IF;

    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'JOIN_QC_HOLD';
        ELSIF NEW.warp_joiner_approval_state = 'PASSED_READY_FOR_WEAVER_START' THEN
            NEW.auto_assigned_routing := 'READY_FOR_WEAVER_START';
        ELSIF NEW.loom_idle_variance_alert = TRUE THEN
            NEW.auto_assigned_routing := 'LOOM_IDLE_VARIANCE_ALERT';
        ELSE
            NEW.auto_assigned_routing := 'TYING_IN_PROGRESS';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_warp_joining_job ON warp_joining_jobs;

CREATE TRIGGER trigger_validate_warp_joining_job
    BEFORE INSERT OR UPDATE ON warp_joining_jobs
    FOR EACH ROW EXECUTE FUNCTION validate_warp_joining_job();

-- ------------------------------------------------------------
-- 6. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_warp_joining_jobs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_warp_joining_jobs_updated_at ON warp_joining_jobs;

CREATE TRIGGER trigger_update_warp_joining_jobs_updated_at
    BEFORE UPDATE ON warp_joining_jobs
    FOR EACH ROW EXECUTE FUNCTION update_warp_joining_jobs_updated_at();

-- ------------------------------------------------------------
-- 7. UPDATE LOOM-HARNESS-SETTER ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","harness:create","harness:read","harness:write","harness:certificate:read","warp_join:read","sales:forecast:read"]',
    description = 'Loom Harness Setter (Harness Building Master): harness cord assembly, comber board positioning, mail eye leveling, lingo weight integration, shed alignment, mechanical synchronization, preventive maintenance tracking, harness certification; provides pre-process harness setup to Warp Joiner'
WHERE role_id = 'ROLE-LOOM-HARNESS-SETTER';
