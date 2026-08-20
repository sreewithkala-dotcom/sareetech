-- ============================================================
-- Migration: 025_master_weaver.sql
-- Master Weaver (Loom Operator & Saree Production Specialist) module
-- Loom operation, real-time quality troubleshooting, weaver
-- skill-to-loom matching, material allocation, yield calculation,
-- and saree completion clearance.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE MASTER-WEAVER ROLE
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","weaving:create","weaving:read","weaving:write","weaving:certificate:read","sales:forecast:read"]',
    description = 'Master Weaver (Loom Operator & Saree Production Specialist): loom operation, real-time quality troubleshooting, weaver skill-to-loom matching, material allocation, yield calculation, first-pick verification, saree completion clearance, weaving certification'
WHERE role_id = 'ROLE-MASTER-WEAVER';

-- ------------------------------------------------------------
-- 2. MASTER WEAVER JOBS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS master_weaver_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    production_run_id VARCHAR(100) UNIQUE NOT NULL,
    loom_id VARCHAR(100) NOT NULL,
    production_lot_id UUID REFERENCES production_lots(id),
    design_master_id UUID REFERENCES design_masters(id),
    petni_master_job_id UUID REFERENCES petni_master_jobs(id),
    warp_joining_job_id UUID REFERENCES warp_joining_jobs(id),
    harness_setup_log_id UUID REFERENCES harness_setup_logs(id),
    warp_beam_production_log_id UUID REFERENCES warp_beam_production_logs(id),
    card_puncher_job_id UUID REFERENCES programming_jobs(id),
    pirn_winding_job_id UUID REFERENCES pirn_winding_jobs(id),
    bobbin_winder_job_card_id UUID REFERENCES winding_job_cards(id),
    factory_node_id VARCHAR(50) NOT NULL,
    master_weaver_employee_id UUID REFERENCES users(id),
    assigned_weaver_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'WEAVING_IN_PROGRESS' CHECK (status IN (
        'WEAVING_IN_PROGRESS', 'SAREE_COMPLETED_PENDING_CUT', 'LOOM_STOPPED_MAINTENANCE_REQUIRED',
        'SHIFT_HANDOVER_COMPLETE', 'LOOM_ACTIVE_PRODUCTION', 'IN_PROGRESS_WEAVING', 'ARCHIVED'
    )),

    -- Category A: Weaving Operation & Loom Kinematics
    saree_production_id VARCHAR(100),
    loom_operating_speed_ppm INTEGER,
    warp_let_off_tension_cn DECIMAL(8,2),
    weft_insertion_feeder_profile VARCHAR(50) DEFAULT 'DUAL_FEEDER_SILK_GROUND_PLUS_ZARI_MICRO_TENSION' CHECK (weft_insertion_feeder_profile IN (
        'DUAL_FEEDER_SILK_GROUND_PLUS_ZARI_MICRO_TENSION',
        'STANDARD_SINGLE_FEEDER',
        'HEAVY_EXTRA_WEFT_MULTI_FEEDER'
    )),
    dynamic_ppi_control_mode VARCHAR(50) DEFAULT 'AUTOMATED_MULTI_DENSITY_BODY_PALLU_SWITCH' CHECK (dynamic_ppi_control_mode IN (
        'AUTOMATED_MULTI_DENSITY_BODY_PALLU_SWITCH',
        'FIXED_SINGLE_PPI',
        'MANUAL_GEAR_SHIFT'
    )),

    -- Category B: Quality Checks & On-Loom Audit
    on_loom_defect_category VARCHAR(50) DEFAULT 'NONE_ZERO_DEFECTS' CHECK (on_loom_defect_category IN (
        'NONE_ZERO_DEFECTS',
        'WEFT_PICK_GAP_LOOM_STOP',
        'ZARI_LOOPING_TENSION_FAULT',
        'WARP_END_BREAKAGE',
        'BORDER_MISALIGNMENT'
    )),
    zari_catch_selvage_status VARCHAR(50) DEFAULT 'PERFECT_CATCH_SMOOTH_EDGE' CHECK (zari_catch_selvage_status IN (
        'PERFECT_CATCH_SMOOTH_EDGE',
        'LOOSE_ZARI_LOOPS_OVER_TENSION_NEEDED',
        'TIGHT_EDGE_PUCKERING_REDUCE_TENSION'
    )),
    saree_section_phase VARCHAR(50) DEFAULT 'PALLU_HIGH_DENSITY' CHECK (saree_section_phase IN (
        'PALLU_HIGH_DENSITY',
        'MAIN_BODY_MOTIF',
        'SKIRT_BORDER',
        'SAREE_CUT_LINE_TRANSITION'
    )),
    saree_length_measured_meters DECIMAL(8,2),

    -- Category C: Final Inspection & System Status
    saree_piece_clearance_status VARCHAR(50) DEFAULT 'PASSED_GRADE_A_QUALITY' CHECK (saree_piece_clearance_status IN (
        'PASSED_GRADE_A_QUALITY',
        'GRADE_B_MINOR_DEFECT_LOGGED',
        'REJECTED_CRITICAL_WEAVE_FAULT'
    )),
    weaver_approval_state VARCHAR(50) DEFAULT 'WEAVING_IN_PROGRESS' CHECK (weaver_approval_state IN (
        'WEAVING_IN_PROGRESS',
        'SAREE_COMPLETED_PENDING_CUT',
        'LOOM_STOPPED_MAINTENANCE_REQUIRED',
        'SHIFT_HANDOVER_COMPLETE'
    )),

    -- Resource & Machine Allocation
    saree_production_order_no VARCHAR(100),
    reed_width_inches DECIMAL(6,2),
    picks_per_inch_ppi INTEGER,
    first_pick_sample_status VARCHAR(50) DEFAULT 'APPROVED_FLAWLESS' CHECK (first_pick_sample_status IN (
        'APPROVED_FLAWLESS',
        'MINOR_ADJUSTMENT_NEEDED',
        'REJECTED_DESIGN_CORRUPTION'
    )),
    shuttle_setup_mode VARCHAR(50) DEFAULT 'SINGLE_SHUTTLE' CHECK (shuttle_setup_mode IN (
        'SINGLE_SHUTTLE',
        'TWO_SHUTTLE_DROP_BOX',
        'THREE_SHUTTLE_KORVAI_MANUAL_SPLIT'
    )),

    -- Raw Material Allocation Tracker
    allocated_weft_silk_lot_no VARCHAR(100),
    allocated_zari_lot_no VARCHAR(100),
    issued_zari_weight_gm DECIMAL(10,2),

    -- Daily Production Yield Calculation
    loom_rpm INTEGER,
    target_ppi INTEGER,
    efficiency_percent DECIMAL(6,2),
    target_output_yards_per_hour DECIMAL(10,2),
    actual_output_yards DECIMAL(10,2),
    low_efficiency_alert BOOLEAN DEFAULT FALSE,

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

CREATE INDEX IF NOT EXISTS idx_master_weaver_jobs_run ON master_weaver_jobs(production_run_id);
CREATE INDEX IF NOT EXISTS idx_master_weaver_jobs_loom ON master_weaver_jobs(loom_id);
CREATE INDEX IF NOT EXISTS idx_master_weaver_jobs_factory ON master_weaver_jobs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_master_weaver_jobs_status ON master_weaver_jobs(status);
CREATE INDEX IF NOT EXISTS idx_master_weaver_jobs_approval ON master_weaver_jobs(weaver_approval_state);

-- ------------------------------------------------------------
-- 3. MASTER WEAVER CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS master_weaver_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    master_weaver_job_id UUID NOT NULL REFERENCES master_weaver_jobs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    production_run_id VARCHAR(100) NOT NULL,
    loom_id VARCHAR(100) NOT NULL,
    saree_production_id VARCHAR(100),
    loom_operating_speed_ppm INTEGER,
    warp_let_off_tension_cn DECIMAL(8,2),
    weft_insertion_feeder_profile VARCHAR(50),
    dynamic_ppi_control_mode VARCHAR(50),
    on_loom_defect_category VARCHAR(50),
    zari_catch_selvage_status VARCHAR(50),
    saree_section_phase VARCHAR(50),
    saree_length_measured_meters DECIMAL(8,2),
    saree_piece_clearance_status VARCHAR(50),
    weaver_approval_state VARCHAR(50),
    shuttle_setup_mode VARCHAR(50),
    reed_width_inches DECIMAL(6,2),
    picks_per_inch_ppi INTEGER,
    first_pick_sample_status VARCHAR(50),
    allocated_weft_silk_lot_no VARCHAR(100),
    allocated_zari_lot_no VARCHAR(100),
    issued_zari_weight_gm DECIMAL(10,2),
    loom_rpm INTEGER,
    efficiency_percent DECIMAL(6,2),
    target_output_yards_per_hour DECIMAL(10,2),
    actual_output_yards DECIMAL(10,2),
    low_efficiency_alert BOOLEAN,
    auto_assigned_routing VARCHAR(50) NOT NULL,
    master_weaver_employee_id UUID REFERENCES users(id),
    assigned_weaver_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    certification_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'REVOKED', 'EXPIRED', 'PENDING'
    )),
    certified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_master_weaver_certificates_job ON master_weaver_certificates(master_weaver_job_id);
CREATE INDEX IF NOT EXISTS idx_master_weaver_certificates_hash ON master_weaver_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_master_weaver_certificates_qr ON master_weaver_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 4. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_master_weaver_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_master_weaver_plan_factory ON sales_forecast_master_weaver_plan(factory_node_id);

-- ------------------------------------------------------------
-- 5. TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_master_weaver_job()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1 (2400 Hook Speed Protection Guardrail)
    -- IF saree_section_phase = PALLU_HIGH_DENSITY AND loom_operating_speed_ppm > 165
    -- → BLOCK: OVERSPEED_WARNING
    IF NEW.saree_section_phase = 'PALLU_HIGH_DENSITY' THEN
        IF NEW.loom_operating_speed_ppm IS NOT NULL AND NEW.loom_operating_speed_ppm > 165 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'OVERSPEED_WARNING',
                    'message', 'Reduce loom speed during Zari Pallu weaving.',
                    'severity', 'BLOCK'
                ));
        END IF;
    END IF;

    -- Rule 2 (Warp Tension Variance Gate)
    -- IF warp_let_off_tension_cn > 150.0 OR warp_let_off_tension_cn < 110.0
    -- → BLOCK: AUTO_LOOM_PAUSE → SET weaver_approval_state = LOOM_STOPPED_MAINTENANCE_REQUIRED
    IF NEW.warp_let_off_tension_cn IS NOT NULL THEN
        IF NEW.warp_let_off_tension_cn > 150.0 OR NEW.warp_let_off_tension_cn < 110.0 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'AUTO_LOOM_PAUSE',
                    'message', 'Warp tension out of safe range. Risk of saree length distortion.',
                    'severity', 'BLOCK'
                ));
            NEW.weaver_approval_state := 'LOOM_STOPPED_MAINTENANCE_REQUIRED';
        END IF;
    END IF;

    -- Rule 3 (On-Loom Defect Escalation Gate)
    -- IF on_loom_defect_category IN (WEFT_PICK_GAP_LOOM_STOP, BORDER_MISALIGNMENT)
    -- → BLOCK: MANDATORY_INSPECTION_STOP → SET saree_piece_clearance_status = REJECTED_CRITICAL_WEAVE_FAULT
    IF NEW.on_loom_defect_category IN ('WEFT_PICK_GAP_LOOM_STOP', 'BORDER_MISALIGNMENT') THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'MANDATORY_INSPECTION_STOP',
                'message', 'Resolve defect before resuming weaving.',
                'severity', 'BLOCK'
            ));
        NEW.saree_piece_clearance_status := 'REJECTED_CRITICAL_WEAVE_FAULT';
    END IF;

    -- Rule 4 (Saree Cut-Off Clearance Authorization)
    -- IF saree_piece_clearance_status NOT IN (PASSED_GRADE_A_QUALITY, GRADE_B_MINOR_DEFECT_LOGGED)
    -- → BLOCK: DENY_ELECTRONIC_CUT_MARK_APPROVAL
    IF NEW.saree_piece_clearance_status NOT IN ('PASSED_GRADE_A_QUALITY', 'GRADE_B_MINOR_DEFECT_LOGGED') THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_ELECTRONIC_CUT_MARK_APPROVAL',
                'message', 'Saree piece clearance status must be PASSED_GRADE_A_QUALITY or GRADE_B_MINOR_DEFECT_LOGGED for cut-off.',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Daily Production Yield Calculation
    -- Target Output Yards/Hour = (Loom RPM * 60) / (Target PPI * 36) * Efficiency %
    IF NEW.loom_rpm IS NOT NULL AND NEW.target_ppi IS NOT NULL AND NEW.efficiency_percent IS NOT NULL THEN
        NEW.target_output_yards_per_hour := (NEW.loom_rpm * 60.0) / (NEW.target_ppi * 36.0) * (NEW.efficiency_percent / 100.0);
    END IF;

    -- Low Efficiency Alert: IF actual_output_yards < 0.8 * target_output_yards_per_hour
    IF NEW.actual_output_yards IS NOT NULL AND NEW.target_output_yards_per_hour IS NOT NULL THEN
        IF NEW.actual_output_yards < (0.8 * NEW.target_output_yards_per_hour) THEN
            NEW.low_efficiency_alert := TRUE;
        END IF;
    END IF;

    -- Production State Transition: IF first_pick_sample_status = APPROVED_FLAWLESS
    -- → SET status = IN_PROGRESS_WEAVING
    IF NEW.first_pick_sample_status = 'APPROVED_FLAWLESS' THEN
        NEW.status := 'IN_PROGRESS_WEAVING';
    END IF;

    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'WEAVING_QC_HOLD';
        ELSIF NEW.saree_piece_clearance_status IN ('PASSED_GRADE_A_QUALITY', 'GRADE_B_MINOR_DEFECT_LOGGED') THEN
            NEW.auto_assigned_routing := 'READY_FOR_CUT_ROUTE_INSPECTION';
        ELSIF NEW.low_efficiency_alert = TRUE THEN
            NEW.auto_assigned_routing := 'LOW_EFFICIENCY_ALERT';
        ELSE
            NEW.auto_assigned_routing := 'WEAVING_IN_PROGRESS';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_master_weaver_job ON master_weaver_jobs;

CREATE TRIGGER trigger_validate_master_weaver_job
    BEFORE INSERT OR UPDATE ON master_weaver_jobs
    FOR EACH ROW EXECUTE FUNCTION validate_master_weaver_job();

-- ------------------------------------------------------------
-- 6. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_master_weaver_jobs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_master_weaver_jobs_updated_at ON master_weaver_jobs;

CREATE TRIGGER trigger_update_master_weaver_jobs_updated_at
    BEFORE UPDATE ON master_weaver_jobs
    FOR EACH ROW EXECUTE FUNCTION update_master_weaver_jobs_updated_at();

-- ------------------------------------------------------------
-- 7. UPDATE PETNI-MASTER ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","petni:create","petni:read","petni:write","petni:certificate:read","weaving:read","sales:forecast:read"]',
    description = 'Petni Master (Warp Pulling & Reed Denting Specialist): Petni warp transition joint, reed denting, dropper pinning, lease verification, border alignment, structural integrity checks, loom activation release, Petni certification; provides pre-process Petni/reeding to Master Weaver'
WHERE role_id = 'ROLE-PETNI-MASTER';
