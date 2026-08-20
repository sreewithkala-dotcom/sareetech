-- ============================================================
-- Migration: 021_warp_beam_preparation.sql
-- Warp Beam Preparation (80 Saree Length) module
-- Sectional warping, creel tension synchronization, beaming,
-- mass balance accounting, and loom mounting authorization.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE WARP-BEAM-PREPARATION ROLE
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","beam:create","beam:read","beam:write","beam:certificate:read","sales:forecast:read"]',
    description = 'Warp Beam Preparation Specialist (80 Saree Length): sectional warping, creel tension synchronization, beaming-off, mass balance accounting, lease insertion, loom mounting authorization, warp beam certification'
WHERE role_id = 'ROLE-WARP-BEAM-PREPARATION';

-- ------------------------------------------------------------
-- 2. WARP BEAM PRODUCTION LOGS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS warp_beam_production_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    warp_set_id VARCHAR(100) UNIQUE NOT NULL,
    production_lot_id UUID REFERENCES production_lots(id),
    design_master_id UUID REFERENCES design_masters(id),
    card_puncher_job_id UUID REFERENCES programming_jobs(id),
    pirn_winding_job_id UUID REFERENCES pirn_winding_jobs(id),
    bobbin_winder_job_card_id UUID REFERENCES winding_job_cards(id),
    factory_node_id VARCHAR(50) NOT NULL,
    operator_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'PENDING_WARPING' CHECK (status IN (
        'PENDING_WARPING', 'PASSED_APPROVED_FOR_LOOM', 'REJECTED_TENSION_VARIATION',
        'REJECTED_LENGTH_SHORTAGE', 'READY_FOR_LOOM_MOUNTING', 'BEAM_QC_HOLD', 'ARCHIVED'
    )),

    -- Category A: Beam Setup & Sectional Warping Parameters
    target_loom_type VARCHAR(50) CHECK (target_loom_type IN (
        '2400_HOOK_ELECTRONIC_JACQUARD',
        '1536_HOOK_ELECTRONIC_JACQUARD',
        'PLAIN_MECHANICAL_DOBBY'
    )),
    warping_machine_type VARCHAR(50) DEFAULT 'AUTOMATIC_SECTIONAL_WARPER' CHECK (warping_machine_type IN (
        'AUTOMATIC_SECTIONAL_WARPER',
        'MANUAL_SECTIONAL_WARPER',
        'DIRECT_HIGH_SPEED_BEAM_WARPER'
    )),
    total_warp_length_meters DECIMAL(8,2),
    creel_capacity_bobbins INTEGER,
    number_of_sections INTEGER,

    -- Category B: Quality Controls & Tension Audit
    creel_tension_setting_grams DECIMAL(6,2),
    static_control_status VARCHAR(50) DEFAULT 'ACTIVE_IONIZING_BARS_65RH' CHECK (static_control_status IN (
        'ACTIVE_IONIZING_BARS_65RH',
        'PASSIVE_RODS_ONLY',
        'DISABLED'
    )),
    leasing_method_used VARCHAR(50) DEFAULT 'AUTOMATIC_LEASE_REED_1X1_LOCK' CHECK (leasing_method_used IN (
        'AUTOMATIC_LEASE_REED_1X1_LOCK',
        'MANUAL_SPLIT_LEASE',
        'STANDARD_END_TO_END_LEASE'
    )),
    warp_wax_conditioning VARCHAR(50) DEFAULT 'LIQUID_ANTISTATIC_WAX_EMULSION' CHECK (warp_wax_conditioning IN (
        'LIQUID_ANTISTATIC_WAX_EMULSION',
        'DRY_WAX_BAR_APPLICATION',
        'NONE_RAW_SILK'
    )),
    beam_density_shore_d DECIMAL(5,2),

    -- Category C: Final Inspection & System Status
    section_gap_overlap_inspection VARCHAR(50) DEFAULT 'ZERO_GAP_ZERO_OVERLAP' CHECK (section_gap_overlap_inspection IN (
        'ZERO_GAP_ZERO_OVERLAP',
        'SLIGHT_RIDGE_WARNING',
        'SEVERE_GAP_REJECT'
    )),
    broken_ends_repaired_count INTEGER DEFAULT 0,
    warp_beam_approval_state VARCHAR(50) DEFAULT 'PENDING_WARPING' CHECK (warp_beam_approval_state IN (
        'PENDING_WARPING',
        'PASSED_APPROVED_FOR_LOOM',
        'REJECTED_TENSION_VARIATION',
        'REJECTED_LENGTH_SHORTAGE'
    )),

    -- Inventory Consumption & Mass Balance Accounting
    total_allocated_yarn_weight_kg DECIMAL(10,3),
    post_job_returned_yarn_weight_kg DECIMAL(10,3),
    calculated_net_beam_weight_kg DECIMAL(10,3),
    actual_scale_beam_weight_kg DECIMAL(10,3),
    weight_variance_percent DECIMAL(6,3),
    qc_hold_reason TEXT,

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

CREATE INDEX IF NOT EXISTS idx_warp_beam_production_logs_warp_set ON warp_beam_production_logs(warp_set_id);
CREATE INDEX IF NOT EXISTS idx_warp_beam_production_logs_factory ON warp_beam_production_logs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_warp_beam_production_logs_status ON warp_beam_production_logs(status);
CREATE INDEX IF NOT EXISTS idx_warp_beam_production_logs_loom ON warp_beam_production_logs(target_loom_type);

-- ------------------------------------------------------------
-- 3. WARP BEAM CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS warp_beam_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    warp_beam_production_log_id UUID NOT NULL REFERENCES warp_beam_production_logs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    warp_set_id VARCHAR(100) NOT NULL,
    target_loom_type VARCHAR(50),
    warping_machine_type VARCHAR(50),
    total_warp_length_meters DECIMAL(8,2),
    creel_capacity_bobbins INTEGER,
    number_of_sections INTEGER,
    creel_tension_setting_grams DECIMAL(6,2),
    static_control_status VARCHAR(50),
    leasing_method_used VARCHAR(50),
    warp_wax_conditioning VARCHAR(50),
    beam_density_shore_d DECIMAL(5,2),
    section_gap_overlap_inspection VARCHAR(50),
    broken_ends_repaired_count INTEGER,
    warp_beam_approval_state VARCHAR(50),
    total_allocated_yarn_weight_kg DECIMAL(10,3),
    post_job_returned_yarn_weight_kg DECIMAL(10,3),
    calculated_net_beam_weight_kg DECIMAL(10,3),
    actual_scale_beam_weight_kg DECIMAL(10,3),
    weight_variance_percent DECIMAL(6,3),
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

CREATE INDEX IF NOT EXISTS idx_warp_beam_certificates_log ON warp_beam_certificates(warp_beam_production_log_id);
CREATE INDEX IF NOT EXISTS idx_warp_beam_certificates_hash ON warp_beam_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_warp_beam_certificates_qr ON warp_beam_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 4. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_warp_beam_plan (
    id UUID PRIMARY DEFAULT uuid_generate_v4(),
    forecast_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    source_system VARCHAR(50) DEFAULT 'SALES_TEAM_API',
    forecast_period_days INTEGER DEFAULT 30,
    material_plan JSONB DEFAULT '[]'::jsonb,
    upcoming_lots JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sales_forecast_warp_beam_plan_factory ON sales_forecast_warp_beam_plan(factory_node_id);

-- ------------------------------------------------------------
-- 5. TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_warp_beam_production_log()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1 (2400 Hook Long-Warp Tension Gate)
    -- IF target_loom_type = 2400 Hook Electronic Jacquard AND creel_tension_setting_grams >5.5
    -- → BLOCK: HIGH_TENSION_WARNING
    IF NEW.target_loom_type = '2400_HOOK_ELECTRONIC_JACQUARD' THEN
        IF NEW.creel_tension_setting_grams > 5.5 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'HIGH_TENSION_WARNING',
                    'message', '2400 Hook long-warp tension exceeds 5.5g. Excess tension will stretch 80-saree silk warp.',
                    'severity', 'BLOCK'
                ));
        END IF;
    END IF;

    -- Rule 2 (Beam Hardness Clearance Gate)
    -- IF total_warp_length_meters >400.0 AND beam_density_shore_d <75.0
    -- → BLOCK: REJECT_SOFT_BEAM → SET warp_beam_approval_state = REJECTED_TENSION_VARIATION
    IF NEW.total_warp_length_meters > 400.0 THEN
        IF NEW.beam_density_shore_d < 75.0 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'REJECT_SOFT_BEAM',
                    'message', 'Soft beam will collapse under 2400 hook harness tension.',
                    'severity', 'BLOCK'
                ));
            NEW.warp_beam_approval_state := 'REJECTED_TENSION_VARIATION';
        END IF;
    END IF;

    -- Rule 3 (Section Join Inspection Enforcement)
    -- IF section_gap_overlap_inspection = Severe Gap (Reject)
    -- → BLOCK: CANNOT_CLEAR_BEAM_FOR_WEAVING
    IF NEW.section_gap_overlap_inspection = 'SEVERE_GAP_REJECT' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'CANNOT_CLEAR_BEAM_FOR_WEAVING',
                'message', 'Warper marks will ruin all 80 sarees.',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Weight Variance Calculation (Material Discrepancy & Theft Firewall)
    IF NEW.calculated_net_beam_weight_kg IS NOT NULL AND NEW.actual_scale_beam_weight_kg IS NOT NULL THEN
        IF NEW.calculated_net_beam_weight_kg > 0 THEN
            NEW.weight_variance_percent :=
                ((NEW.calculated_net_beam_weight_kg - NEW.actual_scale_beam_weight_kg) / NEW.calculated_net_beam_weight_kg) * 100;
        END IF;

        -- If physical beam weight varies from calculated by > 1%, freeze with BEAM_QC_HOLD
        IF ABS(NEW.weight_variance_percent) > 1.0 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'MATERIAL_DISCREPANCY_DETECTED',
                    'message', format('Weight variance of %s%% exceeds 1%% threshold. Beam frozen for theft/machine calibration check.', ROUND(ABS(NEW.weight_variance_percent), 2)),
                    'severity', 'BLOCK'
                ));
            NEW.status := 'BEAM_QC_HOLD';
            NEW.qc_hold_reason := 'Weight variance exceeds 1% threshold';
        END IF;
    END IF;

    -- Rule 4 (Loom Mounting Approval)
    -- IF warp_beam_approval_state NOT EQUAL TO PASSED_APPROVED_FOR_LOOM
    -- → BLOCK: DENY_LOOM_LOADING_WORK_ORDER
    IF NEW.warp_beam_approval_state != 'PASSED_APPROVED_FOR_LOOM' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_LOOM_LOADING_WORK_ORDER',
                'message', 'Warp beam approval state must be PASSED_APPROVED_FOR_LOOM before loom mounting.',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'BEAM_QC_HOLD';
        ELSIF NEW.warp_beam_approval_state = 'PASSED_APPROVED_FOR_LOOM' THEN
            NEW.auto_assigned_routing := 'READY_FOR_LOOM_MOUNTING';
        ELSE
            NEW.auto_assigned_routing := 'PENDING_WARPING';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_warp_beam_production_log ON warp_beam_production_logs;

CREATE TRIGGER trigger_validate_warp_beam_production_log
    BEFORE INSERT OR UPDATE ON warp_beam_production_logs
    FOR EACH ROW EXECUTE FUNCTION validate_warp_beam_production_log();

-- ------------------------------------------------------------
-- 6. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_warp_beam_production_logs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_warp_beam_production_logs_updated_at ON warp_beam_production_logs;

CREATE TRIGGER trigger_update_warp_beam_production_logs_updated_at
    BEFORE UPDATE ON warp_beam_production_logs
    FOR EACH ROW EXECUTE FUNCTION update_warp_beam_production_logs_updated_at();

-- ------------------------------------------------------------
-- 7. UPDATE CARD-PUNCHER ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","design:write","design:approve","card:program:create","card:program:read","card:program:write","card:certificate:read","beam:read","sales:forecast:read"]',
    description = 'Card Puncher (Digital/E-Jacquard Programmer): CAD-to-CAM file compilation, weave structure code injection, card-punch machine operation, loom controller programming, physical card-lacing & verification, checksum security gate; provides pre-process design/programming to Warp Beam Preparation'
WHERE role_id = 'ROLE-CARD-PUNCHER';
