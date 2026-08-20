-- ============================================================
-- Migration: 026_sup_loom_floor_supervisor.sql
-- SUP Loom Floor Supervisor module: floor operations monitoring,
-- environmental control, quality escapes, root cause audits,
-- first saree approval, batch release authorization, and
-- shift handover clearance.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE ROLES: Expand permissions
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","weaving:read","weaving:supervise","assistant:read","master-weaver:read","petni-master:read","warp-joiner:read","harness-setter:read","sales:forecast:read","environment:read","environment:write","oee:read","oee:write"]',
    description = 'SUP Loom Floor Supervisor (Weaving Shed Supervisor / Jacquard Floor Lead): oversees production, mechanical efficiency, quality compliance, and shift coordination across the weaving floor; monitors OEE, environmental controls, workflow handovers, first saree approval, root cause audits, and batch release authorization'
WHERE role_id = 'ROLE-SUP-LOOM-FLOOR-SUPERVISOR';

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:weaving","assistant:log:write","assistant:wage:read","assistant:certificate:read","sales:forecast:read","supervisor:read"]',
    description = 'Assistant Weaver (Junior Loom Operator & Bobbin/Shuttle Specialist): continuous pirn and bobbin feeding, broken end piecing, selvedge and border tension management, temple advancement and fabric let-off, loom cleanliness and maintenance support, shift handover, assistant weaver certification; provides floor support to Master Weaver and reports to SUP Loom Floor Supervisor'
WHERE role_id = 'ROLE-ASSISTANT-WEAVER';

-- ------------------------------------------------------------
-- 2. SUP LOOM FLOOR SUPERVISOR LOGS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sup_loom_floor_supervisor_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    supervisor_log_id VARCHAR(100) UNIQUE NOT NULL,
    loom_shed_line_id VARCHAR(100) NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    supervisor_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    assistant_weaver_job_log_id UUID REFERENCES assistant_weaver_job_logs(id),
    master_weaver_job_id UUID REFERENCES master_weaver_jobs(id),
    petni_master_job_id UUID REFERENCES petni_master_jobs(id),
    warp_joining_job_id UUID REFERENCES warp_joining_jobs(id),
    harness_setup_log_id UUID REFERENCES harness_setup_logs(id),
    shift_start_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    shift_end_time TIMESTAMP WITH TIME ZONE,

    -- Category A: Floor Operations & Environmental Monitoring
    ambient_relative_humidity_pct DECIMAL(5,2),
    ambient_temperature_celsius DECIMAL(5,2),
    shift_target_oee_pct DECIMAL(5,2) DEFAULT 85.0,
    gaiting_handover_status VARCHAR(50) DEFAULT 'ALL_PRE_WEAVE_CHECKS_PASSED' CHECK (gaiting_handover_status IN (
        'ALL_PRE_WEAVE_CHECKS_PASSED',
        'HARNESS_SETTER_HOLD',
        'WARP_JOINER_HOLD',
        'PETNI_MASTER_HOLD'
    )),

    -- Category B: Quality Escapes & Root Cause Audits
    loom_stop_rate_per_hour DECIMAL(6,2),
    primary_stop_root_cause VARCHAR(50) DEFAULT 'NONE_NORMAL_RUNNING' CHECK (primary_stop_root_cause IN (
        'NONE_NORMAL_RUNNING',
        'WARP_END_ABRASION_STATIC',
        'WEFT_PIRN_DELAYS',
        'ELECTRONIC_JACQUARD_SOLENOID_FAULT',
        'HUMIDITY_OUT_OF_SPEC'
    )),
    first_saree_dimensional_audit VARCHAR(50) DEFAULT 'APPROVED_FULL_SPEC_MATCH' CHECK (first_saree_dimensional_audit IN (
        'APPROVED_FULL_SPEC_MATCH',
        'REJECTED_PALLU_LENGTH_ERROR',
        'REJECTED_BORDER_MISALIGNMENT',
        'REJECTED_DENSITY_VARIANCE'
    )),
    waste_percentage_current_run DECIMAL(5,2),

    -- Category C: Executive Clearance & Shift Sign-Off
    shift_handover_approval_state VARCHAR(50) DEFAULT 'SHIFT_ACTIVE_NORMAL' CHECK (shift_handover_approval_state IN (
        'SHIFT_ACTIVE_NORMAL',
        'PASSED_SHIFT_TARGETS_MET',
        'LINE_HALTED_QUALITY_INVESTIGATION',
        'REJECTED_ENVIRONMENTAL_OUT_OF_SPEC'
    )),
    saree_batch_release_authorization VARCHAR(50) DEFAULT 'APPROVED_FOR_FINISHING' CHECK (saree_batch_release_authorization IN (
        'APPROVED_FOR_FINISHING',
        'HOLD_PENDING_QA_REVIEW',
        'REJECTED_DEFECTIVE_BATCH'
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

CREATE INDEX IF NOT EXISTS idx_sup_supervisor_logs_line ON sup_loom_floor_supervisor_logs(loom_shed_line_id);
CREATE INDEX IF NOT EXISTS idx_sup_supervisor_logs_supervisor ON sup_loom_floor_supervisor_logs(supervisor_id);
CREATE INDEX IF NOT EXISTS idx_sup_supervisor_logs_factory ON sup_loom_floor_supervisor_logs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_sup_supervisor_logs_shift ON sup_loom_floor_supervisor_logs(shift_start_time);
CREATE INDEX IF NOT EXISTS idx_sup_supervisor_logs_assistant ON sup_loom_floor_supervisor_logs(assistant_weaver_job_log_id);

-- ------------------------------------------------------------
-- 3. SUP LOOM FLOOR SUPERVISOR CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sup_loom_floor_supervisor_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    supervisor_log_id UUID NOT NULL REFERENCES sup_loom_floor_supervisor_logs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    supervisor_log_id_ref VARCHAR(100) NOT NULL,
    loom_shed_line_id VARCHAR(100) NOT NULL,
    assistant_weaver_job_log_id UUID REFERENCES assistant_weaver_job_logs(id),
    master_weaver_job_id UUID REFERENCES master_weaver_jobs(id),
    ambient_relative_humidity_pct DECIMAL(5,2),
    ambient_temperature_celsius DECIMAL(5,2),
    shift_target_oee_pct DECIMAL(5,2),
    gaiting_handover_status VARCHAR(50),
    loom_stop_rate_per_hour DECIMAL(6,2),
    primary_stop_root_cause VARCHAR(50),
    first_saree_dimensional_audit VARCHAR(50),
    waste_percentage_current_run DECIMAL(5,2),
    shift_handover_approval_state VARCHAR(50),
    saree_batch_release_authorization VARCHAR(50),
    supervisor_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    certification_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'REVOKED', 'EXPIRED', 'PENDING'
    )),
    certified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sup_supervisor_certificates_log ON sup_loom_floor_supervisor_certificates(supervisor_log_id);
CREATE INDEX IF NOT EXISTS idx_sup_supervisor_certificates_hash ON sup_loom_floor_supervisor_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_sup_supervisor_certificates_qr ON sup_loom_floor_supervisor_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 4. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_sup_loom_floor_supervisor_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_sup_supervisor_plan_factory ON sales_forecast_sup_loom_floor_supervisor_plan(factory_node_id);

-- ------------------------------------------------------------
-- 5. TRIGGERS: Validation and automation
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_sup_loom_floor_supervisor_log()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1 (Environmental Humidity Interlock Guardrail)
    -- IF ambient_relative_humidity_pct < 62.0% OR ambient_relative_humidity_pct > 73.0%
    -- → BLOCK: ENVIRONMENT_OUT_OF_SPEC_WARNING
    -- → SET shift_handover_approval_state = REJECTED_ENVIRONMENTAL_OUT_OF_SPEC
    IF NEW.ambient_relative_humidity_pct IS NOT NULL THEN
        IF NEW.ambient_relative_humidity_pct < 62.0 OR NEW.ambient_relative_humidity_pct > 73.0 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'ENVIRONMENT_OUT_OF_SPEC_WARNING',
                    'message', 'STATIC_AND_WARP_BREAK_RISK_FOR_HIGH_DENSITY_SILK',
                    'severity', 'BLOCK'
                ));
            NEW.shift_handover_approval_state := 'REJECTED_ENVIRONMENTAL_OUT_OF_SPEC';
        END IF;
    END IF;

    -- Rule 2 (First Saree Quality Gate)
    -- IF first_saree_dimensional_audit NOT EQUAL TO APPROVED_FULL_SPEC_MATCH
    -- → BLOCK: HALT_LOOM_RUN
    IF NEW.first_saree_dimensional_audit IS NOT NULL AND NEW.first_saree_dimensional_audit != 'APPROVED_FULL_SPEC_MATCH' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'HALT_LOOM_RUN',
                'message', 'CANNOT_PROCEED_WITH_REMAINING_79_SAREES_WITHOUT_FIRST_SAREE_APPROVAL',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Rule 3 (High Loom Stop Rate Trigger)
    -- IF loom_stop_rate_per_hour > 2.5
    -- → TRIGGER: AUTO_MAINTENANCE_TICKET
    -- → SET primary_stop_root_cause = WARP_END_ABRASION_STATIC
    IF NEW.loom_stop_rate_per_hour IS NOT NULL AND NEW.loom_stop_rate_per_hour > 2.5 THEN
        NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'AUTO_MAINTENANCE_TICKET',
                'message', 'EXCESSIVE_STOP_RATE_REQUIRES_SUPERVISOR_INTERVENTION',
                'severity', 'WARNING'
            ));
        NEW.primary_stop_root_cause := 'WARP_END_ABRASION_STATIC';
    END IF;

    -- Rule 4 (Batch Release Authorization Gate)
    -- IF saree_batch_release_authorization NOT EQUAL TO APPROVED_FOR_FINISHING
    -- → BLOCK: DENY_CUT_SAREE_TRANSFER_TO_FINISHING_WAREHOUSE
    IF NEW.saree_batch_release_authorization IS NOT NULL AND NEW.saree_batch_release_authorization != 'APPROVED_FOR_FINISHING' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_CUT_SAREE_TRANSFER_TO_FINISHING_WAREHOUSE',
                'message', 'BATCH_RELEASE_AUTHORIZATION_REQUIRED',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'SUPERVISOR_QC_HOLD';
        ELSIF NEW.shift_handover_approval_state IN ('PASSED_SHIFT_TARGETS_MET', 'SHIFT_ACTIVE_NORMAL') THEN
            NEW.auto_assigned_routing := 'READY_FOR_BATCH_RELEASE';
        ELSE
            NEW.auto_assigned_routing := 'ACTIVE_SUPERVISION';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_sup_loom_floor_supervisor_log ON sup_loom_floor_supervisor_logs;

CREATE TRIGGER trigger_validate_sup_loom_floor_supervisor_log
    BEFORE INSERT OR UPDATE ON sup_loom_floor_supervisor_logs
    FOR EACH ROW EXECUTE FUNCTION validate_sup_loom_floor_supervisor_log();

-- ------------------------------------------------------------
-- 6. TRIGGER: Auto-generate certificate on approval
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION generate_sup_supervisor_certificate()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.shift_handover_approval_state IN ('PASSED_SHIFT_TARGETS_MET', 'SHIFT_ACTIVE_NORMAL')
       AND OLD.shift_handover_approval_state IS DISTINCT FROM NEW.shift_handover_approval_state THEN
        NEW.certificate_hash := generate_certificate_hash(NEW.id, NEW.supervisor_log_id);
        NEW.qr_tag_id := 'SUP-SUPERVISOR-' || NEW.supervisor_log_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_generate_sup_supervisor_certificate ON sup_loom_floor_supervisor_logs;

CREATE TRIGGER trigger_generate_sup_supervisor_certificate
    BEFORE UPDATE OF shift_handover_approval_state ON sup_loom_floor_supervisor_logs
    FOR EACH ROW EXECUTE FUNCTION generate_sup_supervisor_certificate();

-- ------------------------------------------------------------
-- 7. TRIGGER: Update timestamps
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_sup_supervisor_logs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_sup_supervisor_logs_updated_at ON sup_loom_floor_supervisor_logs;

CREATE TRIGGER trigger_update_sup_supervisor_logs_updated_at
    BEFORE UPDATE ON sup_loom_floor_supervisor_logs
    FOR EACH ROW EXECUTE FUNCTION update_sup_supervisor_logs_updated_at();
