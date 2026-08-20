-- ============================================================
-- Migration: 008_assistant_weaver.sql
-- Assistant Weaver module: job logs, wage splits, guardrails,
-- and loom-level operational tracking for assistant/junior weavers.
-- ============================================================

-- ------------------------------------------------------------
-- 1. ASSISTANT WEAVER JOB LOGS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS assistant_weaver_job_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    assistant_job_log_id VARCHAR(100) UNIQUE NOT NULL,
    active_loom_id VARCHAR(100) NOT NULL,
    production_lot_id UUID REFERENCES production_lots(id),
    design_master_id UUID REFERENCES design_masters(id),
    master_weaver_job_id UUID REFERENCES master_weaver_jobs(id),
    petni_master_job_id UUID REFERENCES petni_master_jobs(id),
    warp_joining_job_id UUID REFERENCES warp_joining_jobs(id),
    harness_setup_log_id UUID REFERENCES harness_setup_logs(id),
    warp_beam_production_log_id UUID REFERENCES warp_beam_production_logs(id),
    card_puncher_job_id UUID REFERENCES programming_jobs(id),
    pirn_winding_job_id UUID REFERENCES pirn_winding_jobs(id),
    bobbin_winder_job_card_id UUID REFERENCES winding_job_cards(id),
    assistant_weaver_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    lead_weaver_id UUID REFERENCES users(id) ON DELETE SET NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    approver_id UUID REFERENCES users(id),
    shift_start_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    shift_end_time TIMESTAMP WITH TIME ZONE,
    pirns_replaced_count INTEGER DEFAULT 0,
    logged_warp_breaks INTEGER DEFAULT 0,
    logged_weft_breaks INTEGER DEFAULT 0,
    lead_weaver_signoff BOOLEAN DEFAULT FALSE,
    signoff_at TIMESTAMP WITH TIME ZONE,
    approval_state VARCHAR(50) DEFAULT 'ACTIVE_LOGGING' CHECK (approval_state IN (
        'ACTIVE_LOGGING', 'PASSED_SHIFT_AUDIT', 'REJECTED_UNRESOLVED_WARP_BREAKS'
    )),

    -- Category A: Weft material creeling
    weft_spool_lot_id VARCHAR(100),
    weft_feeder_position VARCHAR(50) DEFAULT 'Feeder 1 (Ground Silk)' CHECK (weft_feeder_position IN (
        'Feeder 1 (Ground Silk)',
        'Feeder 2 (Zari Extra Weft)',
        'Feeder 3 (Contrast Border Silk)',
        'Feeder 4 (Secondary Zari)'
    )),
    yarn_tail_transfer_status VARCHAR(100) DEFAULT 'Spliced & Tail-Locked' CHECK (yarn_tail_transfer_status IN (
        'Spliced & Tail-Locked',
        'Single Spool (No Reserve)',
        'Unverified / Loose Tail'
    )),
    zari_tension_disc_setting VARCHAR(100) DEFAULT 'Micro-Tension Active (Fine Zari)' CHECK (zari_tension_disc_setting IN (
        'Micro-Tension Active (Fine Zari)',
        'Standard Friction',
        'Low-Tension Light',
        'Bypassed'
    )),

    -- Category B: Warp repair & maintenance
    warp_break_repair_count INTEGER DEFAULT 0,
    mending_knot_type VARCHAR(100) DEFAULT 'Weaver''s Micro-Knot (Short Tail)' CHECK (mending_knot_type IN (
        'Weaver''s Micro-Knot (Short Tail)',
        'Standard Overhand Knot',
        'Spliced Loop'
    )),
    dropper_rethread_verification VARCHAR(100) DEFAULT 'Threaded & Dropper Active' CHECK (dropper_rethread_verification IN (
        'Threaded & Dropper Active',
        'Bypassed Dropper (Unsafe)',
        'Missing Dropper'
    )),
    comber_board_cleaning_status VARCHAR(100) DEFAULT 'Cleaned / Compressed Air Blowout Done' CHECK (comber_board_cleaning_status IN (
        'Cleaned / Compressed Air Blowout Done',
        'Pending Cleaning',
        'Heavy Fly Buildup'
    )),

    -- Category C: Shift status
    shift_handover_readiness VARCHAR(50) DEFAULT 'READY_FOR_NEXT_SHIFT' CHECK (shift_handover_readiness IN (
        'READY_FOR_NEXT_SHIFT',
        'PENDING_WARP_BREAK_REPAIR',
        'LOW_WEFT_RESERVE_WARNING'
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

CREATE INDEX IF NOT EXISTS idx_assistant_job_logs_loom ON assistant_weaver_job_logs(active_loom_id);
CREATE INDEX IF NOT EXISTS idx_assistant_job_logs_assistant ON assistant_weaver_job_logs(assistant_weaver_id);
CREATE INDEX IF NOT EXISTS idx_assistant_job_logs_lead ON assistant_weaver_job_logs(lead_weaver_id);
CREATE INDEX IF NOT EXISTS idx_assistant_job_logs_factory ON assistant_weaver_job_logs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_assistant_job_logs_shift ON assistant_weaver_job_logs(shift_start_time);

-- ------------------------------------------------------------
-- 2. WAGE DISTRIBUTION / PIECE-RATE SPLITS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS wage_distributions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_log_id UUID NOT NULL REFERENCES assistant_weaver_job_logs(id) ON DELETE CASCADE,
    loom_id VARCHAR(100) NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    lead_weaver_id UUID REFERENCES users(id),
    assistant_weaver_id UUID REFERENCES users(id),
    lot_id UUID REFERENCES production_lots(id),
    total_piece_rate_inr DECIMAL(10,2) NOT NULL,
    lead_share_percent DECIMAL(5,2) DEFAULT 75.0,
    assistant_share_percent DECIMAL(5,2) DEFAULT 25.0,
    lead_share_inr DECIMAL(10,2) NOT NULL,
    assistant_share_inr DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'INR',
    distribution_status VARCHAR(20) DEFAULT 'PENDING' CHECK (distribution_status IN (
        'PENDING', 'PROCESSED', 'PAID', 'FAILED', 'REVERSED'
    )),
    processed_by UUID REFERENCES users(id),
    processed_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_wage_distributions_job ON wage_distributions(job_log_id);
CREATE INDEX IF NOT EXISTS idx_wage_distributions_loom ON wage_distributions(loom_id);
CREATE INDEX IF NOT EXISTS idx_wage_distributions_lot ON wage_distributions(lot_id);

-- ------------------------------------------------------------
-- 3. LOOM BREAKAGE ALARMS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS loom_breakage_alarms (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    loom_id VARCHAR(100) NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    alarm_type VARCHAR(50) DEFAULT 'HIGH_BREAKAGE_RATE' CHECK (alarm_type IN (
        'HIGH_BREAKAGE_RATE',
        'STRUCTURAL_ANOMALY',
        'HARNESS_MALFUNCTION',
        'DROPPER_FAILURE'
    )),
    breakage_rate_per_hour DECIMAL(6,2),
    threshold_per_hour DECIMAL(6,2) DEFAULT 3.0,
    severity VARCHAR(20) DEFAULT 'WARNING' CHECK (severity IN (
        'INFO', 'WARNING', 'CRITICAL'
    )),
    message TEXT,
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by UUID REFERENCES users(id),
    acknowledged_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_loom_breakage_alarms_loom ON loom_breakage_alarms(loom_id);
CREATE INDEX IF NOT EXISTS idx_loom_breakage_alarms_factory ON loom_breakage_alarms(factory_node_id);

-- ------------------------------------------------------------
-- 4. ASSISTANT WEAVER SHIFT AUDIT TRAIL
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS assistant_weaver_shift_audits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_log_id UUID NOT NULL REFERENCES assistant_weaver_job_logs(id) ON DELETE CASCADE,
    audit_action VARCHAR(50) NOT NULL CHECK (audit_action IN (
        'SUBMITTED', 'LEAD_SIGNOFF', 'REJECTED', 'REOPENED', 'WAGE_DISTRIBUTED'
    )),
    performed_by UUID REFERENCES users(id),
    notes TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_assistant_shift_audits_job ON assistant_weaver_shift_audits(job_log_id);

-- ------------------------------------------------------------
-- 5. ASSISTANT WEAVER CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS assistant_weaver_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    assistant_weaver_job_log_id UUID NOT NULL REFERENCES assistant_weaver_job_logs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    assistant_job_log_id VARCHAR(100) NOT NULL,
    active_loom_id VARCHAR(100) NOT NULL,
    master_weaver_job_id UUID REFERENCES master_weaver_jobs(id),
    pirns_replaced_count INTEGER,
    logged_warp_breaks INTEGER,
    logged_weft_breaks INTEGER,
    warp_break_repair_count INTEGER,
    weft_feeder_position VARCHAR(50),
    yarn_tail_transfer_status VARCHAR(100),
    zari_tension_disc_setting VARCHAR(100),
    mending_knot_type VARCHAR(100),
    dropper_rethread_verification VARCHAR(100),
    comber_board_cleaning_status VARCHAR(100),
    shift_handover_readiness VARCHAR(50),
    assistant_weaver_approval_state VARCHAR(50),
    lead_weaver_signoff BOOLEAN,
    auto_assigned_routing VARCHAR(50) NOT NULL,
    assistant_weaver_id UUID REFERENCES users(id),
    lead_weaver_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    certification_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'REVOKED', 'EXPIRED', 'PENDING'
    )),
    certified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_assistant_weaver_certificates_log ON assistant_weaver_certificates(assistant_weaver_job_log_id);
CREATE INDEX IF NOT EXISTS idx_assistant_weaver_certificates_hash ON assistant_weaver_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_assistant_weaver_certificates_qr ON assistant_weaver_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 6. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_assistant_weaver_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_assistant_weaver_plan_factory ON sales_forecast_assistant_weaver_plan(factory_node_id);

-- ------------------------------------------------------------
-- 7. TRIGGERS: Validation and automation
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_assistant_weaver_job()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1 (Mending Knot Clearance Guardrail)
    -- IF mending_knot_type = Standard Overhand Knot AND comber_board_cleaning_status = Cleaned / Compressed Air Blowout Done (On 2400 Hook Line)
    -- → BLOCK: INVALID_KNOT_TYPE_WARNING
    IF NEW.mending_knot_type = 'Standard Overhand Knot' THEN
        IF NEW.comber_board_cleaning_status = 'Cleaned / Compressed Air Blowout Done' THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'INVALID_KNOT_TYPE_WARNING',
                    'message', 'Standard knots will catch in 2400 hook reed dents.',
                    'severity', 'BLOCK'
                ));
        END IF;
    END IF;

    -- Rule 2 (Tail-Lock Creel Verification Gate)
    -- IF yarn_tail_transfer_status = Unverified / Loose Tail AND weft_feeder_position = Feeder 2 (Zari Extra Weft)
    -- → BLOCK: REJECT_LOOSE_TAIL
    IF NEW.yarn_tail_transfer_status = 'Unverified / Loose Tail' THEN
        IF NEW.weft_feeder_position = 'Feeder 2 (Zari Extra Weft)' THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'REJECT_LOOSE_TAIL',
                    'message', 'Loose Zari tails will cause weft stops during Pallu weaving.',
                    'severity', 'BLOCK'
                ));
        END IF;
    END IF;

    -- Rule 3 (Bypassed Dropper Prevention Gate)
    -- IF dropper_rethread_verification = Bypassed Dropper (Unsafe)
    -- → BLOCK: SAFETY_VIOLATION → SET approval_state = REJECTED_UNRESOLVED_WARP_BREAKS
    IF NEW.dropper_rethread_verification = 'Bypassed Dropper (Unsafe)' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'SAFETY_VIOLATION',
                'message', 'All warp ends must be threaded through active droppers.',
                'severity', 'BLOCK'
            ));
        NEW.approval_state := 'REJECTED_UNRESOLVED_WARP_BREAKS';
    END IF;

    -- Rule 4 (Shift Handover Clearance Authorization)
    -- IF approval_state NOT EQUAL TO PASSED_SHIFT_AUDIT
    -- → BLOCK: DENY_SHIFT_HANDOVER_CLEARANCE
    IF NEW.approval_state != 'PASSED_SHIFT_AUDIT' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_SHIFT_HANDOVER_CLEARANCE',
                'message', 'Assistant weaver approval state must be PASSED_SHIFT_AUDIT for handover.',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Loom Friction & Breakage Alarm
    -- IF logged_warp_breaks / shift_hours > 3.0
    -- → SET breakage alarm
    IF NEW.logged_warp_breaks IS NOT NULL AND NEW.shift_start_time IS NOT NULL AND NEW.shift_end_time IS NOT NULL THEN
        DECLARE
            shift_hours DECIMAL;
        BEGIN
            shift_hours := EXTRACT(EPOCH FROM (NEW.shift_end_time - NEW.shift_start_time)) / 3600.0;
            IF shift_hours > 0 AND (NEW.logged_warp_breaks / shift_hours) > 3.0 THEN
                NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) ||
                    jsonb_build_array(jsonb_build_object(
                        'code', 'HIGH_BREAKAGE_RATE',
                        'message', format('Warp breakage rate of %s breaks/hour exceeds threshold of 3.0/hour on loom %s.', ROUND(NEW.logged_warp_breaks / shift_hours, 2), NEW.active_loom_id),
                        'severity', 'WARNING'
                    ));
            END IF;
        END;
    END IF;

    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'ASSISTANT_QC_HOLD';
        ELSIF NEW.approval_state = 'PASSED_SHIFT_AUDIT' THEN
            NEW.auto_assigned_routing := 'READY_FOR_WAGE_DISTRIBUTION';
        ELSE
            NEW.auto_assigned_routing := 'ACTIVE_LOGGING';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_assistant_weaver_job ON assistant_weaver_job_logs;

CREATE TRIGGER trigger_validate_assistant_weaver_job
    BEFORE INSERT OR UPDATE ON assistant_weaver_job_logs
    FOR EACH ROW EXECUTE FUNCTION validate_assistant_weaver_job();

-- ------------------------------------------------------------
-- 8. TRIGGER: Auto-calculate wage shares on completion
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION calculate_assistant_wage_split()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.approval_state = 'PASSED_SHIFT_AUDIT' AND OLD.approval_state IS DISTINCT FROM 'PASSED_SHIFT_AUDIT' THEN
        INSERT INTO wage_distributions (
            job_log_id,
            loom_id,
            factory_node_id,
            lead_weaver_id,
            assistant_weaver_id,
            lot_id,
            total_piece_rate_inr,
            lead_share_inr,
            assistant_share_inr,
            distribution_status
        )
        SELECT
            NEW.id,
            NEW.active_loom_id,
            NEW.factory_node_id,
            NEW.lead_weaver_id,
            NEW.assistant_weaver_id,
            NULL,
            0.0,
            0.0,
            0.0,
            'PENDING'
        WHERE NOT EXISTS (
            SELECT 1 FROM wage_distributions WHERE job_log_id = NEW.id
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_calculate_assistant_wage_split ON assistant_weaver_job_logs;

CREATE TRIGGER trigger_calculate_assistant_wage_split
    AFTER UPDATE OF approval_state ON assistant_weaver_job_logs
    FOR EACH ROW EXECUTE FUNCTION calculate_assistant_wage_split();

-- ------------------------------------------------------------
-- 9. TRIGGER: Update timestamps
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_assistant_job_logs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_assistant_job_logs_updated_at ON assistant_weaver_job_logs;

CREATE TRIGGER trigger_update_assistant_job_logs_updated_at
    BEFORE UPDATE ON assistant_weaver_job_logs
    FOR EACH ROW EXECUTE FUNCTION update_assistant_job_logs_updated_at();

-- ------------------------------------------------------------
-- 10. UPDATE ROLES: Expand Assistant Weaver permissions
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:weaving","assistant:log:write","assistant:wage:read","assistant:certificate:read","sales:forecast:read"]',
    description = 'Assistant Weaver (Junior Loom Operator & Bobbin/Shuttle Specialist): continuous pirn and bobbin feeding, broken end piecing, selvedge and border tension management, temple advancement and fabric let-off, loom cleanliness and maintenance support, shift handover, assistant weaver certification; provides floor support to Master Weaver'
WHERE role_id = 'ROLE-ASSISTANT-WEAVER';

-- ------------------------------------------------------------
-- 11. UPDATE MASTER-WEAVER ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","weaving:create","weaving:read","weaving:write","weaving:certificate:read","assistant:read","sales:forecast:read"]',
    description = 'Master Weaver (Loom Operator & Saree Production Specialist): loom operation, real-time quality troubleshooting, weaver skill-to-loom matching, material allocation, yield calculation, first-pick verification, saree completion clearance, weaving certification; oversees Assistant Weaver floor operations'
WHERE role_id = 'ROLE-MASTER-WEAVER';
