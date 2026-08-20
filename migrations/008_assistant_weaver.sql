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
    assistant_weaver_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    lead_weaver_id UUID REFERENCES users(id) ON DELETE SET NULL,
    factory_node_id VARCHAR(50) NOT NULL,
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
-- 5. UPDATE ROLES: Expand Assistant Weaver permissions
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:weaving","assistant:log:write","assistant:wage:read"]',
    description = 'Support lead weaver, manage pirns/bobbins, mend warp/weft breaks, temples, housekeeping, shift handover'
WHERE role_id = 'ROLE-ASSISTANT-WEAVER';

-- ------------------------------------------------------------
-- 6. TRIGGERS: Auto-calculate wage shares on completion
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
-- 7. TRIGGER: Update timestamps
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
-- 8. loom_assignments assistant_weaver_id extension
-- ------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'loom_assignments' AND column_name = 'assistant_weaver_id'
    ) THEN
        ALTER TABLE loom_assignments ADD COLUMN assistant_weaver_id UUID REFERENCES users(id);
    END IF;
END $$;
