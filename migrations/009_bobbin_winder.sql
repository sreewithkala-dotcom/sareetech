-- ============================================================
-- Migration: 009_bobbin_winder.sql
-- Bobbin Winder module: winding job cards, bobbin records,
-- waste guardrails, stock routing, and certificate linkage.
-- ============================================================

-- ------------------------------------------------------------
-- 1. WINDING JOB CARDS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS winding_job_cards (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    winding_job_card_id VARCHAR(100) UNIQUE NOT NULL,
    operator_employee_id UUID REFERENCES users(id),
    spindle_machine_id VARCHAR(100) NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    input_dyed_lot_no VARCHAR(100) NOT NULL,
    yarn_type VARCHAR(50) NOT NULL CHECK (yarn_type IN (
        'WARP_ORGANZINE_HIGH_TWIST',
        'WEFT_TRAM_LOW_TWIST',
        'CREPE_ULTRA_TWIST',
        'DUPION_SLUB_YARN'
    )),
    carrier_destination_type VARCHAR(50) NOT NULL CHECK (carrier_destination_type IN (
        'Flanged Bobbin',
        'Paper Cone',
        'Plastic Spool',
        'Pirn (for Shuttle Filling)'
    )),
    allocated_input_weight_kg DECIMAL(10,3) NOT NULL,
    output_wound_weight_kg DECIMAL(10,3) NOT NULL,
    winding_scrap_waste_gm DECIMAL(10,3) NOT NULL DEFAULT 0,
    process_variance_kg DECIMAL(10,3) GENERATED ALWAYS AS (
        allocated_input_weight_kg - (output_wound_weight_kg + (winding_scrap_waste_gm / 1000.0))
    ) STORED,
    waste_variance_percent DECIMAL(5,2) GENERATED ALWAYS AS (
        CASE 
            WHEN allocated_input_weight_kg > 0 
            THEN ROUND((winding_scrap_waste_gm / 1000.0) / allocated_input_weight_kg * 100, 2)
            ELSE 0 
        END
    ) STORED,
    -- Element 1
    winding_operation_type VARCHAR(50) DEFAULT 'ROUTINE_PRODUCTION' CHECK (winding_operation_type IN (
        'ROUTINE_PRODUCTION',
        'SAMPLE_CONING',
        'RE_WINDING_CORRECTION',
        'CLEANING_RUN'
    )),
    winding_machine_type VARCHAR(50) NOT NULL CHECK (winding_machine_type IN (
        'HIGH_SPEED_AUTOMATIC_CONER',
        'SEMI_AUTOMATIC_BOBBIN_WINDER',
        'TRADITIONAL_HAND_WINDER'
    )),
    worker_attendance_shift_code VARCHAR(20) DEFAULT 'SHIFT_A_MORNING' CHECK (worker_attendance_shift_code IN (
        'SHIFT_A_MORNING',
        'SHIFT_B_EVENING',
        'SHIFT_C_NIGHT'
    )),
    -- Element 2
    yarn_processing_profile VARCHAR(50) NOT NULL CHECK (yarn_processing_profile IN (
        'WARP_ORGANZINE_HIGH_TWIST',
        'WEFT_TRAM_LOW_TWIST',
        'CREPE_ULTRA_TWIST',
        'DUPION_SLUB_YARN'
    )),
    silk_fiber_variety VARCHAR(50) NOT NULL CHECK (silk_fiber_variety IN (
        'PURE_MULBERRY_SILK',
        'ORGANIC_TUSSAR_WILD',
        'MATTE_ERI_SPUN',
        'SHIMMERING_MUGA'
    )),
    -- Element 3
    target_output_carrier_type VARCHAR(50) NOT NULL CHECK (target_output_carrier_type IN (
        'FLANGED_PLASTIC_BOBBIN',
        'TAPERED_PAPER_CONE',
        'CYLINDRICAL_PLASTIC_CHEESE',
        'WOODEN_HANK_SWIFT_SPOOL'
    )),
    bobbin_traverse_length_config VARCHAR(50) NOT NULL CHECK (bobbin_traverse_length_config IN (
        'TRAVERSE_4_INCH',
        'TRAVERSE_6_INCH',
        'TRAVERSE_8_INCH_JUMBO'
    )),
    -- Element 4
    knot_mechanical_join_profiling VARCHAR(50) NOT NULL CHECK (knot_mechanical_join_profiling IN (
        'STANDARD_WEAVERS_KNOT',
        'AUTOMATED_AIR_SPLICED_JOIN',
        'FISHERMANS_KNOT',
        'ILLEGAL_OVERHAND_KNOT'
    )),
    bobbin_structural_build_verdict VARCHAR(50) NOT NULL CHECK (bobbin_structural_build_verdict IN (
        'OPTIMAL_CROSS_WOUND',
        'SOFT_BUILD_COLLAPSE',
        'HARD_BUILD_STRETCHED',
        'RIDGED_SHOULDER_TRAP'
    )),
    bobbin_hardness_shore_d DECIMAL(4,1),
    splice_count_per_bobbin INTEGER DEFAULT 0,
    bobbin_flange_trapping_found BOOLEAN DEFAULT FALSE,
    yarn_break_rate_per_1000m DECIMAL(6,2),
    -- Element 5 / Status
    inventory_output_routing_allocation VARCHAR(50) DEFAULT 'BOBBIN_CLEARED_FOR_WARPING' CHECK (inventory_output_routing_allocation IN (
        'BOBBIN_CLEARED_FOR_WARPING',
        'BOBBIN_CLEARED_FOR_PIRN_WEFT',
        'WINDING_REJECT_RE_RUN'
    )),
    -- Machine profile
    winding_speed_mpm INTEGER,
    applied_tension_grams DECIMAL(5,2),
    -- Validation metadata
    validation_errors JSONB DEFAULT '[]'::jsonb,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'COMPLETED', 'REJECTED', 'REWORK', 'CERTIFIED'
    )),
    certificate_hash VARCHAR(255),
    qr_tag_id VARCHAR(100) UNIQUE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_winding_job_cards_operator ON winding_job_cards(operator_employee_id);
CREATE INDEX IF NOT EXISTS idx_winding_job_cards_machine ON winding_job_cards(spindle_machine_id);
CREATE INDEX IF NOT EXISTS idx_winding_job_cards_factory ON winding_job_cards(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_winding_job_cards_dyed_lot ON winding_job_cards(input_dyed_lot_no);
CREATE INDEX IF NOT EXISTS idx_winding_job_cards_status ON winding_job_cards(status);

-- ------------------------------------------------------------
-- 2. BOBBIN RECORDS (Output carriers produced from a job card)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bobbin_records (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bobbin_id VARCHAR(100) UNIQUE NOT NULL,
    job_card_id UUID NOT NULL REFERENCES winding_job_cards(id) ON DELETE CASCADE,
    operator_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    carrier_type VARCHAR(50) NOT NULL CHECK (carrier_type IN (
        'Flanged Bobbin',
        'Paper Cone',
        'Plastic Spool',
        'Pirn (for Shuttle Filling)'
    )),
    yarn_type VARCHAR(50) NOT NULL,
    silk_fiber_variety VARCHAR(50) NOT NULL,
    input_dyed_lot_no VARCHAR(100) NOT NULL,
    net_weight_kg DECIMAL(10,3) NOT NULL,
    traverse_length_config VARCHAR(50),
    knot_method VARCHAR(50),
    structural_verdict VARCHAR(50),
    status VARCHAR(50) DEFAULT 'READY_FOR_WARPING' CHECK (status IN (
        'READY_FOR_WARPING',
        'READY_FOR_PIRN_WEFT',
        'REJECTED_RE_RUN',
        'CONSUMED'
    )),
    qr_tag_id VARCHAR(100) UNIQUE,
    certificate_hash VARCHAR(255),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bobbin_records_job ON bobbin_records(job_card_id);
CREATE INDEX IF NOT EXISTS idx_bobbin_records_operator ON bobbin_records(operator_id);
CREATE INDEX IF NOT EXISTS idx_bobbin_records_factory ON bobbin_records(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_bobbin_records_dyed_lot ON bobbin_records(input_dyed_lot_no);

-- ------------------------------------------------------------
-- 3. WASTE VARIANCE ALARMS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS winding_waste_variance_alarms (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_card_id UUID REFERENCES winding_job_cards(id) ON DELETE CASCADE,
    factory_node_id VARCHAR(50) NOT NULL,
    operator_id UUID REFERENCES users(id),
    allocated_input_weight_kg DECIMAL(10,3),
    output_wound_weight_kg DECIMAL(10,3),
    winding_scrap_waste_gm DECIMAL(10,3),
    waste_variance_percent DECIMAL(5,2),
    threshold_percent DECIMAL(5,2) DEFAULT 0.5,
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

CREATE INDEX IF NOT EXISTS idx_winding_waste_alarms_job ON winding_waste_variance_alarms(job_card_id);
CREATE INDEX IF NOT EXISTS idx_winding_waste_alarms_factory ON winding_waste_variance_alarms(factory_node_id);

-- ------------------------------------------------------------
-- 4. WINDING PRODUCTION CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS winding_production_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_card_id UUID NOT NULL REFERENCES winding_job_cards(id) ON DELETE CASCADE,
    bobbin_id UUID REFERENCES bobbin_records(id),
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    operator_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    certification_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'REVOKED', 'EXPIRED'
    )),
    certified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_winding_certificates_job ON winding_production_certificates(job_card_id);
CREATE INDEX IF NOT EXISTS idx_winding_certificates_bobbin ON winding_production_certificates(bobbin_id);

-- ------------------------------------------------------------
-- 5. UPDATE ROLES: Bobbin Winder permissions
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari","winding:log:write","winding:read"]',
    description = 'Wind bobbins, manage spool inventory, tag output, maintain tension profiles, quality checks'
WHERE role_id = 'ROLE-BOBBIN-WINDER';

-- ------------------------------------------------------------
-- 6. TRIGGERS: Auto-create certificate on completion
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION create_winding_certificate()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'CERTIFIED' AND OLD.status IS DISTINCT FROM 'CERTIFIED' THEN
        INSERT INTO winding_production_certificates (
            job_card_id,
            certificate_hash,
            operator_id,
            factory_node_id,
            qr_tag_id,
            certification_data
        )
        SELECT
            NEW.id,
            encode(digest(NEW.id::text || NEW.winding_job_card_id || CURRENT_TIMESTAMP::text, 'sha256'), 'hex'),
            NEW.operator_employee_id,
            NEW.factory_node_id,
            'WIND-' || NEW.winding_job_card_id,
            jsonb_build_object(
                'winding_job_card_id', NEW.winding_job_card_id,
                'input_dyed_lot_no', NEW.input_dyed_lot_no,
                'yarn_type', NEW.yarn_type,
                'carrier_destination_type', NEW.carrier_destination_type,
                'allocated_input_weight_kg', NEW.allocated_input_weight_kg,
                'output_wound_weight_kg', NEW.output_wound_weight_kg,
                'winding_scrap_waste_gm', NEW.winding_scrap_waste_gm,
                'waste_variance_percent', NEW.waste_variance_percent,
                'inventory_output_routing_allocation', NEW.inventory_output_routing_allocation
            )
        WHERE NOT EXISTS (
            SELECT 1 FROM winding_production_certificates WHERE job_card_id = NEW.id
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_create_winding_certificate ON winding_job_cards;

CREATE TRIGGER trigger_create_winding_certificate
    AFTER UPDATE OF status ON winding_job_cards
    FOR EACH ROW EXECUTE FUNCTION create_winding_certificate();

-- ------------------------------------------------------------
-- 7. TRIGGER: Update timestamps
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_winding_job_cards_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_winding_job_cards_updated_at ON winding_job_cards;

CREATE TRIGGER trigger_update_winding_job_cards_updated_at
    BEFORE UPDATE ON winding_job_cards
    FOR EACH ROW EXECUTE FUNCTION update_winding_job_cards_updated_at();

-- ------------------------------------------------------------
-- 8. STOCK ROUTING VIEW
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW vw_bobbin_stock_routing AS
SELECT
    br.id AS bobbin_id,
    br.job_card_id,
    wjc.winding_job_card_id,
    br.carrier_type,
    br.yarn_type,
    br.silk_fiber_variety,
    br.input_dyed_lot_no,
    br.net_weight_kg,
    br.status AS bobbin_status,
    wjc.inventory_output_routing_allocation,
    wjc.waste_variance_percent,
    wjc.bobbin_structural_build_verdict,
    wjc.knot_mechanical_join_profiling,
    wjc.factory_node_id,
    wjc.certificate_hash,
    wcp.qr_tag_id AS winding_certificate_qr,
    CASE 
        WHEN wjc.status = 'CERTIFIED' AND wjc.waste_variance_percent <= 0.5 
             AND wjc.bobbin_structural_build_verdict = 'OPTIMAL_CROSS_WOUND'
        THEN 'BOBBIN_CLEARED_FOR_WARPING'
        WHEN wjc.status = 'CERTIFIED' AND wjc.waste_variance_percent <= 0.5 
             AND wjc.bobbin_structural_build_verdict IN ('SOFT_BUILD_COLLAPSE', 'RIDGED_SHOULDER_TRAP')
        THEN 'BOBBIN_CLEARED_FOR_PIRN_WEFT'
        ELSE 'WINDING_REJECT_RE_RUN'
    END AS recommended_routing
FROM bobbin_records br
JOIN winding_job_cards wjc ON br.job_card_id = wjc.id
LEFT JOIN winding_production_certificates wcp ON wcp.job_card_id = wjc.id;
