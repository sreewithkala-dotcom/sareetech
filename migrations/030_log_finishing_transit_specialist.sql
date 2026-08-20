-- ============================================================
-- Migration: 030_log_finishing_transit_specialist.sql
-- LOG Finishing & Transit Specialist module: post-weave finishing,
-- anti-tamper barcoding, moisture-controlled luxury packaging,
-- B2B shipment consolidation, and freight forwarding.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE ROLES: Expand permissions
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","quality:read","quality:write","compliance:read","compliance:write","silk-mark:read","silk-mark:certify","finishing:write","finishing:certify","dispatch:write","dispatch:certify","sales:forecast:read","b2b:order:write"]',
    description = 'LOG Finishing & Transit Specialist (Logistics, Finishing, and Transit Specialist): post-weave cosmetic treatments, anti-tamper barcoding, moisture-controlled luxury packaging, B2B shipment consolidation, freight forwarding, secured regional/global distribution, and dispatch release'
WHERE role_id = 'ROLE-LOG-FINISHING';

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","weaving:read","weaving:supervise","weaving:inspect","assistant:read","master-weaver:read","petni-master:read","warp-joiner:read","harness-setter:read","sales:forecast:read","environment:read","environment:write","oee:read","oee:write","quality:read","quality:write","dyeing:read","dyeing:write","compliance:read","compliance:write","silk-mark:read","silk-mark:write","finishing:read","finishing:write","dispatch:read","dispatch:write"]',
    description = 'SUP Loom Floor Supervisor (Weaving Shed Supervisor / Jacquard Floor Lead): oversees production, mechanical efficiency, quality compliance, finishing, dispatch, and shift coordination across the weaving floor; monitors OEE, environmental controls, workflow handovers, first saree approval, batch release authorization, quality inspection clearance, dyeing quality verification, Silk Mark compliance, and finishing/dispatch clearance'
WHERE role_id = 'ROLE-SUP-LOOM-FLOOR-SUPERVISOR';

-- ------------------------------------------------------------
-- 2. LOG FINISHING & TRANSIT SPECIALIST LOGS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS log_finishing_transit_specialist_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    finishing_job_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    log_finishing_transit_specialist_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    quality_inspector_log_id UUID REFERENCES quality_inspector_logs(id),
    qa_dyeing_inspector_log_id UUID REFERENCES qa_dyeing_inspector_logs(id),
    silk_mark_officer_log_id UUID REFERENCES silk_mark_officer_logs(id),
    sup_supervisor_log_id UUID REFERENCES sup_loom_floor_supervisor_logs(id),
    assistant_weaver_job_log_id UUID REFERENCES assistant_weaver_job_logs(id),
    master_weaver_job_id UUID REFERENCES master_weaver_jobs(id),
    b2b_sales_order_ref VARCHAR(100),

    -- Category A: Finishing Setup & Processing Configuration
    saree_piece_serial_id VARCHAR(100),
    finishing_machine_type VARCHAR(50) DEFAULT 'Tensionless Felt-Belt Steam Calender' CHECK (finishing_machine_type IN (
        'Tensionless Felt-Belt Steam Calender',
        'Rotary Roller Calender',
        'Manual Steam Table',
        'Decatising Pressure Vessel'
    )),
    steam_temperature_celsius DECIMAL(5,2),
    edge_fringing_method VARCHAR(50) DEFAULT 'Hand-Twisted Micro-Fringe Knotting' CHECK (edge_fringing_method IN (
        'Hand-Twisted Micro-Fringe Knotting',
        'Satin Ribbon Hem Lock',
        'Ultra-Sonic Edge Cut',
        'Unfinished Open Fringe'
    )),

    -- Category B: Packaging Integrity & Anti-Tarnish Audit
    scanned_saree_serial_no VARCHAR(100),
    verified_silk_mark_tag_id VARCHAR(100),
    packaging_material_spec VARCHAR(50) DEFAULT 'Acid-Free Tissue + Anti-Tarnish Vacuum Pack' CHECK (packaging_material_spec IN (
        'Acid-Free Tissue + Anti-Tarnish Vacuum Pack',
        'Standard 50-Micron Polybag',
        'Breathable Cotton Muslin Bag'
    )),
    anti_tarnish_desiccant_inserted VARCHAR(50) DEFAULT 'Active Silica + Activated Carbon Pack' CHECK (anti_tarnish_desiccant_inserted IN (
        'Active Silica + Activated Carbon Pack',
        'Silica Gel Only',
        'None (Unsafe)'
    )),
    pallu_interleaving_status VARCHAR(50) DEFAULT 'Acid-Free Interleaved (Zero-Contact)' CHECK (pallu_interleaving_status IN (
        'Acid-Free Interleaved (Zero-Contact)',
        'Standard Paper Interleaved',
        'No Interleaving'
    )),
    final_packed_weight_grams DECIMAL(8,2),

    -- Category C: Dispatch Release & Transit Verification
    dispatch_manifest_id VARCHAR(100),
    tamper_seal_barcode_id VARCHAR(100),
    logistics_transit_status VARCHAR(50) DEFAULT 'READY_FOR_DISPATCH_MANIFESTED' CHECK (logistics_transit_status IN (
        'READY_FOR_DISPATCH_MANIFESTED',
        'HOLD_PENDING_EXCISE_CUSTOMS',
        'REJECTED_PACKAGING_DAMAGED'
    )),
    logistics_carrier_name VARCHAR(100),
    consignment_airway_bill_no VARCHAR(100),
    gross_consignment_shipping_weight_kg DECIMAL(8,2),

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

CREATE INDEX IF NOT EXISTS idx_log_finishing_transit_specialist_logs_job ON log_finishing_transit_specialist_logs(finishing_job_id);
CREATE INDEX IF NOT EXISTS idx_log_finishing_transit_specialist_logs_specialist ON log_finishing_transit_specialist_logs(log_finishing_transit_specialist_id);
CREATE INDEX IF NOT EXISTS idx_log_finishing_transit_specialist_logs_factory ON log_finishing_transit_specialist_logs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_log_finishing_transit_specialist_logs_silk_mark ON log_finishing_transit_specialist_logs(silk_mark_officer_log_id);
CREATE INDEX IF NOT EXISTS idx_log_finishing_transit_specialist_logs_transit_status ON log_finishing_transit_specialist_logs(logistics_transit_status);
CREATE INDEX IF NOT EXISTS idx_log_finishing_transit_specialist_logs_manifest ON log_finishing_transit_specialist_logs(dispatch_manifest_id);
CREATE INDEX IF NOT EXISTS idx_log_finishing_transit_specialist_logs_airway ON log_finishing_transit_specialist_logs(consignment_airway_bill_no);

-- ------------------------------------------------------------
-- 3. LOG FINISHING & TRANSIT SPECIALIST CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS log_finishing_transit_specialist_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    log_finishing_transit_specialist_log_id UUID NOT NULL REFERENCES log_finishing_transit_specialist_logs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    finishing_job_id VARCHAR(100) NOT NULL,
    saree_piece_serial_id VARCHAR(100),
    scanned_saree_serial_no VARCHAR(100),
    verified_silk_mark_tag_id VARCHAR(100),
    dispatch_manifest_id VARCHAR(100),
    tamper_seal_barcode_id VARCHAR(100),
    logistics_transit_status VARCHAR(50),
    logistics_carrier_name VARCHAR(100),
    consignment_airway_bill_no VARCHAR(100),
    finishing_machine_type VARCHAR(50),
    steam_temperature_celsius DECIMAL(5,2),
    edge_fringing_method VARCHAR(50),
    packaging_material_spec VARCHAR(50),
    anti_tarnish_desiccant_inserted VARCHAR(50),
    pallu_interleaving_status VARCHAR(50),
    final_packed_weight_grams DECIMAL(8,2),
    gross_consignment_shipping_weight_kg DECIMAL(8,2),
    specialist_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    certification_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'IN_TRANSIT', 'DELIVERED', 'CANCELLED', 'PENDING'
    )),
    certified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_log_finishing_transit_specialist_certificates_log ON log_finishing_transit_specialist_certificates(log_finishing_transit_specialist_log_id);
CREATE INDEX IF NOT EXISTS idx_log_finishing_transit_specialist_certificates_hash ON log_finishing_transit_specialist_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_log_finishing_transit_specialist_certificates_qr ON log_finishing_transit_specialist_certificates(qr_tag_id);
CREATE INDEX IF NOT EXISTS idx_log_finishing_transit_specialist_certificates_manifest ON log_finishing_transit_specialist_certificates(dispatch_manifest_id);
CREATE INDEX IF NOT EXISTS idx_log_finishing_transit_specialist_certificates_airway ON log_finishing_transit_specialist_certificates(consignment_airway_bill_no);

-- ------------------------------------------------------------
-- 4. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_log_finishing_transit_specialist_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_log_finishing_transit_specialist_plan_factory ON sales_forecast_log_finishing_transit_specialist_plan(factory_node_id);

-- ------------------------------------------------------------
-- 5. TRIGGERS: Validation and automation
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_log_finishing_transit_specialist_log()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1 (Anti-Tarnish Packaging Protection Guardrail)
    -- IF zari_purity_classification (from Compliance) = PURE_GOLD_SILVER_TESTED_ZARI
    --    AND packaging_material_spec = Standard 50-Micron Polybag
    -- → BLOCK: REJECT_STANDARD_PACKAGING
    -- → SET logistics_transit_status = REJECTED_PACKAGING_DAMAGED
    IF EXISTS (
        SELECT 1 FROM silk_mark_officer_logs smol
        WHERE smol.id = NEW.silk_mark_officer_log_id
          AND smol.zari_purity_classification = 'PURE_GOLD_SILVER_TESTED_ZARI'
    ) AND NEW.packaging_material_spec = 'Standard 50-Micron Polybag' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'REJECT_STANDARD_PACKAGING',
                'message', 'PURE_ZARI_REQUIRES_ANTI_TARNISH_VACUUM_PACKAGING',
                'severity', 'BLOCK'
            ));
        NEW.logistics_transit_status := 'REJECTED_PACKAGING_DAMAGED';
    END IF;

    -- Rule 2 (Pre-Requirement Compliance Gate)
    -- IF silk_mark_officer_approval_state != CERTIFIED_GENUINE_SILK_MARK_RELEASED
    --    OR qa_inspector_approval_state != PASSED_CLEARED_FOR_PACKING
    -- → BLOCK: DENY_FINISHING_RECEIPT
    IF NEW.silk_mark_officer_log_id IS NULL THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_FINISHING_RECEIPT',
                'message', 'CANNOT_FINISH_UNAPPROVED_OR_UNCERTIFIED_SAREES',
                'severity', 'BLOCK'
            ));
    END IF;

    IF NEW.quality_inspector_log_id IS NULL THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_FINISHING_RECEIPT',
                'message', 'CANNOT_FINISH_UNAPPROVED_OR_UNCERTIFIED_SAREES',
                'severity', 'BLOCK'
            ));
    END IF;

    IF EXISTS (
        SELECT 1 FROM quality_inspector_logs qil
        WHERE qil.id = NEW.quality_inspector_log_id
          AND qil.qa_inspector_approval_state != 'PASSED_CLEARED_FOR_PACKING'
    ) THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_FINISHING_RECEIPT',
                'message', 'CANNOT_FINISH_UNAPPROVED_OR_UNCERTIFIED_SAREES',
                'severity', 'BLOCK'
            ));
    END IF;

    IF EXISTS (
        SELECT 1 FROM silk_mark_officer_logs smol
        WHERE smol.id = NEW.silk_mark_officer_log_id
          AND smol.silk_mark_officer_approval_state != 'CERTIFIED_GENUINE_SILK_MARK_RELEASED'
    ) THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_FINISHING_RECEIPT',
                'message', 'CANNOT_FINISH_UNAPPROVED_OR_UNCERTIFIED_SAREES',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Rule 3 (Zari Motif Crush Prevention Gate)
    -- IF finishing_machine_type = Rotary Roller Calender AND warp_density_measured_epi >= 140.0
    -- → BLOCK: INVALID_MACHINE_SELECTION
    -- Note: warp_density_measured_epi would come from quality_inspector_logs, but for now
    -- we check if finishing_machine_type is Rotary Roller Calender and warn
    IF NEW.finishing_machine_type = 'Rotary Roller Calender' THEN
        NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'INVALID_MACHINE_SELECTION_WARNING',
                'message', 'HEAVY_ROLLER_CALENDERS_WILL_CRUSH_2400_HOOK_JACQUARD_ZARI',
                'severity', 'WARNING'
            ));
    END IF;

    -- Rule 4 (Dispatch Manifest Release Gate)
    -- IF logistics_transit_status != READY_FOR_DISPATCH_MANIFESTED
    -- → BLOCK: DENY_WAREHOUSE_GATE_PASS_GENERATION
    IF NEW.logistics_transit_status IS NOT NULL
       AND NEW.logistics_transit_status != 'READY_FOR_DISPATCH_MANIFESTED' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_WAREHOUSE_GATE_PASS_GENERATION',
                'message', 'TRANSIT_STATUS_NOT_READY_FOR_DISPATCH',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'FINISHING_QC_HOLD';
        ELSIF NEW.logistics_transit_status = 'READY_FOR_DISPATCH_MANIFESTED' THEN
            NEW.auto_assigned_routing := 'READY_FOR_WAREHOUSE_DISPATCH';
        ELSE
            NEW.auto_assigned_routing := 'FINISHING_IN_PROGRESS';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_log_finishing_transit_specialist_log ON log_finishing_transit_specialist_logs;

CREATE TRIGGER trigger_validate_log_finishing_transit_specialist_log
    BEFORE INSERT OR UPDATE ON log_finishing_transit_specialist_logs
    FOR EACH ROW EXECUTE FUNCTION validate_log_finishing_transit_specialist_log();

-- ------------------------------------------------------------
-- 6. TRIGGER: Auto-generate certificate on approval
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION generate_log_finishing_transit_specialist_certificate()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.logistics_transit_status = 'READY_FOR_DISPATCH_MANIFESTED'
       AND OLD.logistics_transit_status IS DISTINCT FROM NEW.logistics_transit_status THEN
        NEW.certificate_hash := generate_certificate_hash(NEW.id, NEW.finishing_job_id);
        NEW.qr_tag_id := 'LOG-' || NEW.finishing_job_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_generate_log_finishing_transit_specialist_certificate ON log_finishing_transit_specialist_logs;

CREATE TRIGGER trigger_generate_log_finishing_transit_specialist_certificate
    BEFORE UPDATE OF logistics_transit_status ON log_finishing_transit_specialist_logs
    FOR EACH ROW EXECUTE FUNCTION generate_log_finishing_transit_specialist_certificate();

-- ------------------------------------------------------------
-- 7. TRIGGER: Update timestamps
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_log_finishing_transit_specialist_logs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_log_finishing_transit_specialist_logs_updated_at ON log_finishing_transit_specialist_logs;

CREATE TRIGGER trigger_update_log_finishing_transit_specialist_logs_updated_at
    BEFORE UPDATE ON log_finishing_transit_specialist_logs
    FOR EACH ROW EXECUTE FUNCTION update_log_finishing_transit_specialist_logs_updated_at();
