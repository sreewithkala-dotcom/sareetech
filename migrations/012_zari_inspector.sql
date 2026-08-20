-- ============================================================
-- Migration: 012_zari_inspector.sql
-- Zari Inspector Post-Process Inspection module
-- Adds physical, geometric, aesthetic, and routing inspection
-- with automated 1536/2400 hook validation rules.
-- ============================================================

-- ------------------------------------------------------------
-- 1. ALTER EXISTING ZARI ASSAY RECORDS: add missing fields
-- ------------------------------------------------------------

ALTER TABLE zari_assay_records
    ADD COLUMN IF NOT EXISTS tensile_strength_gd DECIMAL(5,2),
    ADD COLUMN IF NOT EXISTS target_machine_type VARCHAR(50) CHECK (target_machine_type IN (
        '1536_HOOK_JACQUARD',
        '2400_HOOK_JACQUARD',
        'HANDLOOM',
        'POWERLOOM',
        'RAPIER_LOOM'
    )),
    ADD COLUMN IF NOT EXISTS joint_breaks_per_1000m INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS flattened_wire_width_mm DECIMAL(5,2),
    ADD COLUMN IF NOT EXISTS surface_coating_lubrication VARCHAR(50) CHECK (surface_coating_lubrication IN (
        'STANDARD_PARAFFIN',
        'SILICONE_MICRO_WAX',
        'HIGH_GRADE_SILICONE',
        'NONE'
    )),
    ADD COLUMN IF NOT EXISTS surface_coating_check_passed BOOLEAN DEFAULT FALSE;

-- ------------------------------------------------------------
-- 2. ZARI INSPECTION RECORDS (Post-Process from Zari Refinery)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zari_inspection_records (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    zari_assay_id UUID NOT NULL REFERENCES zari_assay_records(id) ON DELETE CASCADE,
    zari_lot_batch_id UUID NOT NULL REFERENCES zari_lot_batches(id) ON DELETE CASCADE,
    -- XRF Spectrometer Verification
    xrf_silver_purity_pct DECIMAL(5,2),
    xrf_gold_plating_pct DECIMAL(5,2),
    xrf_verification_passed BOOLEAN DEFAULT FALSE,
    -- Core Yarn Auditing
    core_yarn_audit_result VARCHAR(50) CHECK (core_yarn_audit_result IN (
        'PURE_SILK_RED_MAROON_DYED',
        'PURE_SILK_UN_DYED_WHITE',
        'PURE_COTTON_COMBED_FINE',
        'POLYESTER_FILAMENT_HIGH_TENACITY',
        'VISCOSE_RAYON_CORE',
        'NYLON_MONOFILAMENT'
    )),
    core_yarn_audit_method VARCHAR(50) CHECK (core_yarn_audit_method IN (
        'BURN_TEST',
        'CHEMICAL_STRIP',
        'MICROSCOPE_VISUAL',
        'FTIR_SPECTROSCOPY'
    )),
    core_yarn_audit_passed BOOLEAN DEFAULT FALSE,
    -- Physical & Textile Geometrics
    denier_measured DECIMAL(5,2),
    denier_target VARCHAR(50) CHECK (denier_target IN (
        '13/15_DENIER',
        '16/18_DENIER',
        '20/22_DENIER',
        '24/26_DENIER',
        '28/30_DENIER'
    )),
    tensile_strength_gd DECIMAL(5,2),
    tensile_strength_min_gd DECIMAL(5,2) DEFAULT 3.5,
    bobbin_winding_integrity VARCHAR(50) CHECK (bobbin_winding_integrity IN (
        'EXCELLENT',
        'GOOD',
        'POOR',
        'FAIL'
    )),
    -- Aesthetic & Oxidation Control
    tarnish_free_scan BOOLEAN DEFAULT FALSE,
    color_luster_match BOOLEAN DEFAULT FALSE,
    delta_e_value DECIMAL(5,2),
    -- Precision Weight Auditing
    gross_scale_weight_gm DECIMAL(10,3),
    tare_weight_gm DECIMAL(10,3),
    net_zari_weight_gm DECIMAL(10,3),
    moisture_reading_pct DECIMAL(5,2),
    -- Defect Logging
    wire_cuts_per_1000m INTEGER DEFAULT 0,
    micro_cuts_detected BOOLEAN DEFAULT FALSE,
    frayed_joints_detected BOOLEAN DEFAULT FALSE,
    -- Automated Validation
    validation_errors JSONB DEFAULT '[]'::jsonb,
    validation_warnings JSONB DEFAULT '[]'::jsonb,
    -- Routing
    auto_assigned_routing VARCHAR(50),
    status VARCHAR(50) DEFAULT 'DRAFT' CHECK (status IN (
        'DRAFT', 'SUBMITTED', 'CERTIFIED', 'REJECTED', 'QC_HOLD',
        'DOWNGRADE_TO_1536_HOOK_OR_HANDLOOM'
    )),
    certificate_hash VARCHAR(255),
    qr_tag_id VARCHAR(100) UNIQUE,
    -- Audit
    inspector_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_zari_inspection_assay ON zari_inspection_records(zari_assay_id);
CREATE INDEX IF NOT EXISTS idx_zari_inspection_batch ON zari_inspection_records(zari_lot_batch_id);
CREATE INDEX IF NOT EXISTS idx_zari_inspection_factory ON zari_inspection_records(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_zari_inspection_status ON zari_inspection_records(status);
CREATE INDEX IF NOT EXISTS idx_zari_inspection_routing ON zari_inspection_records(auto_assigned_routing);

-- ------------------------------------------------------------
-- 3. ZARI INSPECTOR CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zari_inspector_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    zari_inspection_id UUID NOT NULL REFERENCES zari_inspection_records(id) ON DELETE CASCADE,
    zari_assay_id UUID NOT NULL REFERENCES zari_assay_records(id) ON DELETE CASCADE,
    zari_lot_batch_id UUID NOT NULL REFERENCES zari_lot_batches(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    zari_type VARCHAR(50) NOT NULL,
    zari_origin_cluster VARCHAR(100) NOT NULL,
    certified_grade VARCHAR(10),
    auto_assigned_routing VARCHAR(50) NOT NULL,
    xrf_silver_purity_pct DECIMAL(5,2),
    xrf_gold_plating_pct DECIMAL(5,2),
    core_yarn_audit_result VARCHAR(50),
    tensile_strength_gd DECIMAL(5,2),
    net_zari_weight_gm DECIMAL(10,3),
    precious_metal_value_estimate DECIMAL(12,4),
    inspector_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    certification_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'REVOKED', 'EXPIRED', 'PENDING'
    )),
    certified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_zari_inspector_certificates_inspection ON zari_inspector_certificates(zari_inspection_id);
CREATE INDEX IF NOT EXISTS idx_zari_inspector_certificates_hash ON zari_inspector_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_zari_inspector_certificates_qr ON zari_inspector_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 4. TRIGGERS: Auto-calculate fields and routing
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION calculate_zari_inspection_derived_fields()
RETURNS TRIGGER AS $$
BEGIN
    -- Calculate net zari weight
    IF NEW.gross_scale_weight_gm IS NOT NULL AND NEW.tare_weight_gm IS NOT NULL THEN
        NEW.net_zari_weight_gm := ROUND(
            NEW.gross_scale_weight_gm - NEW.tare_weight_gm,
            3
        );
    END IF;
    
    -- Rule 1: 2400 Hook Compatibility Check
    -- IF target_machine = 2400_HOOK_JACQUARD AND core_yarn_type NOT IN (Pure Silk Core)
    -- → BLOCK: REJECT_FOR_2400_HOOK (HIGH_STATIC_FLEX_RISK)
    IF NEW.target_machine_type = '2400_HOOK_JACQUARD' THEN
        IF NEW.core_yarn_audit_result NOT IN (
            'PURE_SILK_RED_MAROON_DYED',
            'PURE_SILK_UN_DYED_WHITE'
        ) THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'REJECT_FOR_2400_HOOK',
                    'message', 'Non-silk core yarn is not compatible with 2400 Hook Jacquard. High static/flex risk.',
                    'severity', 'BLOCK'
                ));
        END IF;
        
        -- Rule 2: Joint & Surface Defect Check
        -- IF target_machine = 2400_HOOK_JACQUARD AND joint_breaks_per_1000m > 0
        -- → ROUTE: DOWNGRADE_TO_1536_HOOK_OR_HANDLOOM
        IF NEW.joint_breaks_per_1000m IS NOT NULL AND NEW.joint_breaks_per_1000m > 0 THEN
            NEW.auto_assigned_routing := 'DOWNGRADE_TO_1536_HOOK_OR_HANDLOOM';
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'DOWNGRADE_TO_1536_HOOK_OR_HANDLOOM',
                    'message', 'Joint breaks detected. Downgrade routing applied.',
                    'severity', 'WARNING'
                ));
        END IF;
        
        -- Rule 3: Badla Width Verification
        -- IF flattened_wire_width_mm > 0.15 AND target_machine = 2400_HOOK_JACQUARD
        -- → TRIGGER WARNING: HEAVY_ZARI_WILL_CAUSE_FABRIC_STIFFNESS
        IF NEW.flattened_wire_width_mm IS NOT NULL AND NEW.flattened_wire_width_mm > 0.15 THEN
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'HEAVY_ZARI_WILL_CAUSE_FABRIC_STIFFNESS',
                    'message', format('Badla width %s mm exceeds 0.15 mm limit for 2400 Hook Jacquard', NEW.flattened_wire_width_mm),
                    'severity', 'WARNING'
                ));
        END IF;
    END IF;
    
    -- Tarnish check
    IF NOT NEW.tarnish_free_scan THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'TARNISH_DETECTED',
                'message', 'Zari shows oxidation/tarnish under 5000K lamp. Reject batch.',
                'severity', 'BLOCK'
            ));
    END IF;
    
    -- Wire cuts check
    IF NEW.wire_cuts_per_1000m IS NOT NULL AND NEW.wire_cuts_per_1000m > 0 THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'WIRE_CUTS_DETECTED',
                'message', format('Wire cuts detected: %s per 1000m', NEW.wire_cuts_per_1000m),
                'severity', 'BLOCK'
            ));
    END IF;
    
    -- Auto-assign routing if not already set by rule-based downgrade
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'QC_REJECT_HOLD';
        ELSIF NEW.target_machine_type = '2400_HOOK_JACQUARD' AND NEW.core_yarn_audit_result IN (
            'PURE_SILK_RED_MAROON_DYED', 'PURE_SILK_UN_DYED_WHITE'
        ) AND NEW.tensile_strength_gd >= 3.8 THEN
            NEW.auto_assigned_routing := 'LUXURY_JACQUARD_LOOM_POOL';
        ELSIF NEW.tensile_strength_gd >= 3.5 AND NEW.tarnish_free_scan THEN
            NEW.auto_assigned_routing := 'COMMERCIAL_SEMI_PREMIUM';
        ELSE
            NEW.auto_assigned_routing := 'QC_REJECT_HOLD';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_calculate_zari_inspection_derived_fields ON zari_inspection_records;

CREATE TRIGGER trigger_calculate_zari_inspection_derived_fields
    BEFORE INSERT OR UPDATE ON zari_inspection_records
    FOR EACH ROW EXECUTE FUNCTION calculate_zari_inspection_derived_fields();

-- ------------------------------------------------------------
-- 5. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_zari_inspection_records_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_zari_inspection_records_updated_at ON zari_inspection_records;

CREATE TRIGGER trigger_update_zari_inspection_records_updated_at
    BEFORE UPDATE ON zari_inspection_records
    FOR EACH ROW EXECUTE FUNCTION update_zari_inspection_records_updated_at();

-- ------------------------------------------------------------
-- 6. UPDATE ZARI-INSPECTOR ROLE PERMISSIONS
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari","zari:lot:read","zari:assay:read","zari:inspection:write","zari:inspection:read","zari:certificate:read","zari:routing:write","sales:forecast:read"]',
    description = 'Zari Inspector: post-process XRF verification, physical/geometric inspection, aesthetic control, defect logging, ERP routing, certificate issuance'
WHERE role_id = 'ROLE-ZARI-INSPECTOR';

-- ------------------------------------------------------------
-- 7. UPDATE FILATURE SUPPLIER ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari","inward:gate:create","inward:gate:read","inward:quality:read","inward:lab:read","zari:lot:read"]',
    description = 'Filature Supplier: source raw silk, inward quality gate, provide pre-process material to Zari Refinery and Zari Inspector'
WHERE role_id = 'ROLE-FILATURE-SUPPLIER';
