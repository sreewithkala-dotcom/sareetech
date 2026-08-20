-- ============================================================
-- Migration: 013_silk_degumming_master.sql
-- Silk Degumming Master module
-- Chemical bath formulation, thermal-process control,
-- weight loss accounting, and post-degumming certification.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE SILK-DEGUMMING-MASTER ROLE
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","process:control","degumming:batch:create","degumming:batch:read","degumming:record:write","degumming:record:read","degumming:certificate:read","sales:forecast:read"]',
    description = 'Silk Degumming Master: chemical bath formulation, thermal-process control, weight loss accounting, yarn preparation for dyeing, certificate issuance'
WHERE role_id = 'ROLE-SILK-DEGUMMING-MASTER';

-- ------------------------------------------------------------
-- 2. DEGUMMING BATCHES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS degumming_batches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    degumming_batch_no VARCHAR(100) UNIQUE NOT NULL,
    zari_inspection_id UUID REFERENCES zari_inspection_records(id),
    zari_assay_id UUID REFERENCES zari_assay_records(id),
    zari_lot_batch_id UUID REFERENCES zari_lot_batches(id),
    production_lot_id UUID REFERENCES production_lots(id),
    factory_node_id VARCHAR(50) NOT NULL,
    operator_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'QUEUED' CHECK (status IN (
        'QUEUED', 'IN_PROGRESS', 'COMPLETED', 'CERTIFIED', 'REJECTED', 'QC_HOLD'
    )),
    target_machine_type VARCHAR(50) CHECK (target_machine_type IN (
        '1536_HOOK_JACQUARD',
        '2400_HOOK_JACQUARD',
        'HANDLOOM',
        'POWERLOOM',
        'RAPIER_LOOM'
    )),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_degumming_batches_no ON degumming_batches(degumming_batch_no);
CREATE INDEX IF NOT EXISTS idx_degumming_batches_factory ON degumming_batches(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_degumming_batches_status ON degumming_batches(status);
CREATE INDEX IF NOT EXISTS idx_degumming_batches_zari ON degumming_batches(zari_lot_batch_id);

-- ------------------------------------------------------------
-- 3. DEGUMMING RECORDS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS degumming_records (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    degumming_batch_id UUID NOT NULL REFERENCES degumming_batches(id) ON DELETE CASCADE,
    -- Category A: Process Inputs & Chemical Control
    bath_liquor_ratio VARCHAR(50) NOT NULL CHECK (bath_liquor_ratio IN (
        '1:30_STANDARD_HANK',
        '1:40_GENTLE_HIGH_VOLUME',
        '1:50_ULTRA_FINE_YARN'
    )),
    deaerating_agent_used BOOLEAN DEFAULT FALSE,
    bath_ph_level DECIMAL(4,2) CHECK (bath_ph_level BETWEEN 0 AND 14),
    boil_duration_minutes INTEGER CHECK (boil_duration_minutes BETWEEN 0 AND 480),
    -- Category B: Post-Process Quality & Weight Loss Metrics
    raw_dry_weight_kg DECIMAL(10,3) NOT NULL,
    degummed_dry_weight_kg DECIMAL(10,3) NOT NULL,
    sericin_loss_pct DECIMAL(5,2),
    post_degum_tenacity_gd DECIMAL(5,2),
    fibrillation_index VARCHAR(50) CHECK (fibrillation_index IN (
        'GRADE_5_FLAWLESS_GLASSY',
        'GRADE_3_4_SLIGHT_FUZZ',
        'GRADE_1_2_SEVERE_CHAFING_SLUBS'
    )),
    -- Chemical Recipe
    degumming_agent_base VARCHAR(50) CHECK (degumming_agent_base IN (
        'NEUTRAL_MARSEILLE_SOAP',
        'SYNTHETIC_ANIONIC_DETERGENT',
        'ENZYMATIC_PROTEASE_AGENT'
    )),
    alkali_buffer_additive VARCHAR(50) CHECK (alkali_buffer_additive IN (
        'SODIUM_CARBONATE_SODA_ASH',
        'SODIUM_BICARBONATE',
        'TETRASODIUM_PYROPHOSPHATE',
        'NONE'
    )),
    water_softening_agent VARCHAR(50) CHECK (water_softening_agent IN (
        'EDTA_CHELATING_AGENT',
        'ZEOLITE_POWDER',
        'REVERSE_OSMOSIS_PURE_WATER',
        'NONE'
    )),
    -- Thermal-Process Physics
    vessel_type_allocated VARCHAR(50) CHECK (vessel_type_allocated IN (
        'OPEN_BOILING_VAT',
        'CLOSED_PRESSURE_KETTLE',
        'CONTINUOUS_ROPE_WASHER'
    )),
    boil_temperature_profile INTEGER CHECK (boil_temperature_profile BETWEEN 0 AND 120),
    -- Validation & Guardrails
    validation_errors JSONB DEFAULT '[]'::jsonb,
    validation_warnings JSONB DEFAULT '[]'::jsonb,
    auto_assigned_routing VARCHAR(50),
    status VARCHAR(50) DEFAULT 'DRAFT' CHECK (status IN (
        'DRAFT', 'IN_PROGRESS', 'COMPLETED', 'CERTIFIED', 'REJECTED', 'QC_HOLD',
        'DOWNGRADE_TO_1536_OR_WEFT'
    )),
    certificate_hash VARCHAR(255),
    qr_tag_id VARCHAR(100) UNIQUE,
    factory_node_id VARCHAR(50) NOT NULL,
    operator_id UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_degumming_records_batch ON degumming_records(degumming_batch_id);
CREATE INDEX IF NOT EXISTS idx_degumming_records_factory ON degumming_records(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_degumming_records_status ON degumming_records(status);
CREATE INDEX IF NOT EXISTS idx_degumming_records_routing ON degumming_records(auto_assigned_routing);

-- ------------------------------------------------------------
-- 4. DEGUMMING CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS degumming_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    degumming_batch_id UUID NOT NULL REFERENCES degumming_batches(id) ON DELETE CASCADE,
    degumming_record_id UUID NOT NULL REFERENCES degumming_records(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    zari_lot_batch_id UUID REFERENCES zari_lot_batches(id),
    certified_grade VARCHAR(10),
    auto_assigned_routing VARCHAR(50) NOT NULL,
    sericin_loss_pct DECIMAL(5,2),
    post_degum_tenacity_gd DECIMAL(5,2),
    fibrillation_index VARCHAR(50),
    pre_boil_dry_weight_kg DECIMAL(10,3),
    post_boil_dry_weight_kg DECIMAL(10,3),
    bath_ph_level DECIMAL(4,2),
    boil_temperature_profile INTEGER,
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

CREATE INDEX IF NOT EXISTS idx_degumming_certificates_batch ON degumming_certificates(degumming_batch_id);
CREATE INDEX IF NOT EXISTS idx_degumming_certificates_hash ON degumming_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_degumming_certificates_qr ON degumming_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 5. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_degumming_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_degumming_factory ON sales_forecast_degumming_plan(factory_node_id);

-- ------------------------------------------------------------
-- 6. TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION calculate_sericin_loss()
RETURNS TRIGGER AS $$
BEGIN
    -- Calculate sericin loss percentage
    IF NEW.raw_dry_weight_kg IS NOT NULL AND NEW.degummed_dry_weight_kg IS NOT NULL 
       AND NEW.raw_dry_weight_kg > 0 THEN
        NEW.sericin_loss_pct := ROUND(
            ((NEW.raw_dry_weight_kg - NEW.degummed_dry_weight_kg) / NEW.raw_dry_weight_kg) * 100,
            2
        );
    END IF;
    
    -- Rule 1: 2400 Hook Tenacity Protection
    -- IF target_machine = 2400_HOOK_JACQUARD AND post_degum_tenacity_gd < 3.8
    -- → BLOCK: DOWNGRADE_TO_1536_OR_WEFT
    IF NEW.target_machine_type = '2400_HOOK_JACQUARD' THEN
        IF NEW.post_degum_tenacity_gd IS NOT NULL AND NEW.post_degum_tenacity_gd < 3.8 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'DOWNGRADE_TO_1536_OR_WEFT',
                    'message', format('Post-degum tenacity %s g/d below 3.8 minimum for 2400 Hook Jacquard', NEW.post_degum_tenacity_gd),
                    'severity', 'BLOCK'
                ));
        END IF;
        
        -- Rule 3: Fibrillation / Surface Defect Check
        -- IF target_machine = 2400_HOOK_JACQUARD AND fibrillation_index IN (Grade 3-4, Grade 1-2)
        -- → ROUTE: QC_REJECT_HOLD
        IF NEW.fibrillation_index IN ('GRADE_3_4_SLIGHT_FUZZ', 'GRADE_1_2_SEVERE_CHAFING_SLUBS') THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'SURFACE_FUZZ_WILL_JAM_FINE_REED',
                    'message', 'Fibrillation detected. Surface fuzz will jam fine reed in 2400 Hook Jacquard.',
                    'severity', 'BLOCK'
                ));
        END IF;
        
        -- Check pH and temperature for 2400 Hook (mild conditions)
        IF NEW.bath_ph_level IS NOT NULL AND NEW.bath_ph_level > 9.5 THEN
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'PH_TOO_HIGH_FOR_2400_HOOK',
                    'message', format('pH %s exceeds 9.5 limit for 2400 Hook Jacquard (mild degumming required)', NEW.bath_ph_level),
                    'severity', 'WARNING'
                ));
        END IF;
        
        IF NEW.boil_temperature_profile IS NOT NULL AND NEW.boil_temperature_profile > 92 THEN
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'TEMPERATURE_TOO_HIGH_FOR_2400_HOOK',
                    'message', format('Boil temperature %s°C exceeds 92°C limit for 2400 Hook Jacquard', NEW.boil_temperature_profile),
                    'severity', 'WARNING'
                ));
        END IF;
    END IF;
    
    -- Rule 2: Sericin Over-Boil Flag
    -- IF sericin_loss_pct > 24.0%
    -- → TRIGGER WARNING: OVER_DEGUMMED
    IF NEW.sericin_loss_pct IS NOT NULL AND NEW.sericin_loss_pct > 24.0 THEN
        NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'OVER_DEGUMMED',
                'message', format('Sericin loss %s%% exceeds 24%% threshold. High risk of warp breakage.', NEW.sericin_loss_pct),
                'severity', 'WARNING'
            ));
    END IF;
    
    -- Check pH range
    IF NEW.bath_ph_level IS NOT NULL AND (NEW.bath_ph_level < 8.5 OR NEW.bath_ph_level > 10.5) THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'PH_OUT_OF_RANGE',
                'message', format('Bath pH %s is outside acceptable range 8.5-10.5', NEW.bath_ph_level),
                'severity', 'BLOCK'
            ));
    END IF;
    
    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'QC_REJECT_HOLD';
        ELSIF NEW.target_machine_type = '2400_HOOK_JACQUARD' 
              AND NEW.post_degum_tenacity_gd >= 3.8
              AND NEW.sericin_loss_pct >= 18.0 AND NEW.sericin_loss_pct <= 20.0
              AND NEW.fibrillation_index = 'GRADE_5_FLAWLESS_GLASSY' THEN
            NEW.auto_assigned_routing := 'LUXURY_2400_HOOK_JACQUARD_POOL';
        ELSIF NEW.target_machine_type = '1536_HOOK_JACQUARD'
              AND NEW.sericin_loss_pct >= 20.0 AND NEW.sericin_loss_pct <= 23.0 THEN
            NEW.auto_assigned_routing := 'LUXURY_1536_HOOK_JACQUARD_POOL';
        ELSIF NEW.sericin_loss_pct >= 18.0 AND NEW.sericin_loss_pct <= 25.0 THEN
            NEW.auto_assigned_routing := 'COMMERCIAL_SEMI_PREMIUM';
        ELSE
            NEW.auto_assigned_routing := 'QC_REJECT_HOLD';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_calculate_sericin_loss ON degumming_records;

CREATE TRIGGER trigger_calculate_sericin_loss
    BEFORE INSERT OR UPDATE ON degumming_records
    FOR EACH ROW EXECUTE FUNCTION calculate_sericin_loss();

-- ------------------------------------------------------------
-- 7. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_degumming_batches_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_degumming_batches_updated_at ON degumming_batches;

CREATE TRIGGER trigger_update_degumming_batches_updated_at
    BEFORE UPDATE ON degumming_batches
    FOR EACH ROW EXECUTE FUNCTION update_degumming_batches_updated_at();

CREATE OR REPLACE FUNCTION update_degumming_records_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_degumming_records_updated_at ON degumming_records;

CREATE TRIGGER trigger_update_degumming_records_updated_at
    BEFORE UPDATE ON degumming_records
    FOR EACH ROW EXECUTE FUNCTION update_degumming_records_updated_at();

-- ------------------------------------------------------------
-- 8. UPDATE ZARI-INSPECTOR ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari","zari:lot:read","zari:assay:read","zari:inspection:write","zari:inspection:read","zari:certificate:read","zari:routing:write","sales:forecast:read","degumming:batch:read"]',
    description = 'Zari Inspector: post-process XRF verification, physical/geometric inspection, aesthetic control, defect logging, ERP routing, certificate issuance; provides pre-process material to Degumming Master'
WHERE role_id = 'ROLE-ZARI-INSPECTOR';
