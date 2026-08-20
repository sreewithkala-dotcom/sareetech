-- ============================================================
-- Migration: 014_throwster_twister.sql
-- Throwster / Twister module
-- Converts raw silk filaments into multi-ply twisted yarn
-- (Organzine for warp, Tram for weft) with process control,
-- twist validation, steam stabilization, and certification.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE THROWSTER-TWISTER ROLE
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari","throwster:batch:create","throwster:batch:read","throwster:record:write","throwster:record:read","throwster:certificate:read","sales:forecast:read"]',
    description = 'Throwster/Twister: yarn ply doubling, TPI control, twist setting/steaming, defect elimination, certificate issuance'
WHERE role_id = 'ROLE-THROWSTER-TWISTER';

-- ------------------------------------------------------------
-- 2. THROWSTER BATCHES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS throwster_batches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    throwster_batch_no VARCHAR(100) UNIQUE NOT NULL,
    degumming_record_id UUID REFERENCES degumming_records(id),
    degumming_batch_id UUID REFERENCES degumming_batches(id),
    zari_lot_batch_id UUID REFERENCES zari_lot_batches(id),
    production_lot_id UUID REFERENCES production_lots(id),
    factory_node_id VARCHAR(50) NOT NULL,
    operator_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'QUEUED' CHECK (status IN (
        'QUEUED', 'IN_PROGRESS', 'COMPLETED', 'CERTIFIED', 'REJECTED', 'QC_HOLD'
    )),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_throwster_batches_no ON throwster_batches(throwster_batch_no);
CREATE INDEX IF NOT EXISTS idx_throwster_batches_factory ON throwster_batches(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_throwster_batches_status ON throwster_batches(status);
CREATE INDEX IF NOT EXISTS idx_throwster_batches_degumming ON throwster_batches(degumming_record_id);

-- ------------------------------------------------------------
-- 3. THROWSTER PRODUCTION RECORDS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS throwster_production_records (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    throwster_batch_id UUID NOT NULL REFERENCES throwster_batches(id) ON DELETE CASCADE,
    -- Category A: Input Materials
    input_raw_lot_no VARCHAR(100) NOT NULL,
    input_weight_kg DECIMAL(10,3) NOT NULL,
    target_ply_count INTEGER CHECK (target_ply_count IN (1, 2, 3, 4, 6)),
    intended_use VARCHAR(50) NOT NULL CHECK (intended_use IN (
        'WARP_YARN_HIGH_TWIST',
        'WEFT_YARN_LOW_TWIST'
    )),
    input_yarn_type VARCHAR(50) NOT NULL CHECK (input_yarn_type IN (
        'BIVOLTINE_WHITE',
        'MULTIVOLTINE_YELLOW',
        'DUPION_SILK',
        'SPUN_SILK',
        'TUSSAR_WILD_SILK'
    )),
    input_lot_purity_clearance BOOLEAN DEFAULT FALSE,
    -- Category B: Machine & Process Settings
    machinery_id VARCHAR(100),
    target_tpi DECIMAL(5,2),
    twist_direction VARCHAR(50) CHECK (twist_direction IN (
        'S_TWIST',
        'Z_TWIST',
        'S_Z_CABLE_TWIST'
    )),
    steam_setting_duration_mins INTEGER,
    -- Mechanical Structure Configurations
    ply_count INTEGER CHECK (ply_count IN (1, 2, 3, 4, 6)),
    first_twist_tpm DECIMAL(6,2),
    final_twist_tpm DECIMAL(6,2),
    engineered_yarn_profile VARCHAR(50) CHECK (engineered_yarn_profile IN (
        'ORGANZINE',
        'TRAM',
        'CREPE'
    )),
    spindle_rotational_speed_rpm INTEGER,
    steam_stabilization_method VARCHAR(50) CHECK (steam_stabilization_method IN (
        'VACUUM_AUTOCLAVE',
        'MANUAL_STEAM_CHAMBER',
        'NATURAL_AGING_ROOM'
    )),
    steam_temperature_celsius INTEGER CHECK (steam_temperature_celsius BETWEEN 0 AND 120),
    steaming_duration_minutes INTEGER,
    -- Category C: Output Materials & Waste Accounting
    output_twisted_weight_kg DECIMAL(10,3),
    process_scrap_waste_kg DECIMAL(10,3),
    material_discrepancy_kg DECIMAL(10,3),
    -- Category B (Post-Twist Quality & Lab Audit)
    tested_tpm_average DECIMAL(6,2),
    twist_variation_pct DECIMAL(5,2),
    snarl_count_per_100m INTEGER DEFAULT 0,
    oil_lubrication_pick_up_pct DECIMAL(5,2),
    -- Validation & Guardrails
    validation_errors JSONB DEFAULT '[]'::jsonb,
    validation_warnings JSONB DEFAULT '[]'::jsonb,
    auto_assigned_routing VARCHAR(50),
    status VARCHAR(50) DEFAULT 'DRAFT' CHECK (status IN (
        'DRAFT', 'IN_PROGRESS', 'COMPLETED', 'CERTIFIED', 'REJECTED', 'QC_HOLD',
        'DOWNGRADE_TO_1536_HOOK',
        'RE_STEAMING_REQUIRED'
    )),
    certificate_hash VARCHAR(255),
    qr_tag_id VARCHAR(100) UNIQUE,
    factory_node_id VARCHAR(50) NOT NULL,
    operator_id UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_throwster_records_batch ON throwster_production_records(throwster_batch_id);
CREATE INDEX IF NOT EXISTS idx_throwster_records_factory ON throwster_production_records(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_throwster_records_status ON throwster_production_records(status);
CREATE INDEX IF NOT EXISTS idx_throwster_records_routing ON throwster_production_records(auto_assigned_routing);

-- ------------------------------------------------------------
-- 4. THROWSTER CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS throwster_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    throwster_batch_id UUID NOT NULL REFERENCES throwster_batches(id) ON DELETE CASCADE,
    throwster_record_id UUID NOT NULL REFERENCES throwster_production_records(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    input_raw_lot_no VARCHAR(100) NOT NULL,
    certified_grade VARCHAR(10),
    auto_assigned_routing VARCHAR(50) NOT NULL,
    ply_count INTEGER,
    final_twist_tpm DECIMAL(6,2),
    twist_direction VARCHAR(50),
    engineered_yarn_profile VARCHAR(50),
    twist_variation_pct DECIMAL(5,2),
    snarl_count_per_100m INTEGER,
    input_weight_kg DECIMAL(10,3),
    output_twisted_weight_kg DECIMAL(10,3),
    process_scrap_waste_kg DECIMAL(10,3),
    material_discrepancy_kg DECIMAL(10,3),
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

CREATE INDEX IF NOT EXISTS idx_throwster_certificates_batch ON throwster_certificates(throwster_batch_id);
CREATE INDEX IF NOT EXISTS idx_throwster_certificates_hash ON throwster_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_throwster_certificates_qr ON throwster_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 5. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_throwster_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_throwster_factory ON sales_forecast_throwster_plan(factory_node_id);

-- ------------------------------------------------------------
-- 6. TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION calculate_throwster_derived_fields()
RETURNS TRIGGER AS $$
BEGIN
    -- Calculate material discrepancy
    IF NEW.input_weight_kg IS NOT NULL AND NEW.output_twisted_weight_kg IS NOT NULL 
       AND NEW.process_scrap_waste_kg IS NOT NULL THEN
        NEW.material_discrepancy_kg := ROUND(
            NEW.input_weight_kg - (NEW.output_twisted_weight_kg + NEW.process_scrap_waste_kg),
            3
        );
    END IF;
    
    -- Calculate twist variation if both target and tested are present
    IF NEW.target_tpi IS NOT NULL AND NEW.tested_tpm_average IS NOT NULL AND NEW.target_tpi > 0 THEN
        NEW.twist_variation_pct := ROUND(
            (ABS(NEW.target_tpi - NEW.tested_tpm_average) / NEW.target_tpi) * 100,
            2
        );
    END IF;
    
    -- Rule 1: 2400 Hook TPM Requirement
    -- IF target_machine = 2400_HOOK_JACQUARD AND final_twist_tpm < 700
    -- → BLOCK: DOWNGRADE_TO_1536_HOOK
    IF NEW.engineered_yarn_profile = 'ORGANZINE' AND NEW.final_twist_tpm IS NOT NULL AND NEW.final_twist_tpm < 700 THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'DOWNGRADE_TO_1536_HOOK',
                'message', format('Final twist TPM %s below 700 minimum for 2400 Hook Jacquard', NEW.final_twist_tpm),
                'severity', 'BLOCK'
            ));
    END IF;
    
    -- Rule 2: Twist Uniformity Flag
    -- IF twist_variation_pct > 3.0%
    -- → TRIGGER WARNING: WARP_BANDING_RISK
    IF NEW.twist_variation_pct IS NOT NULL AND NEW.twist_variation_pct > 3.0 THEN
        NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'WARP_BANDING_RISK',
                'message', format('Twist variation %s%% exceeds 3.0%% threshold. Risk of uneven shine on saree.', NEW.twist_variation_pct),
                'severity', 'WARNING'
            ));
    END IF;
    
    -- Rule 3: Torque / Snarl Rejection
    -- IF snarl_count_per_100m > 0
    -- → ROUTE: RE_STEAMING_REQUIRED
    IF NEW.snarl_count_per_100m IS NOT NULL AND NEW.snarl_count_per_100m > 0 THEN
        NEW.auto_assigned_routing := 'RE_STEAMING_REQUIRED';
        NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'RE_STEAMING_REQUIRED',
                'message', format('Snarl count %s per 100m detected. Active torque will cause loom stops.', NEW.snarl_count_per_100m),
                'severity', 'WARNING'
            ));
    END IF;
    
    -- Check material discrepancy > 1.5%
    IF NEW.material_discrepancy_kg IS NOT NULL AND NEW.input_weight_kg > 0 THEN
        IF ABS(NEW.material_discrepancy_kg / NEW.input_weight_kg) > 0.015 THEN
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'MATERIAL_VARIANCE_ALERT',
                    'message', format('Material discrepancy %s kg exceeds 1.5%% threshold. Investigate pilferage or machine calibration.', NEW.material_discrepancy_kg),
                    'severity', 'WARNING'
                ));
        END IF;
    END IF;
    
    -- Auto-assign routing if not already set
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'DOWNGRADE_TO_1536_HOOK';
        ELSIF NEW.engineered_yarn_profile = 'ORGANZINE' 
              AND NEW.final_twist_tpm >= 700
              AND NEW.twist_variation_pct <= 2.5 THEN
            NEW.auto_assigned_routing := 'TWISTED_WARP_READY';
        ELSIF NEW.engineered_yarn_profile = 'TRAM' THEN
            NEW.auto_assigned_routing := 'TWISTED_WEFT_READY';
        ELSIF NEW.engineered_yarn_profile = 'CREPE' THEN
            NEW.auto_assigned_routing := 'TWISTED_CREPE_READY';
        ELSE
            NEW.auto_assigned_routing := 'COMMERCIAL_SEMI_PREMIUM';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_calculate_throwster_derived_fields ON throwster_production_records;

CREATE TRIGGER trigger_calculate_throwster_derived_fields
    BEFORE INSERT OR UPDATE ON throwster_production_records
    FOR EACH ROW EXECUTE FUNCTION calculate_throwster_derived_fields();

-- ------------------------------------------------------------
-- 7. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_throwster_batches_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_throwster_batches_updated_at ON throwster_batches;

CREATE TRIGGER trigger_update_throwster_batches_updated_at
    BEFORE UPDATE ON throwster_batches
    FOR EACH ROW EXECUTE FUNCTION update_throwster_batches_updated_at();

CREATE OR REPLACE FUNCTION update_throwster_production_records_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_throwster_production_records_updated_at ON throwster_production_records;

CREATE TRIGGER trigger_update_throwster_production_records_updated_at
    BEFORE UPDATE ON throwster_production_records
    FOR EACH ROW EXECUTE FUNCTION update_throwster_production_records_updated_at();

-- ------------------------------------------------------------
-- 8. UPDATE SILK-DEGUMMING-MASTER ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","process:control","degumming:batch:create","degumming:batch:read","degumming:record:write","degumming:record:read","degumming:certificate:read","sales:forecast:read","throwster:batch:read"]',
    description = 'Silk Degumming Master: chemical bath formulation, thermal-process control, weight loss accounting, yarn preparation for dyeing, certificate issuance; provides pre-process material to Throwster/Twister'
WHERE role_id = 'ROLE-SILK-DEGUMMING-MASTER';
