-- ============================================================
-- Migration: 016_skein_dye_master.sql
-- Skein Dye Master module
-- Physical dye bath execution, temperature profiling, chemical
-- additive management, post-dye washing, drying, and certification.
-- ============================================================

-- ------------------------------------------------------------
-- 1. CREATE SKEIN-DYE-MASTER ROLE
-- ------------------------------------------------------------

INSERT INTO roles (role_id, role_name, permitted_operations, description)
VALUES (
    'ROLE-SKEIN-DYE-MASTER',
    'Skein Dye Master',
    '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:dye","skein:job:create","skein:job:read","skein:job:write","skein:certificate:read","sales:forecast:read"]',
    'Skein Dye Master: hank loading, temperature profiling, chemical additive management, post-dye washing, drying, floor material accounting, skein certification'
) ON CONFLICT (role_id) DO UPDATE SET
    permitted_operations = EXCLUDED.permitted_operations,
    description = EXCLUDED.description;

-- ------------------------------------------------------------
-- 2. SKEIN DYE JOBS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS skein_dye_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id VARCHAR(100) UNIQUE NOT NULL,
    master_colorist_recipe_id UUID REFERENCES master_colorist_recipes(id),
    master_colorist_certificate_id UUID REFERENCES master_colorist_certificates(id),
    throwster_record_id UUID REFERENCES throwster_production_records(id),
    throwster_batch_id UUID REFERENCES throwster_batches(id),
    production_lot_id UUID REFERENCES production_lots(id),
    factory_node_id VARCHAR(50) NOT NULL,
    operator_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'DRAFT' CHECK (status IN (
        'DRAFT', 'IN_PROGRESS', 'COMPLETED', 'QC_HOLD', 'REJECTED', 'CERTIFIED'
    )),
    
    -- Machine & Process Logs
    allocated_machine_id VARCHAR(100),
    vessel_type_allocated VARCHAR(50) CHECK (vessel_type_allocated IN (
        'AUTOMATED_SKEIN_CABINET_CLOSED',
        'HANK_DYE_MOVING_ARM_OPEN',
        'WINCH_DYEING_BECK',
        'TRADITIONAL_WOODEN_VAT_MANUAL',
        'STAINLESS_STEEL_GAS_KETTLE'
    )),
    operator_name VARCHAR(255),
    actual_liquor_volume_liters DECIMAL(10,2),
    
    -- Temperature & Timing Checklist
    bath_start_time TIMESTAMP WITH TIME ZONE,
    bath_end_time TIMESTAMP WITH TIME ZONE,
    peak_boil_temperature_celsius INTEGER CHECK (peak_boil_temperature_celsius BETWEEN 0 AND 120),
    fixation_duration_minutes INTEGER,
    
    -- Hank Loading & Machine Setup
    hank_unit_weight_g INTEGER CHECK (hank_unit_weight_g > 0),
    lease_tie_type VARCHAR(50) CHECK (lease_tie_type IN (
        'STANDARD_COTTON_STRING',
        'FIGURE_8_POLY_SOFT_TIE',
        'ELASTICIZED_SOFT_BAND'
    )),
    machine_type VARCHAR(50) CHECK (machine_type IN (
        'MANUAL_OPEN_VAT',
        'ARM_TYPE_CABINET_HANK_MACHINE',
        'PULSATOR_CABINET_MACHINE'
    )),
    pump_flow_rate_lpm DECIMAL(10,2),
    
    -- Liquor Ratio & Water Treatment
    liquor_ratio VARCHAR(50) CHECK (liquor_ratio IN (
        '1_15_ULTRA_LOW_RATIO',
        '1_20_LOW_RATIO',
        '1_30_MEDIUM_RATIO',
        '1_40_HIGH_RATIO',
        '1_50_MAX_DILUTION'
    )),
    pre_boil_hardness_ppm INTEGER,
    water_treatment_status VARCHAR(50) CHECK (water_treatment_status IN (
        'RO_FILTERED_PURE',
        'SOFTENED_ION_EXCHANGE',
        'RAW_BOREWELL_UNTREATED',
        'MUNICIPAL_CHLORINATED'
    )),
    
    -- Peak Heating Temperature Bracket
    peak_heating_temperature_bracket VARCHAR(50) CHECK (peak_heating_temperature_bracket IN (
        '70C_75C_LOW_TEMP',
        '80C_85C_STANDARD',
        '86C_90C_HIGH_BOIL',
        '91C_95C_CRITICAL',
        '96C_100C_OVER_BOIL'
    )),
    
    -- Material In/Out & Inventory Balances
    input_skein_dry_weight_kg DECIMAL(10,3),
    output_skein_dry_weight_kg DECIMAL(10,3),
    dye_house_yield_variance DECIMAL(10,3),
    
    -- Post-Dye Softening
    post_dye_softening_type VARCHAR(50) CHECK (post_dye_softening_type IN (
        'CATIONIC_FATTY_AMIDE_EMULSION',
        'AMINO_FUNCTIONAL_SILICONE',
        'HYDROPHILIC_SILICONE_SOFTENER',
        'NATURAL_COCONUT_OIL_STARCH_BLEND',
        'POLYURETHANE_ELASTOMERIC_FINISH',
        'NONE'
    )),
    
    -- Quality Audit
    core_to_surface_shade_match VARCHAR(50) CHECK (core_to_surface_shade_match IN (
        'PASS_100_PENETRATION',
        'FAIL_LIGHTER_CORE_SHADE'
    )),
    tie_mark_spot_found BOOLEAN DEFAULT FALSE,
    post_dye_winding_break_count INTEGER DEFAULT 0,
    hank_entanglement_rating VARCHAR(50) CHECK (hank_entanglement_rating IN (
        'GRADE_5_FREE_FLOWING',
        'GRADE_3_SLIGHT_TANGLING',
        'GRADE_1_SEVERELY_MATTED'
    )),
    
    -- Recipe Scaler Logic (auto-calculated)
    recipe_scaler_multiplier DECIMAL(10,3) DEFAULT 1.0,
    
    -- Automated Validation
    validation_errors JSONB DEFAULT '[]'::jsonb,
    validation_warnings JSONB DEFAULT '[]'::jsonb,
    auto_assigned_routing VARCHAR(50),
    
    -- Certification
    certificate_hash VARCHAR(255),
    qr_tag_id VARCHAR(100) UNIQUE,
    
    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_skein_dye_jobs_code ON skein_dye_jobs(job_id);
CREATE INDEX IF NOT EXISTS idx_skein_dye_jobs_factory ON skein_dye_jobs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_skein_dye_jobs_status ON skein_dye_jobs(status);
CREATE INDEX IF NOT EXISTS idx_skein_dye_jobs_recipe ON skein_dye_jobs(master_colorist_recipe_id);

-- ------------------------------------------------------------
-- 3. SKEIN DYE JOB CHEMICAL CONSUMPTION
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS skein_dye_job_chemicals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID NOT NULL REFERENCES skein_dye_jobs(id) ON DELETE CASCADE,
    chemical_name VARCHAR(255) NOT NULL,
    quantity_grams DECIMAL(10,3) NOT NULL,
    volume_ml DECIMAL(10,3),
    component_type VARCHAR(50) CHECK (component_type IN (
        'DYE_COMPONENT',
        'FIXING_AGENT',
        'ACID_BUFFER',
        'LEVELING_ADDITIVE',
        'SOFTENER',
        'ANTISTATIC_AGENT'
    )),
    sequence_order INTEGER,
    auto_requisitioned BOOLEAN DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_skein_dye_job_chemicals_job ON skein_dye_job_chemicals(job_id);

-- ------------------------------------------------------------
-- 4. SKEIN DYE CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS skein_dye_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID NOT NULL REFERENCES skein_dye_jobs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    job_id_ref VARCHAR(100) NOT NULL,
    allocated_machine_id VARCHAR(100),
    vessel_type_allocated VARCHAR(50),
    peak_boil_temperature_celsius INTEGER,
    fixation_duration_minutes INTEGER,
    input_skein_dry_weight_kg DECIMAL(10,3),
    output_skein_dry_weight_kg DECIMAL(10,3),
    dye_house_yield_variance DECIMAL(10,3),
    core_to_surface_shade_match VARCHAR(50),
    tie_mark_spot_found BOOLEAN,
    post_dye_winding_break_count INTEGER,
    hank_entanglement_rating VARCHAR(50),
    post_dye_softening_type VARCHAR(50),
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

CREATE INDEX IF NOT EXISTS idx_skein_dye_certificates_job ON skein_dye_certificates(job_id);
CREATE INDEX IF NOT EXISTS idx_skein_dye_certificates_hash ON skein_dye_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_skein_dye_certificates_qr ON skein_dye_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 5. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_skein_dye_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_skein_dye_factory ON sales_forecast_skein_dye_plan(factory_node_id);

-- ------------------------------------------------------------
-- 6. TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_skein_dye_job()
RETURNS TRIGGER AS $$
DECLARE
    target_fixation_minutes INTEGER;
BEGIN
    -- Rule 1: 2400 Hook Winding Quality Check
    -- IF target_machine = 2400_HOOK_JACQUARD AND post_dye_winding_break_count > 1
    -- → BLOCK: REJECT_FOR_2400_WARP (HIGH_KNOT_COUNT_WILL_JAM_REED)
    IF NEW.target_machine_type = '2400_HOOK_JACQUARD' THEN
        IF NEW.post_dye_winding_break_count > 1 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'REJECT_FOR_2400_WARP',
                    'message', format('Winding break count %s exceeds 1 for 2400 Hook Jacquard. High knot count will jam reed.', NEW.post_dye_winding_break_count),
                    'severity', 'BLOCK'
                ));
        END IF;
        
        -- Rule 2: Hank Weight Restriction for High Density
        -- IF target_machine = 2400_HOOK_JACQUARD AND hank_unit_weight_g > 300
        -- → TRIGGER WARNING: RE_SKEIN_TO_SMALLER_HANKS
        IF NEW.hank_unit_weight_g > 300 THEN
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'RE_SKEIN_TO_SMALLER_HANKS',
                    'message', format('Hank weight %sg exceeds 300g limit for 2400 Hook. Core dye penetration risk.', NEW.hank_unit_weight_g),
                    'severity', 'WARNING'
                ));
        END IF;
    END IF;
    
    -- Rule 3: Tie-Mark Defect Guardrail
    -- IF tie_mark_spot_found = True
    -- → ROUTE: QC_REJECT_HOLD / DOWNGRADE_TO_WEFT_ONLY
    IF NEW.tie_mark_spot_found = TRUE THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'QC_REJECT_HOLD',
                'message', 'Tie-mark spot found. Downgrading to WEFT_ONLY or QC_REJECT_HOLD.',
                'severity', 'BLOCK'
            ));
    END IF;
    
    -- Over-Boiling Strength Check
    -- IF fixation_duration_minutes exceeds recipe target by >15 minutes
    -- → TRIGGER WARNING: MATERIAL_STRESS_RISK
    IF NEW.fixation_duration_minutes IS NOT NULL AND NEW.master_colorist_recipe_id IS NOT NULL THEN
        SELECT mcr.fixation_duration_minutes INTO target_fixation_minutes
        FROM master_colorist_recipes mcr
        WHERE mcr.id = NEW.master_colorist_recipe_id;
        
        IF target_fixation_minutes IS NOT NULL 
           AND NEW.fixation_duration_minutes > target_fixation_minutes + 15 THEN
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'MATERIAL_STRESS_RISK',
                    'message', format('Fixation duration %s exceeds recipe target %s by >15 min. Over-boiling risk.', 
                        NEW.fixation_duration_minutes, target_fixation_minutes),
                    'severity', 'WARNING'
                ));
        END IF;
    END IF;
    
    -- Water Hardness Check
    IF NEW.pre_boil_hardness_ppm IS NOT NULL AND NEW.pre_boil_hardness_ppm > 50 THEN
        NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'HARD_WATER_WARNING',
                'message', format('Pre-boil hardness %s ppm exceeds 50 ppm. Risk of uneven dye precipitation.', NEW.pre_boil_hardness_ppm),
                'severity', 'WARNING'
            ));
    END IF;
    
    -- Calculate dye house yield variance
    IF NEW.input_skein_dry_weight_kg IS NOT NULL AND NEW.output_skein_dry_weight_kg IS NOT NULL THEN
        NEW.dye_house_yield_variance := NEW.output_skein_dry_weight_kg - NEW.input_skein_dry_weight_kg;
    END IF;
    
    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'QC_REJECT_HOLD';
        ELSIF NEW.target_machine_type = '2400_HOOK_JACQUARD' 
              AND NEW.post_dye_winding_break_count <= 1
              AND NEW.hank_unit_weight_g <= 300
              AND NEW.tie_mark_spot_found = FALSE THEN
            NEW.auto_assigned_routing := 'WARP_PREMIUM_2400_HOOK_READY';
        ELSIF NEW.target_machine_type = '1536_HOOK_JACQUARD'
              AND NEW.post_dye_winding_break_count <= 3 THEN
            NEW.auto_assigned_routing := 'WARP_PREMIUM_1536_HOOK_READY';
        ELSE
            NEW.auto_assigned_routing := 'WEFT_ONLY_APPROVED';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_skein_dye_job ON skein_dye_jobs;

CREATE TRIGGER trigger_validate_skein_dye_job
    BEFORE INSERT OR UPDATE ON skein_dye_jobs
    FOR EACH ROW EXECUTE FUNCTION validate_skein_dye_job();

-- ------------------------------------------------------------
-- 7. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_skein_dye_jobs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_skein_dye_jobs_updated_at ON skein_dye_jobs;

CREATE TRIGGER trigger_update_skein_dye_jobs_updated_at
    BEFORE UPDATE ON skein_dye_jobs
    FOR EACH ROW EXECUTE FUNCTION update_skein_dye_jobs_updated_at();

-- ------------------------------------------------------------
-- 8. UPDATE MASTER-COLORIST ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:dye","color:recipe:create","color:recipe:read","color:recipe:write","color:recipe:approve","color:certificate:read","skein:job:read","sales:forecast:read"]',
    description = 'Master Colorist: shade matching, chemical recipe formulation, colorfastness engineering, spectrophotometer QA, recipe approval, color certification; provides pre-process material to Skein Dye Master'
WHERE role_id = 'ROLE-MASTER-COLORIST';
