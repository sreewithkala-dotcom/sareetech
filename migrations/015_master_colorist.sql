-- ============================================================
-- Migration: 015_master_colorist.sql
-- Master Colorist module
-- Color recipe formulation, shade matching, chemical kitchen,
-- spectrophotometer QA, and color certification.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE MASTER-COLORIST ROLE
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:dye","color:recipe:create","color:recipe:read","color:recipe:write","color:recipe:approve","color:certificate:read","sales:forecast:read"]',
    description = 'Master Colorist: shade matching, chemical recipe formulation, colorfastness engineering, spectrophotometer QA, recipe approval, color certification'
WHERE role_id = 'ROLE-MASTER-COLORIST';

-- ------------------------------------------------------------
-- 2. MASTER COLORIST RECIPES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS master_colorist_recipes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recipe_code VARCHAR(100) UNIQUE NOT NULL,
    internal_shade_code VARCHAR(100) NOT NULL,
    pantone_reference_id VARCHAR(100),
    silk_origin_type_suitability VARCHAR(50) NOT NULL CHECK (silk_origin_type_suitability IN (
        'BIVOLTINE_WHITE_ONLY',
        'MULTIVOLTINE_YELLOW_ONLY',
        'UNIVERSAL_BLENDED',
        'DUPION_COARSE_YARN',
        'TUSSAR_WILD_SILK',
        'ERI_WOOLEN_SILK',
        'MUGA_GOLDEN_SILK'
    )),
    liquor_ratio VARCHAR(50) NOT NULL CHECK (liquor_ratio IN (
        '1_30_STANDARD',
        '1_40_GENTLE_HIGH_VOLUME',
        '1_50_ULTRA_FINE_YARN'
    )),
    throwster_record_id UUID REFERENCES throwster_production_records(id),
    throwster_batch_id UUID REFERENCES throwster_batches(id),
    production_lot_id UUID REFERENCES production_lots(id),
    factory_node_id VARCHAR(50) NOT NULL,
    operator_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'DRAFT' CHECK (status IN (
        'DRAFT', 'LAB_DIP_PENDING', 'APPROVED', 'SHADE_REJECTED', 'CERTIFIED', 'QC_HOLD'
    )),
    version INTEGER DEFAULT 1,
    delta_e_value DECIMAL(5,2),
    light_source_profile JSONB DEFAULT '[]'::jsonb,
    cie_lab_coordinates JSONB DEFAULT '{}'::jsonb,
    dye_class_used VARCHAR(50) CHECK (dye_class_used IN (
        'ACID_LEV_EQUALISING',
        'ACID_MILL_MILLING',
        'ACID_SUPER_MIL_PRE_METALLISED_1_1',
        'ACID_PRE_METALLISED_1_2',
        'REACTIVE_CIBACRON_F',
        'REACTIVE_REMAZOL_VINYLSULFONE',
        'NATURAL_INDIGO_PLANT',
        'NATURAL_MADDER_ROOT',
        'NATURAL_TURMERIC_MARIGOLD'
    )),
    dyebath_ph DECIMAL(4,2),
    max_temperature_celsius INTEGER CHECK (max_temperature_celsius BETWEEN 0 AND 120),
    leveling_agent_added BOOLEAN DEFAULT FALSE,
    color_difference_delta_e DECIMAL(5,2),
    dry_crocking_fastness VARCHAR(20) CHECK (dry_crocking_fastness IN (
        'GRADE_5_NO_TRANSFER', 'GRADE_4', 'GRADE_3', 'GRADE_1_2_SEVERE_TRANSFER'
    )),
    wet_crocking_fastness VARCHAR(20) CHECK (wet_crocking_fastness IN (
        'GRADE_5', 'GRADE_4', 'GRADE_3', 'GRADE_1_2_SEVERE_TRANSFER'
    )),
    post_dye_tenacity_gd DECIMAL(5,2),
    antistatic_lubricant_applied BOOLEAN DEFAULT FALSE,
    acid_fixative_type VARCHAR(50) CHECK (acid_fixative_type IN (
        'ACETIC_ACID_GLACIAL_99PCT',
        'ACETIC_ACID_DILUTE_50PCT',
        'FORMIC_ACID_85PCT',
        'AMMONIUM_SULFATE_SALT',
        'CITRIC_ACID_POWDER',
        'SULFURIC_ACID_DILUTE'
    )),
    leveling_exhausting_agent VARCHAR(50) CHECK (leveling_exhausting_agent IN (
        'GLAUBER_SALT_ANHYDROUS',
        'COMMON_SALT_SODIUM_CHLORIDE',
        'NON_IONIC_ETHOXYLATED_AMINE',
        'ANIONIC_ALKYL_SULFATE',
        'CATIONIC_DYE_FIXING_POLYMER',
        'NONE'
    )),
    validation_errors JSONB DEFAULT '[]'::jsonb,
    validation_warnings JSONB DEFAULT '[]'::jsonb,
    auto_assigned_routing VARCHAR(50),
    certificate_hash VARCHAR(255),
    qr_tag_id VARCHAR(100) UNIQUE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_master_colorist_recipes_code ON master_colorist_recipes(recipe_code);
CREATE INDEX IF NOT EXISTS idx_master_colorist_recipes_factory ON master_colorist_recipes(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_master_colorist_recipes_status ON master_colorist_recipes(status);
CREATE INDEX IF NOT EXISTS idx_master_colorist_recipes_throwster ON master_colorist_recipes(throwster_record_id);

-- ------------------------------------------------------------
-- 3. RECIPE CHEMICAL COMPONENTS (Dynamic Grid)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS recipe_chemical_components (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recipe_id UUID NOT NULL REFERENCES master_colorist_recipes(id) ON DELETE CASCADE,
    component_type VARCHAR(50) NOT NULL CHECK (component_type IN (
        'DYE_COMPONENT',
        'FIXING_AGENT',
        'ACID_BUFFER',
        'LEVELING_ADDITIVE',
        'SOFTENER',
        'ANTISTATIC_AGENT'
    )),
    chemical_name VARCHAR(255) NOT NULL,
    quantity_grams DECIMAL(10,3) NOT NULL,
    volume_ml DECIMAL(10,3),
    sequence_order INTEGER,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_recipe_components_recipe ON recipe_chemical_components(recipe_id);

-- ------------------------------------------------------------
-- 4. MASTER COLORIST CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS master_colorist_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recipe_id UUID NOT NULL REFERENCES master_colorist_recipes(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    internal_shade_code VARCHAR(100) NOT NULL,
    pantone_reference_id VARCHAR(100),
    certified_grade VARCHAR(10) DEFAULT '4A',
    auto_assigned_routing VARCHAR(50) NOT NULL,
    dye_class_used VARCHAR(50),
    liquor_ratio VARCHAR(50),
    delta_e_value DECIMAL(5,2),
    dry_crocking_fastness VARCHAR(20),
    wet_crocking_fastness VARCHAR(20),
    post_dye_tenacity_gd DECIMAL(5,2),
    antistatic_lubricant_applied BOOLEAN,
    throwster_record_id UUID REFERENCES throwster_production_records(id),
    throwster_batch_id UUID REFERENCES throwster_batches(id),
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

CREATE INDEX IF NOT EXISTS idx_master_colorist_certificates_recipe ON master_colorist_certificates(recipe_id);
CREATE INDEX IF NOT EXISTS idx_master_colorist_certificates_hash ON master_colorist_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_master_colorist_certificates_qr ON master_colorist_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 5. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_colorist_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_colorist_factory ON sales_forecast_colorist_plan(factory_node_id);

-- ------------------------------------------------------------
-- 6. TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_master_colorist_recipe()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1: 2400 Hook Shade Precision Check
    -- IF target_machine = 2400_HOOK_JACQUARD AND color_difference_delta_e > 0.5
    -- → BLOCK: DOWNGRADE_TO_WEFT_OR_RE_DYE
    IF NEW.target_machine_type = '2400_HOOK_JACQUARD' THEN
        IF NEW.color_difference_delta_e IS NOT NULL AND NEW.color_difference_delta_e > 0.5 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'DOWNGRADE_TO_WEFT_OR_RE_DYE',
                    'message', format('Delta-E %s exceeds 0.5 strict limit for 2400 Hook Jacquard. Warp streak risk.', NEW.color_difference_delta_e),
                    'severity', 'BLOCK'
                ));
        END IF;
        
        -- Rule 2: Tenacity Loss & Heat Damage Guardrail
        -- IF target_machine = 2400_HOOK_JACQUARD AND post_dye_tenacity_gd < 3.8
        -- → ROUTE: REJECT_FOR_2400_WARP
        IF NEW.post_dye_tenacity_gd IS NOT NULL AND NEW.post_dye_tenacity_gd < 3.8 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'REJECT_FOR_2400_WARP',
                    'message', format('Post-dye tenacity %s g/d below 3.8 minimum for 2400 Hook Jacquard. Fiber weakened in dye bath.', NEW.post_dye_tenacity_gd),
                    'severity', 'BLOCK'
                ));
        END IF;
        
        -- Rule 3: Mandatory Finish Verification
        -- IF target_machine = 2400_HOOK_JACQUARD AND antistatic_lubricant_applied = False
        -- → BLOCK: CANNOT_DISPATCH_TO_WARPING
        IF NOT NEW.antistatic_lubricant_applied THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'CANNOT_DISPATCH_TO_WARPING',
                    'message', 'Antistatic lubricant not applied. Mandatory friction shield missing for 2400 Hook warp.',
                    'severity', 'BLOCK'
                ));
        END IF;
        
        -- pH check for 2400 Hook (strict buffer 5.0-5.5)
        IF NEW.dyebath_ph IS NOT NULL AND (NEW.dyebath_ph < 5.0 OR NEW.dyebath_ph > 5.5) THEN
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'PH_OUT_OF_STRICT_RANGE_2400',
                    'message', format('Dye bath pH %s is outside strict 5.0-5.5 range for 2400 Hook Jacquard', NEW.dyebath_ph),
                    'severity', 'WARNING'
                ));
        END IF;
        
        -- Temperature check for 2400 Hook (max 90°C)
        IF NEW.max_temperature_celsius IS NOT NULL AND NEW.max_temperature_celsius > 90 THEN
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'TEMPERATURE_TOO_HIGH_FOR_2400',
                    'message', format('Max temperature %s°C exceeds 90°C limit for 2400 Hook Jacquard', NEW.max_temperature_celsius),
                    'severity', 'WARNING'
                ));
        END IF;
    END IF;
    
    -- General pH validation
    IF NEW.dyebath_ph IS NOT NULL AND (NEW.dyebath_ph < 4.5 OR NEW.dyebath_ph > 6.0) THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'PH_OUT_OF_RANGE',
                'message', format('Dye bath pH %s is outside acceptable range 4.5-6.0', NEW.dyebath_ph),
                'severity', 'BLOCK'
            ));
    END IF;
    
    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'SHADE_REJECTED';
        ELSIF NEW.target_machine_type = '2400_HOOK_JACQUARD' 
              AND NEW.color_difference_delta_e <= 0.5
              AND NEW.post_dye_tenacity_gd >= 3.8
              AND NEW.antistatic_lubricant_applied THEN
            NEW.auto_assigned_routing := 'WARP_PREMIUM_2400_HOOK_READY';
        ELSIF NEW.target_machine_type = '1536_HOOK_JACQUARD'
              AND NEW.color_difference_delta_e <= 1.0 THEN
            NEW.auto_assigned_routing := 'WARP_PREMIUM_1536_HOOK_READY';
        ELSIF NEW.color_difference_delta_e <= 1.0 THEN
            NEW.auto_assigned_routing := 'WEFT_ONLY_APPROVED';
        ELSE
            NEW.auto_assigned_routing := 'SHADE_REJECTED';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_master_colorist_recipe ON master_colorist_recipes;

CREATE TRIGGER trigger_validate_master_colorist_recipe
    BEFORE INSERT OR UPDATE ON master_colorist_recipes
    FOR EACH ROW EXECUTE FUNCTION validate_master_colorist_recipe();

-- ------------------------------------------------------------
-- 7. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_master_colorist_recipes_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_master_colorist_recipes_updated_at ON master_colorist_recipes;

CREATE TRIGGER trigger_update_master_colorist_recipes_updated_at
    BEFORE UPDATE ON master_colorist_recipes
    FOR EACH ROW EXECUTE FUNCTION update_master_colorist_recipes_updated_at();

-- ------------------------------------------------------------
-- 8. UPDATE THROWSTER-TWISTER ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari","throwster:batch:create","throwster:batch:read","throwster:record:write","throwster:record:read","throwster:certificate:read","sales:forecast:read","color:recipe:read"]',
    description = 'Throwster/Twister: yarn ply doubling, TPI control, twist setting/steaming, defect elimination, certificate issuance; provides pre-process material to Master Colorist'
WHERE role_id = 'ROLE-THROWSTER-TWISTER';
