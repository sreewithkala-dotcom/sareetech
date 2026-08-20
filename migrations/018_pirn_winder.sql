-- ============================================================
-- Migration: 018_pirn_winder.sql
-- Pirn Winder module
-- Weft pirn winding, chase control, density monitoring,
-- quality audit, and certification.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE PIRN-WINDERS ROLE
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:weaving","pirn:job:create","pirn:job:read","pirn:job:write","pirn:certificate:read","sales:forecast:read"]',
    description = 'Pirn Winder: precision taper control, density monitoring, knot formatting, shade/lot segregation, doffing/inspection, pirn certification; receives pre-process material from Bobbin Winder'
WHERE role_id = 'ROLE-PIRN-WINDERS';

-- ------------------------------------------------------------
-- 2. PIRN WINDING JOBS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pirn_winding_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pirn_winding_job_no VARCHAR(100) UNIQUE NOT NULL,
    bobbin_winder_job_card_id UUID REFERENCES winding_job_cards(id),
    bobbin_record_id UUID REFERENCES bobbin_records(id),
    master_colorist_recipe_id UUID REFERENCES master_colorist_recipes(id),
    master_colorist_certificate_id UUID REFERENCES master_colorist_certificates(id),
    skein_dye_job_id UUID REFERENCES skein_dye_jobs(id),
    skein_dye_certificate_id UUID REFERENCES skein_dye_certificates(id),
    throwster_record_id UUID REFERENCES throwster_production_records(id),
    throwster_batch_id UUID REFERENCES throwster_batches(id),
    production_lot_id UUID REFERENCES production_lots(id),
    factory_node_id VARCHAR(50) NOT NULL,
    operator_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'DRAFT' CHECK (status IN (
        'DRAFT', 'IN_PROGRESS', 'COMPLETED', 'QC_HOLD', 'REJECTED', 'CERTIFIED'
    )),
    
    -- Operations & Resource Metadata
    winding_machine_id VARCHAR(100),
    pirn_machine_type VARCHAR(50) CHECK (pirn_machine_type IN (
        'AUTOMATIC_HIGH_SPEED_PIRN_WINDER',
        'MANUAL_SPINDLE_WINDER',
        'PRECISION_FEEL_WHEEL_WINDER'
    )),
    
    -- Material & Output Intakes
    source_bobbin_lot_no VARCHAR(100) NOT NULL,
    target_pirn_count_qty INTEGER NOT NULL CHECK (target_pirn_count_qty > 0),
    input_yarn_weight_kg DECIMAL(10,3) NOT NULL,
    output_pirn_net_weight_kg DECIMAL(10,3),
    pirn_scrap_waste_gm DECIMAL(10,3) DEFAULT 0,
    material_variance_kg DECIMAL(10,3) GENERATED ALWAYS AS (
        input_yarn_weight_kg - (output_pirn_net_weight_kg + (pirn_scrap_waste_gm / 1000.0))
    ) STORED,
    
    -- Category A: Winding Setup & Machine Controls
    spindle_speed_rpm INTEGER CHECK (spindle_speed_rpm > 0),
    pirn_base_and_nose_taper_deg DECIMAL(5,2),
    weft_joint_method VARCHAR(50) CHECK (weft_joint_method IN (
        'AIR_SPLICING',
        'MICRO_MECHANICAL_JOIN',
        'STANDARD_WEAVERS_KNOT'
    )),
    
    -- Category B: Post-Winding Quality Audit
    pirn_hardness_shore_d DECIMAL(5,2),
    splices_per_pirn INTEGER DEFAULT 0,
    sloughing_risk_index VARCHAR(50) CHECK (sloughing_risk_index IN (
        'GRADE_5_ZERO_RISK_COMPACT',
        'GRADE_3_SLIGHT_LOOSE_COILS',
        'GRADE_1_HIGH_SLOUGH_RISK'
    )),
    pirn_surface_inspection VARCHAR(50) CHECK (pirn_surface_inspection IN (
        'SMOOTH_FLAWLESS',
        'ROUGH_CHAFED_FILAMENTS'
    )),
    
    -- Target machine type for validation
    target_machine_type VARCHAR(50) DEFAULT '1536_HOOK_JACQUARD' CHECK (target_machine_type IN (
        '1536_HOOK_JACQUARD',
        '2400_HOOK_JACQUARD',
        'HANDLOOM',
        'POWERLOOM',
        'RAPIER_LOOM'
    )),
    
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

CREATE INDEX IF NOT EXISTS idx_pirn_winding_jobs_code ON pirn_winding_jobs(pirn_winding_job_no);
CREATE INDEX IF NOT EXISTS idx_pirn_winding_jobs_factory ON pirn_winding_jobs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_pirn_winding_jobs_status ON pirn_winding_jobs(status);
CREATE INDEX IF NOT EXISTS idx_pirn_winding_jobs_bobbin ON pirn_winding_jobs(bobbin_record_id);
CREATE INDEX IF NOT EXISTS idx_pirn_winding_jobs_target_machine ON pirn_winding_jobs(target_machine_type);

-- ------------------------------------------------------------
-- 3. PIRN RECORDS (Output carriers produced from a job)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pirn_records (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pirn_id VARCHAR(100) UNIQUE NOT NULL,
    job_id UUID NOT NULL REFERENCES pirn_winding_jobs(id) ON DELETE CASCADE,
    operator_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    source_bobbin_lot_no VARCHAR(100) NOT NULL,
    yarn_type VARCHAR(50) NOT NULL,
    silk_fiber_variety VARCHAR(50),
    net_weight_kg DECIMAL(10,3) NOT NULL,
    pirn_hardness_shore_d DECIMAL(5,2),
    sloughing_risk_index VARCHAR(50),
    pirn_surface_inspection VARCHAR(50),
    weft_joint_method VARCHAR(50),
    splices_per_pirn INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'WEFT_SHUTTLE_READY' CHECK (status IN (
        'WEFT_SHUTTLE_READY',
        'REJECTED_RE_WINDING',
        'CONSUMED'
    )),
    qr_tag_id VARCHAR(100) UNIQUE,
    certificate_hash VARCHAR(255),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pirn_records_job ON pirn_records(job_id);
CREATE INDEX IF NOT EXISTS idx_pirn_records_operator ON pirn_records(operator_id);
CREATE INDEX IF NOT EXISTS idx_pirn_records_factory ON pirn_records(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_pirn_records_bobbin_lot ON pirn_records(source_bobbin_lot_no);

-- ------------------------------------------------------------
-- 4. PIRN WINDING CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pirn_winding_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID NOT NULL REFERENCES pirn_winding_jobs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    pirn_winding_job_no VARCHAR(100) NOT NULL,
    source_bobbin_lot_no VARCHAR(100),
    target_pirn_count_qty INTEGER,
    input_yarn_weight_kg DECIMAL(10,3),
    output_pirn_net_weight_kg DECIMAL(10,3),
    pirn_scrap_waste_gm DECIMAL(10,3),
    material_variance_kg DECIMAL(10,3),
    spindle_speed_rpm INTEGER,
    pirn_base_and_nose_taper_deg DECIMAL(5,2),
    weft_joint_method VARCHAR(50),
    pirn_hardness_shore_d DECIMAL(5,2),
    splices_per_pirn INTEGER,
    sloughing_risk_index VARCHAR(50),
    pirn_surface_inspection VARCHAR(50),
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

CREATE INDEX IF NOT EXISTS idx_pirn_winding_certificates_job ON pirn_winding_certificates(job_id);
CREATE INDEX IF NOT EXISTS idx_pirn_winding_certificates_hash ON pirn_winding_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_pirn_winding_certificates_qr ON pirn_winding_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 5. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_pirn_winding_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_pirn_winding_factory ON sales_forecast_pirn_winding_plan(factory_node_id);

-- ------------------------------------------------------------
-- 6. TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_pirn_winding_job()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1: 2400 Hook Splice Gate
    -- IF target_machine = 2400_HOOK_JACQUARD AND splices_per_pirn > 0
    -- → BLOCK: REJECT_FOR_2400_WEFT (NO_KNOTS_ALLOWED_IN_HIGH_DENSITY_WEFT)
    IF NEW.target_machine_type = '2400_HOOK_JACQUARD' THEN
        IF NEW.splices_per_pirn > 0 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'REJECT_FOR_2400_WEFT',
                    'message', format('Splices per pirn %s exceeds 0 for 2400 Hook Jacquard. No knots allowed in high-density weft.', NEW.splices_per_pirn),
                    'severity', 'BLOCK'
                ));
        END IF;
        
        -- Rule 2: Pirn Hardness Protection Guardrail
        -- IF target_machine = 2400_HOOK_JACQUARD AND pirn_hardness_shore_d < 70.0
        -- → TRIGGER WARNING: SOFT_PIRN_SLOUGHING_RISK
        IF NEW.pirn_hardness_shore_d IS NOT NULL AND NEW.pirn_hardness_shore_d < 70.0 THEN
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'SOFT_PIRN_SLOUGHING_RISK',
                    'message', format('Pirn hardness %s Shore D is below 70.0 minimum for 2400 Hook Jacquard. Sloughing risk.', NEW.pirn_hardness_shore_d),
                    'severity', 'WARNING'
                ));
        END IF;
        
        -- Spindle speed check for 2400 Hook (max 650 RPM)
        IF NEW.spindle_speed_rpm IS NOT NULL AND NEW.spindle_speed_rpm > 650 THEN
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'HIGH_SPINDLE_SPEED_FRICTION_RISK',
                    'message', format('Spindle speed %s RPM exceeds 650 RPM limit for 2400 Hook Jacquard. Risk of friction heat and fuzzing.', NEW.spindle_speed_rpm),
                    'severity', 'WARNING'
                ));
        END IF;
        
        -- Tension check for 2400 Hook (max 10g)
        IF NEW.weft_joint_method IS NOT NULL THEN
            -- Note: tension check would be done at the machine level
        END IF;
    END IF;
    
    -- Rule 3: Sloughing Risk Rejection
    -- IF sloughing_risk_index IN (Grade 3, Grade 1)
    -- → ROUTE: REJECT_NEEDS_RE_WINDING
    IF NEW.sloughing_risk_index IN ('GRADE_3_SLIGHT_LOOSE_COILS', 'GRADE_1_HIGH_SLOUGH_RISK') THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'REJECT_NEEDS_RE_WINDING',
                'message', format('Sloughing risk index %s indicates unacceptable pirn quality. Reject for re-winding.', NEW.sloughing_risk_index),
                'severity', 'BLOCK'
            ));
    END IF;
    
    -- Material variance check for all machines
    IF NEW.input_yarn_weight_kg IS NOT NULL AND NEW.input_yarn_weight_kg > 0 
       AND NEW.output_pirn_net_weight_kg IS NOT NULL THEN
        DECLARE
            variance_pct DECIMAL(5,2);
        BEGIN
            variance_pct := ROUND(
                (NEW.input_yarn_weight_kg - NEW.output_pirn_net_weight_kg - (NEW.pirn_scrap_waste_gm / 1000.0)) 
                / NEW.input_yarn_weight_kg * 100, 2
            );
            IF variance_pct > 0.3 THEN
                NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                    jsonb_build_array(jsonb_build_object(
                        'code', 'MATERIAL_VARIANCE_LEAKAGE',
                        'message', format('Material variance %s%% exceeds 0.3%% threshold. Possible theft or scale calibration error.', variance_pct),
                        'severity', 'BLOCK'
                    ));
            END IF;
        END;
    END IF;
    
    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'REJECTED_RE_WINDING';
        ELSIF NEW.target_machine_type = '2400_HOOK_JACQUARD' 
              AND NEW.splices_per_pirn = 0
              AND NEW.pirn_hardness_shore_d >= 70.0
              AND NEW.sloughing_risk_index = 'GRADE_5_ZERO_RISK_COMPACT'
              AND NEW.spindle_speed_rpm <= 650 THEN
            NEW.auto_assigned_routing := 'WEFT_SHUTTLE_READY';
        ELSIF NEW.target_machine_type = '1536_HOOK_JACQUARD'
              AND NEW.splices_per_pirn <= 1 THEN
            NEW.auto_assigned_routing := 'WEFT_SHUTTLE_READY';
        ELSE
            NEW.auto_assigned_routing := 'WEFT_SHUTTLE_READY';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_pirn_winding_job ON pirn_winding_jobs;

CREATE TRIGGER trigger_validate_pirn_winding_job
    BEFORE INSERT OR UPDATE ON pirn_winding_jobs
    FOR EACH ROW EXECUTE FUNCTION validate_pirn_winding_job();

-- ------------------------------------------------------------
-- 7. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_pirn_winding_jobs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_pirn_winding_jobs_updated_at ON pirn_winding_jobs;

CREATE TRIGGER trigger_update_pirn_winding_jobs_updated_at
    BEFORE UPDATE ON pirn_winding_jobs
    FOR EACH ROW EXECUTE FUNCTION update_pirn_winding_jobs_updated_at();

-- ------------------------------------------------------------
-- 8. UPDATE BOBBIN-WINDER ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari","winding:log:write","winding:read","winding:job:create","winding:job:read","winding:bobbin:write","winding:certificate:read","pirn:job:read","sales:forecast:read"]',
    description = 'Bobbin Winder: skein opening, tension control, piecing/knot management, bobbin build profile, color/lot segregation, quality audit, certificate issuance; provides pre-process material to Pirn Winder'
WHERE role_id = 'ROLE-BOBBIN-WINDER';
