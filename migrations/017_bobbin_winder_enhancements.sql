-- ============================================================
-- Migration: 017_bobbin_winder_enhancements.sql
-- Bobbin Winder module enhancements
-- Adds 1536/2400 hook validation rules, sales forecast plugin,
-- and pre-process linkage from Skein Dye Master.
-- ============================================================

-- ------------------------------------------------------------
-- 1. ADD MISSING FIELDS TO WINDING_JOB_CARDS
-- ------------------------------------------------------------

ALTER TABLE winding_job_cards
    ADD COLUMN IF NOT EXISTS skein_dye_job_id UUID REFERENCES skein_dye_jobs(id),
    ADD COLUMN IF NOT EXISTS skein_dye_certificate_id UUID REFERENCES skein_dye_certificates(id),
    ADD COLUMN IF NOT EXISTS master_colorist_recipe_id UUID REFERENCES master_colorist_recipes(id),
    ADD COLUMN IF NOT EXISTS master_colorist_certificate_id UUID REFERENCES master_colorist_certificates(id),
    ADD COLUMN IF NOT EXISTS throwster_record_id UUID REFERENCES throwster_production_records(id),
    ADD COLUMN IF NOT EXISTS throwster_batch_id UUID REFERENCES throwster_batches(id),
    ADD COLUMN IF NOT EXISTS production_lot_id UUID REFERENCES production_lots(id),
    ADD COLUMN IF NOT EXISTS target_machine_type VARCHAR(50) DEFAULT '1536_HOOK_JACQUARD' CHECK (target_machine_type IN (
        '1536_HOOK_JACQUARD',
        '2400_HOOK_JACQUARD',
        'HANDLOOM',
        'POWERLOOM',
        'RAPIER_LOOM'
    )),
    ADD COLUMN IF NOT EXISTS joint_method_used VARCHAR(50) CHECK (joint_method_used IN (
        'STANDARD_WEAVERS_KNOT',
        'AUTOMATED_AIR_SPLICED_JOIN',
        'MICRO_MECHANICAL_KNOT',
        'FISHERMANS_KNOT',
        'ILLEGAL_OVERHAND_KNOT'
    )),
    ADD COLUMN IF NOT EXISTS auto_assigned_routing VARCHAR(50),
    ADD COLUMN IF NOT EXISTS validation_warnings JSONB DEFAULT '[]'::jsonb;

CREATE INDEX IF NOT EXISTS idx_winding_job_cards_skein_dye ON winding_job_cards(skein_dye_job_id);
CREATE INDEX IF NOT EXISTS idx_winding_job_cards_colorist ON winding_job_cards(master_colorist_recipe_id);
CREATE INDEX IF NOT EXISTS idx_winding_job_cards_target_machine ON winding_job_cards(target_machine_type);

-- ------------------------------------------------------------
-- 2. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_winding_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_winding_factory ON sales_forecast_winding_plan(factory_node_id);

-- ------------------------------------------------------------
-- 3. ENHANCED VALIDATION TRIGGER
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_winding_job_card()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1: 2400 Hook Joint Method Gate
    -- IF target_machine = 2400_HOOK_JACQUARD AND joint_method_used = Standard Weaver's Knot
    -- → BLOCK: REJECT_FOR_2400_WARP (KNOTS_WILL_JAM_FINE_REED)
    IF NEW.target_machine_type = '2400_HOOK_JACQUARD' THEN
        IF NEW.joint_method_used = 'STANDARD_WEAVERS_KNOT' THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'REJECT_FOR_2400_WARP',
                    'message', 'Standard Weaver''s Knot is not allowed for 2400 Hook Jacquard. Use Air Splicing or Micro Mechanical Knot.',
                    'severity', 'BLOCK'
                ));
        END IF;
        
        -- Rule 2: Winding Speed Protection Guardrail
        -- IF target_machine = 2400_HOOK_JACQUARD AND winding_speed_mpm > 200
        -- → TRIGGER WARNING: HIGH_WINDING_SPEED_FRICTION_RISK
        IF NEW.winding_speed_mpm IS NOT NULL AND NEW.winding_speed_mpm > 200 THEN
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'HIGH_WINDING_SPEED_FRICTION_RISK',
                    'message', format('Winding speed %s m/min exceeds 200 m/min limit for 2400 Hook Jacquard. Risk of friction heat and fraying.', NEW.winding_speed_mpm),
                    'severity', 'WARNING'
                ));
        END IF;
        
        -- Rule 3: Splice Count Limit
        -- IF target_machine = 2400_HOOK_JACQUARD AND splice_count_per_bobbin > 1
        -- → ROUTE: DOWNGRADE_TO_1536_HOOK_OR_WEFT
        IF NEW.splice_count_per_bobbin > 1 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'DOWNGRADE_TO_1536_HOOK_OR_WEFT',
                    'message', format('Splice count %s exceeds 1 for 2400 Hook Jacquard. Downgrade to 1536 Hook or Weft.', NEW.splice_count_per_bobbin),
                    'severity', 'BLOCK'
                ));
        END IF;
        
        -- Additional 2400 Hook checks
        IF NEW.applied_tension_grams IS NOT NULL AND NEW.applied_tension_grams > 6 THEN
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'TENSION_TOO_HIGH_FOR_2400',
                    'message', format('Applied tension %sg exceeds 6g limit for 2400 Hook Jacquard. Risk of elasticity loss.', NEW.applied_tension_grams),
                    'severity', 'WARNING'
                ));
        END IF;
    END IF;
    
    -- Waste variance check for all machines
    IF NEW.allocated_input_weight_kg IS NOT NULL AND NEW.allocated_input_weight_kg > 0 THEN
        DECLARE
            waste_pct DECIMAL(5,2);
        BEGIN
            waste_pct := ROUND((NEW.winding_scrap_waste_gm / 1000.0) / NEW.allocated_input_weight_kg * 100, 2);
            IF waste_pct > 0.5 THEN
                NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                    jsonb_build_array(jsonb_build_object(
                        'code', 'HIGH_WASTE_VARIANCE',
                        'message', format('Waste variance %s%% exceeds 0.5%% threshold. Material loss detected.', waste_pct),
                        'severity', 'BLOCK'
                    ));
            END IF;
        END;
    END IF;
    
    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'WINDING_REJECT_RE_RUN';
        ELSIF NEW.target_machine_type = '2400_HOOK_JACQUARD' 
              AND NEW.joint_method_used IN ('AUTOMATED_AIR_SPLICED_JOIN', 'MICRO_MECHANICAL_KNOT')
              AND NEW.winding_speed_mpm <= 200
              AND NEW.splice_count_per_bobbin <= 1 THEN
            NEW.auto_assigned_routing := 'BOBBIN_CLEARED_FOR_WARPING';
        ELSIF NEW.target_machine_type = '1536_HOOK_JACQUARD'
              AND NEW.waste_variance_percent <= 0.5 THEN
            NEW.auto_assigned_routing := 'BOBBIN_CLEARED_FOR_WARPING';
        ELSIF NEW.waste_variance_percent <= 0.5 THEN
            NEW.auto_assigned_routing := 'BOBBIN_CLEARED_FOR_PIRN_WEFT';
        ELSE
            NEW.auto_assigned_routing := 'WINDING_REJECT_RE_RUN';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_winding_job_card ON winding_job_cards;

CREATE TRIGGER trigger_validate_winding_job_card
    BEFORE INSERT OR UPDATE ON winding_job_cards
    FOR EACH ROW EXECUTE FUNCTION validate_winding_job_card();

-- ------------------------------------------------------------
-- 4. UPDATE ROLES: Bobbin Winder permissions (enhanced)
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari","winding:log:write","winding:read","winding:job:create","winding:job:read","winding:bobbin:write","winding:certificate:read","sales:forecast:read"]',
    description = 'Bobbin Winder: skein opening, tension control, piecing/knot management, bobbin build profile, color/lot segregation, quality audit, certificate issuance; receives pre-process material from Skein Dye Master'
WHERE role_id = 'ROLE-BOBBIN-WINDER';

-- ------------------------------------------------------------
-- 5. UPDATE SKEIN-DYE-MASTER ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:dye","skein:job:create","skein:job:read","skein:job:write","skein:certificate:read","winding:job:read","sales:forecast:read"]',
    description = 'Skein Dye Master: hank loading, temperature profiling, chemical additive management, post-dye washing, drying, floor material accounting, skein certification; provides pre-process material to Bobbin Winder'
WHERE role_id = 'ROLE-SKEIN-DYE-MASTER';
