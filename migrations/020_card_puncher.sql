-- ============================================================
-- Migration: 020_card_puncher.sql
-- Card Puncher (Digital/E-Jacquard Programmer) module
-- CAD-to-CAM file compilation, weave structure code injection,
-- card-punch machine operation, loom controller programming,
-- and physical card-lacing & verification.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE CARD-PUNCHER ROLE
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","design:write","design:approve","card:program:create","card:program:read","card:program:write","card:certificate:read","sales:forecast:read"]',
    description = 'Card Puncher (Digital/E-Jacquard Programmer): CAD-to-CAM file compilation, weave structure code injection, card-punch machine operation, loom controller programming, physical card-lacing & verification, checksum security gate'
WHERE role_id = 'ROLE-CARD-PUNCHER';

-- ------------------------------------------------------------
-- 2. PROGRAMMING JOBS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS programming_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    programming_job_id VARCHAR(100) UNIQUE NOT NULL,
    design_master_id UUID REFERENCES design_masters(id),
    design_certificate_id UUID REFERENCES design_certificates(id),
    pirn_winding_job_id UUID REFERENCES pirn_winding_jobs(id),
    bobbin_winder_job_card_id UUID REFERENCES winding_job_cards(id),
    master_colorist_recipe_id UUID REFERENCES master_colorist_recipes(id),
    skein_dye_job_id UUID REFERENCES skein_dye_jobs(id),
    throwster_record_id UUID REFERENCES throwster_production_records(id),
    production_lot_id UUID REFERENCES production_lots(id),
    factory_node_id VARCHAR(50) NOT NULL,
    programmer_employee_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'PENDING_COMPILATION' CHECK (status IN (
        'PENDING_COMPILATION', 'COMPILED', 'SIMULATION_PASSED', 'LOADED_TO_LOOM',
        'REJECTED_MISMATCH', 'CORRUPT_HOLD', 'ARCHIVED'
    )),
    
    -- File Compilation & Identity Tracker
    compiled_cam_file_name VARCHAR(255),
    target_loom_type VARCHAR(50) CHECK (target_loom_type IN (
        'MECHANICAL_JACQUARD_PHYSICAL_CARDS',
        'ELECTRONIC_JACQUARD_DIGITAL_FILE'
    )),
    
    -- Category A: Controller & Machine Hardware Setup
    loom_hardware_id VARCHAR(100),
    controller_brand_type VARCHAR(100) CHECK (controller_brand_type IN (
        'STAUBLI_JC5_JC6',
        'BONAS_EP_SI',
        'EZY_JACQUARD_DLC',
        'GROSSE_EJP',
        'GENERIC_MECHANICAL_PUNCHER'
    )),
    physical_hook_matrix VARCHAR(100) DEFAULT '2400_HOOK_2250_DESIGN_150_BORDER' CHECK (physical_hook_matrix IN (
        '2400_HOOK_2250_DESIGN_150_BORDER',
        '1536_HOOK_1440_DESIGN_96_BORDER',
        '1200_HOOK_1080_DESIGN_120_BORDER',
        '600_HOOK_540_DESIGN_60_BORDER'
    )),
    data_transfer_method VARCHAR(100) DEFAULT 'DIRECT_LOOM_NETWORK_LAN' CHECK (data_transfer_method IN (
        'DIRECT_LOOM_NETWORK_LAN',
        'USB_STORAGE_MEDIA',
        'DIRECT_SERIAL_LINK_RS422',
        'PHYSICAL_CARD_PUNCH_MACHINE'
    )),
    
    -- Category B: Programming Rules & Repeat Logic
    pattern_repeat_mode VARCHAR(100) DEFAULT 'STRAIGHT_REPEAT' CHECK (pattern_repeat_mode IN (
        'STRAIGHT_REPEAT',
        'MIRROR_FLIP_REPEAT',
        'CENTERED_DOUBLE_REPEAT',
        'PANEL_BORDER_SYNC_MODE'
    )),
    solenoid_firing_profile VARCHAR(100) DEFAULT 'HIGH_DENSITY_FAST_PULSE_LE_8MS' CHECK (solenoid_firing_profile IN (
        'HIGH_DENSITY_FAST_PULSE_LE_8MS',
        'STANDARD_PULSE_12_15MS',
        'EXTENDED_HOLD_PULSE'
    )),
    pick_sequence_interlock VARCHAR(100) DEFAULT 'GROUND_1_1_EXTRA_WEFT' CHECK (pick_sequence_interlock IN (
        'GROUND_1_1_EXTRA_WEFT',
        'GROUND_2_1_ZARI_PICK',
        'GROUND_3_1_HEAVY_WEFT',
        'MULTI_SHUTTLE_CONTINUOUS'
    )),
    
    -- Category C: Verification, Output & System Status
    file_integrity_checksum VARCHAR(255),
    dry_run_simulation_status VARCHAR(100) DEFAULT 'PENDING' CHECK (dry_run_simulation_status IN (
        'PASSED_ZERO_ERRORS',
        'WARNING_HIGH_SOLENOID_LOAD',
        'FAILED_UNMAPPED_PINS',
        'FAILED_CHECKSUM_MISMATCH',
        'PENDING'
    )),
    card_program_approval_state VARCHAR(100) DEFAULT 'PENDING_COMPILATION' CHECK (card_program_approval_state IN (
        'PENDING_COMPILATION',
        'SIMULATION_PASSED',
        'LOADED_TO_LOOM',
        'REJECTED_MISMATCH',
        'ARCHIVED'
    )),
    
    -- Material Consumables (for mechanical card punching)
    input_blank_cards_weight_kg DECIMAL(10,3),
    actual_punched_cards_count INTEGER,
    punch_waste_scrap_weight_gm DECIMAL(10,3),
    
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

CREATE INDEX IF NOT EXISTS idx_programming_jobs_code ON programming_jobs(programming_job_id);
CREATE INDEX IF NOT EXISTS idx_programming_jobs_factory ON programming_jobs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_programming_jobs_status ON programming_jobs(status);
CREATE INDEX IF NOT EXISTS idx_programming_jobs_design ON programming_jobs(design_master_id);
CREATE INDEX IF NOT EXISTS idx_programming_jobs_loom ON programming_jobs(loom_hardware_id);

-- ------------------------------------------------------------
-- 3. PROGRAMMING CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS programming_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    programming_job_id UUID NOT NULL REFERENCES programming_jobs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    programming_job_id_ref VARCHAR(100) NOT NULL,
    design_master_id_ref VARCHAR(100),
    compiled_cam_file_name VARCHAR(255),
    target_loom_type VARCHAR(50),
    loom_hardware_id VARCHAR(100),
    controller_brand_type VARCHAR(100),
    physical_hook_matrix VARCHAR(100),
    data_transfer_method VARCHAR(100),
    pattern_repeat_mode VARCHAR(100),
    solenoid_firing_profile VARCHAR(100),
    pick_sequence_interlock VARCHAR(100),
    file_integrity_checksum VARCHAR(255),
    dry_run_simulation_status VARCHAR(100),
    card_program_approval_state VARCHAR(100),
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

CREATE INDEX IF NOT EXISTS idx_programming_certificates_job ON programming_certificates(programming_job_id);
CREATE INDEX IF NOT EXISTS idx_programming_certificates_hash ON programming_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_programming_certificates_qr ON programming_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 4. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_card_puncher_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_card_puncher_factory ON sales_forecast_card_puncher_plan(factory_node_id);

-- ------------------------------------------------------------
-- 5. TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_programming_job()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1: 2400 Hook Matrix Verification Gate
    -- IF physical_hook_matrix = 2400 Hook AND controller_brand_type = Generic Mechanical Puncher
    -- → BLOCK: INVALID_CONTROLLER_SELECTION (ELECTRONIC_CONTROLLER_MANDATORY_FOR_2400)
    IF NEW.physical_hook_matrix = '2400_HOOK_2250_DESIGN_150_BORDER' THEN
        IF NEW.controller_brand_type = 'GENERIC_MECHANICAL_PUNCHER' THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'INVALID_CONTROLLER_SELECTION',
                    'message', '2400 Hook requires electronic controller. Generic Mechanical Puncher is not allowed.',
                    'severity', 'BLOCK'
                ));
        END IF;
        
        -- 2400 Hook specific: require high-density fast pulse
        IF NEW.solenoid_firing_profile != 'HIGH_DENSITY_FAST_PULSE_LE_8MS' THEN
            NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
                jsonb_build_array(jsonb_build_object(
                    'code', 'SUBOPTIMAL_SOLENOID_PROFILE',
                    'message', '2400 Hook requires High-Density Fast Pulse (≤8ms) for optimal performance.',
                    'severity', 'WARNING'
                ));
        END IF;
    END IF;
    
    -- Rule 2: Simulation Clearance Gate
    -- IF dry_run_simulation_status IN (FAILED_UNMAPPED_PINS, FAILED_CHECKSUM_MISMATCH)
    -- → BLOCK: PREVENT_LOOM_TRANSFER
    IF NEW.dry_run_simulation_status IN ('FAILED_UNMAPPED_PINS', 'FAILED_CHECKSUM_MISMATCH') THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'PREVENT_LOOM_TRANSFER',
                'message', format('Dry-run simulation failed: %s. Loom transfer blocked.', NEW.dry_run_simulation_status),
                'severity', 'BLOCK'
            ));
    END IF;
    
    -- Rule 3: Solenoid Overload Warning
    -- IF dry_run_simulation_status = WARNING_HIGH_SOLENOID_LOAD
    -- → TRIGGER WARNING: HIGH_THERMAL_LOAD_REDUCE_LOOM_SPEED_OR_ADJUST_WEAVE
    IF NEW.dry_run_simulation_status = 'WARNING_HIGH_SOLENOID_LOAD' THEN
        NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'HIGH_THERMAL_LOAD_REDUCE_LOOM_SPEED_OR_ADJUST_WEAVE',
                'message', 'High solenoid thermal load detected. Reduce loom speed or adjust weave parameters.',
                'severity', 'WARNING'
            ));
    END IF;
    
    -- Rule 4: Loom Load Authorization
    -- IF card_program_approval_state NOT EQUAL TO SIMULATION_PASSED
    -- → BLOCK: DENY_DIRECT_LOOM_NETWORK_WRITE
    IF NEW.card_program_approval_state != 'SIMULATION_PASSED' 
       AND NEW.data_transfer_method = 'DIRECT_LOOM_NETWORK_LAN' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_DIRECT_LOOM_NETWORK_WRITE',
                'message', 'Direct loom network write denied. Card program approval state must be SIMULATION_PASSED.',
                'severity', 'BLOCK'
            ));
    END IF;
    
    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'CORRUPT_HOLD';
        ELSIF NEW.dry_run_simulation_status = 'PASSED_ZERO_ERRORS'
              AND NEW.card_program_approval_state = 'SIMULATION_PASSED' THEN
            NEW.auto_assigned_routing := 'DESIGN_PRODUCTION_READY';
        ELSIF NEW.dry_run_simulation_status = 'WARNING_HIGH_SOLENOID_LOAD' THEN
            NEW.auto_assigned_routing := 'PENDING_THERMAL_REVIEW';
        ELSE
            NEW.auto_assigned_routing := 'PENDING_COMPILATION';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_programming_job ON programming_jobs;

CREATE TRIGGER trigger_validate_programming_job
    BEFORE INSERT OR UPDATE ON programming_jobs
    FOR EACH ROW EXECUTE FUNCTION validate_programming_job();

-- ------------------------------------------------------------
-- 6. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_programming_jobs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_programming_jobs_updated_at ON programming_jobs;

CREATE TRIGGER trigger_update_programming_jobs_updated_at
    BEFORE UPDATE ON programming_jobs
    FOR EACH ROW EXECUTE FUNCTION update_programming_jobs_updated_at();

-- ------------------------------------------------------------
-- 7. UPDATE GRAPH-DRAFTER ROLE PERMISSIONS (pre-process)
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","design:write","design:approve","design:export","design:certificate:read","card:program:read","sales:forecast:read"]',
    description = 'Graph Drafter (2400 Hook): pixel-by-pixel grid translation, hook allocation, weave structure assignment, color separation/card-punch coding, technical feasibility auditing, CAD export; provides pre-process design to Card Puncher'
WHERE role_id = 'ROLE-GRAPH-DRAFTER';
