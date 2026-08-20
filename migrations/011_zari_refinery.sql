-- ============================================================
-- Migration: 011_zari_refinery.sql
-- Zari Refinery Inward & Quality Screen module
-- Captures metallurgical, physical, and commercial Zari data
-- with post-process certificate issuance and sales forecast API.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE EXISTING ZARI-INSPECTOR ROLE
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari","zari:lot:create","zari:lot:read","zari:assay:write","zari:quality:write","zari:certificate:read","sales:forecast:read"]',
    description = 'Zari Refinery Specialist: inward batch intake, metallurgical assay, quality gate, certificate issuance'
WHERE role_id = 'ROLE-ZARI-INSPECTOR';

-- ------------------------------------------------------------
-- 2. ZARI LOT BATCHES (Category 1 - Batch & Traceability)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zari_lot_batches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    zari_lot_batch_no VARCHAR(100) UNIQUE NOT NULL,
    zari_type VARCHAR(50) NOT NULL CHECK (zari_type IN (
        'PURE_REAL_ZARI_GOLD_SILVER',
        'TESTED_HALF_FINE_ZARI_COPPER_CORE',
        'IMITATION_METALLIC_ZARI'
    )),
    zari_origin_cluster VARCHAR(100) NOT NULL CHECK (zari_origin_cluster IN (
        'SURAT',
        'KANCHIPURAM',
        'DHARMAVARAM',
        'BANARAS',
        'MYSORE',
        'COCHIN',
        'KOLKATA',
        'AHMEDABAD'
    )),
    saree_bundle_size INTEGER DEFAULT 80,
    factory_node_id VARCHAR(50) NOT NULL,
    recorded_by UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'OPEN' CHECK (status IN (
        'OPEN', 'ASSAY_IN_PROGRESS', 'QUALITY_GATE', 'CERTIFIED', 'REJECTED', 'QC_HOLD'
    )),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_zari_lot_batches_no ON zari_lot_batches(zari_lot_batch_no);
CREATE INDEX IF NOT EXISTS idx_zari_lot_batches_factory ON zari_lot_batches(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_zari_lot_batches_status ON zari_lot_batches(status);
CREATE INDEX IF NOT EXISTS idx_zari_lot_batches_origin ON zari_lot_batches(zari_origin_cluster);

-- ------------------------------------------------------------
-- 3. ZARI ASSAY / METALLURGICAL INTAKE (Categories 2, 3, 4)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zari_assay_records (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    zari_lot_batch_id UUID NOT NULL REFERENCES zari_lot_batches(id) ON DELETE CASCADE,
    assay_certificate_no VARCHAR(100) UNIQUE NOT NULL,
    -- Category 2: Metallurgical & Assay
    silver_purity_pct DECIMAL(5,2) CHECK (silver_purity_pct BETWEEN 0 AND 100),
    gold_plating_pct DECIMAL(5,2) CHECK (gold_plating_pct BETWEEN 0 AND 100),
    copper_base_pct DECIMAL(5,2) CHECK (copper_base_pct BETWEEN 0 AND 100),
    core_yarn_material VARCHAR(50) NOT NULL CHECK (core_yarn_material IN (
        'PURE_SILK_THREAD_RED_DYED',
        'PURE_SILK_THREAD_YELLOW_DYED',
        'PURE_COTTON_THREAD',
        'POLYESTER_FILAMENT',
        'NYLON_FILAMENT'
    )),
    -- Category 3: Physical & Textile Geometrics
    zari_count_denier VARCHAR(50) CHECK (zari_count_denier IN (
        '1200_YARDS_PER_OUNCE',
        '1300_YARDS_PER_OUNCE',
        '1400_YARDS_PER_OUNCE',
        '1500_YARDS_PER_OUNCE'
    )),
    zari_wire_diameter_microns DECIMAL(5,2) CHECK (zari_wire_diameter_microns BETWEEN 0 AND 100),
    winding_bobbin_type VARCHAR(50) NOT NULL CHECK (winding_bobbin_type IN (
        'FLANGED_BOBBIN',
        'PAPER_CONE',
        'PLASTIC_SPOOL'
    )),
    -- Category 4: Commercial, Weight & Precious Metal Accounting
    invoice_declared_weight_gm DECIMAL(10,3),
    gross_scale_weight_gm DECIMAL(10,3),
    bobbin_tare_weight_gm DECIMAL(10,3),
    net_zari_weight_gm DECIMAL(10,3),
    precious_metal_market_rate_per_gm DECIMAL(10,4),
    -- Category 5: Physical Gate-Keeper Quality Toggles
    is_free_from_tarnishing BOOLEAN DEFAULT FALSE,
    is_free_from_wire_cuts BOOLEAN DEFAULT FALSE,
    luster_sheen_match BOOLEAN DEFAULT FALSE,
    -- Guardrails & Validation
    validation_errors JSONB DEFAULT '[]'::jsonb,
    validation_warnings JSONB DEFAULT '[]'::jsonb,
    status VARCHAR(50) DEFAULT 'DRAFT' CHECK (status IN (
        'DRAFT', 'SUBMITTED', 'CERTIFIED', 'REJECTED', 'QC_HOLD'
    )),
    certificate_hash VARCHAR(255),
    qr_tag_id VARCHAR(100) UNIQUE,
    factory_node_id VARCHAR(50) NOT NULL,
    operator_id UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_zari_assay_batch ON zari_assay_records(zari_lot_batch_id);
CREATE INDEX IF NOT EXISTS idx_zari_assay_certificate ON zari_assay_records(assay_certificate_no);
CREATE INDEX IF NOT EXISTS idx_zari_assay_factory ON zari_assay_records(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_zari_assay_status ON zari_assay_records(status);

-- ------------------------------------------------------------
-- 4. ZARI CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS zari_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    zari_lot_batch_id UUID NOT NULL REFERENCES zari_lot_batches(id) ON DELETE CASCADE,
    zari_assay_id UUID NOT NULL REFERENCES zari_assay_records(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    zari_type VARCHAR(50) NOT NULL,
    zari_origin_cluster VARCHAR(100) NOT NULL,
    certified_grade VARCHAR(10),
    auto_assigned_routing VARCHAR(50),
    silver_purity_pct DECIMAL(5,2),
    gold_plating_pct DECIMAL(5,2),
    net_zari_weight_gm DECIMAL(10,3),
    precious_metal_value_estimate DECIMAL(12,4),
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

CREATE INDEX IF NOT EXISTS idx_zari_certificates_batch ON zari_certificates(zari_lot_batch_id);
CREATE INDEX IF NOT EXISTS idx_zari_certificates_hash ON zari_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_zari_certificates_qr ON zari_certificates(qr_tag_id);

-- ------------------------------------------------------------
-- 5. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_zari_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_zari_factory ON sales_forecast_zari_plan(factory_node_id);

-- ------------------------------------------------------------
-- 6. TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION calculate_zari_derived_fields()
RETURNS TRIGGER AS $$
BEGIN
    -- Calculate net zari weight
    IF NEW.gross_scale_weight_gm IS NOT NULL AND NEW.bobbin_tare_weight_gm IS NOT NULL THEN
        NEW.net_zari_weight_gm := ROUND(
            NEW.gross_scale_weight_gm - NEW.bobbin_tare_weight_gm,
            3
        );
    END IF;
    
    -- Validate purity ranges for Pure Real Zari
    IF NEW.silver_purity_pct IS NOT NULL AND (NEW.silver_purity_pct < 55 OR NEW.silver_purity_pct > 57) THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'SILVER_PURITY_OUT_OF_RANGE',
                'message', format('Silver purity %s%% is outside luxury standard 55-57%%', NEW.silver_purity_pct)
            ));
    END IF;
    
    IF NEW.gold_plating_pct IS NOT NULL AND (NEW.gold_plating_pct < 0.5 OR NEW.gold_plating_pct > 1.0) THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'GOLD_PLATING_OUT_OF_RANGE',
                'message', format('Gold plating %s%% is outside luxury standard 0.5-1.0%%', NEW.gold_plating_pct)
            ));
    END IF;
    
    -- Warn if purity is out of range for Tested Zari
    IF NEW.copper_base_pct IS NOT NULL AND NEW.copper_base_pct > 0 THEN
        NEW.validation_warnings := COALESCE(NEW.validation_warnings, '[]'::jsonb) || 
            jsonb_build_array(jsonb_build_object(
                'code', 'COPPER_BASE_DETECTED',
                'message', 'Copper base detected - verify Tested Zari classification'
            ));
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_calculate_zari_derived_fields ON zari_assay_records;

CREATE TRIGGER trigger_calculate_zari_derived_fields
    BEFORE INSERT OR UPDATE ON zari_assay_records
    FOR EACH ROW EXECUTE FUNCTION calculate_zari_derived_fields();

-- ------------------------------------------------------------
-- 7. TIMESTAMP UPDATE TRIGGERS
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_zari_lot_batches_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_zari_lot_batches_updated_at ON zari_lot_batches;

CREATE TRIGGER trigger_update_zari_lot_batches_updated_at
    BEFORE UPDATE ON zari_lot_batches
    FOR EACH ROW EXECUTE FUNCTION update_zari_lot_batches_updated_at();

CREATE OR REPLACE FUNCTION update_zari_assay_records_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_zari_assay_records_updated_at ON zari_assay_records;

CREATE TRIGGER trigger_update_zari_assay_records_updated_at
    BEFORE UPDATE ON zari_assay_records
    FOR EACH ROW EXECUTE FUNCTION update_zari_assay_records_updated_at();

-- ------------------------------------------------------------
-- 8. UPDATE FILATURE SUPPLIER ROLE PERMISSIONS
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari","inward:gate:create","inward:gate:read","inward:quality:read","inward:lab:read","zari:lot:read"]',
    description = 'Source raw silk, manage supplier relationships, inward quality gate, provide pre-process material to Zari Refinery'
WHERE role_id = 'ROLE-FILATURE-SUPPLIER';
