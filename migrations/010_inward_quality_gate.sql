-- ============================================================
-- Migration: 010_inward_quality_gate.sql
-- Inward Quality Gate framework for Filature Supplier QA:
-- Gate Clerk, QC Inspector, Quality Manager roles,
-- inward gate entry, quality intake, lab certificate,
-- approval workflow, and auto-routing logic.
-- ============================================================

-- ------------------------------------------------------------
-- 1. QA ROLES
-- ------------------------------------------------------------

INSERT INTO roles (role_id, name, permitted_operations, description)
VALUES
  ('ROLE-GATE-CLERK', 'Gate Clerk (Logistics Entry)',
   '["inward:gate:create","inward:gate:read"]',
   'Registers vehicle arrival, package count, gross shipment weight')
ON CONFLICT (role_id) DO NOTHING;

INSERT INTO roles (role_id, name, permitted_operations, description)
VALUES
  ('ROLE-QC-INSPECTOR', 'QC Inspector (Technical Tester)',
   '["inward:quality:write","inward:lab:write","inward:quality:read"]',
   'Performs moisture probe tests, inspects hanks, enters lab metrics')
ON CONFLICT (role_id) DO NOTHING;

INSERT INTO roles (role_id, name, permitted_operations, description)
VALUES
  ('ROLE-QUALITY-MANAGER', 'Quality Manager (Approver & Gatekeeper)',
   '["inward:quality:approve","inward:quality:override","inward:quality:read"]',
   'Audits moisture/size deviations, clears or rejects batches')
ON CONFLICT (role_id) DO NOTHING;

-- ------------------------------------------------------------
-- 2. INWARD GATE ENTRIES (Category A - Gate Clerk)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS inward_gate_entries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    inward_gate_entry_no VARCHAR(100) UNIQUE NOT NULL,
    supplier_id UUID REFERENCES users(id),
    invoice_number VARCHAR(100) NOT NULL,
    invoice_gross_weight_kg DECIMAL(10,3) NOT NULL,
    actual_scale_weight_kg DECIMAL(10,3) NOT NULL,
    filature_lot_number VARCHAR(100) NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    recorded_by UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'QC_HOLD' CHECK (status IN (
        'QC_HOLD', 'INSPECTION_IN_PROGRESS', 'WARP_PREMIUM', 'WEFT_ONLY', 'QC_REJECT_HOLD', 'REJECTED_VENDOR_RETURN'
    )),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_inward_gate_entries_supplier ON inward_gate_entries(supplier_id);
CREATE INDEX IF NOT EXISTS idx_inward_gate_entries_factory ON inward_gate_entries(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_inward_gate_entries_lot ON inward_gate_entries(filature_lot_number);
CREATE INDEX IF NOT EXISTS idx_inward_gate_entries_status ON inward_gate_entries(status);

-- ------------------------------------------------------------
-- 3. QUALITY INTAKE RECORDS (Categories B, C, D - QC Inspector)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS quality_intake_records (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    quality_intake_no VARCHAR(100) UNIQUE NOT NULL,
    inward_gate_entry_id UUID NOT NULL REFERENCES inward_gate_entries(id) ON DELETE CASCADE,
    inspector_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    -- Category B
    silk_type VARCHAR(50) NOT NULL CHECK (silk_type IN (
        'BIVOLTINE_WHITE_SILK',
        'MULTIVOLTINE_YELLOW_SILK',
        'TUSSAR_WILD_SILK',
        'MUGA_GOLDEN_SILK',
        'ERI_SPUN_SILK'
    )),
    machinery_source VARCHAR(50) NOT NULL CHECK (machinery_source IN (
        'ARM_AUTOMATIC_REELING',
        'MRM_MULTI_END_REELING'
    )),
    silk_mark_tag_id VARCHAR(100),
    -- Category C
    lab_report_number VARCHAR(100),
    certified_grade VARCHAR(10) NOT NULL CHECK (certified_grade IN (
        '6A', '5A', '4A', '3A', '2A', 'A', 'B', 'C', 'D', 'Reject'
    )),
    size_deviation_pct DECIMAL(5,2),
    evenness_pct DECIMAL(5,2),
    cleanness_pct DECIMAL(5,2),
    neatness_pct DECIMAL(5,2),
    tenacity_gd DECIMAL(5,2),
    cohesion_strokes INTEGER,
    -- Category D
    live_moisture_reading_pct DECIMAL(5,2),
    has_machine_oil_stains BOOLEAN DEFAULT FALSE,
    has_mixed_dye_lots BOOLEAN DEFAULT FALSE,
    sample_hank_weight_g INTEGER,
    -- Category E (system-calculated)
    calculated_conditioned_weight_kg DECIMAL(10,3),
    billing_weight_discrepancy_kg DECIMAL(10,3),
    auto_assigned_routing VARCHAR(50),
    -- Validation flags
    validation_errors JSONB DEFAULT '[]'::jsonb,
    status VARCHAR(50) DEFAULT 'DRAFT' CHECK (status IN (
        'DRAFT', 'SUBMITTED', 'APPROVED', 'REJECTED', 'QC_HOLD'
    )),
    certificate_hash VARCHAR(255),
    qr_tag_id VARCHAR(100) UNIQUE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quality_intake_gate ON quality_intake_records(inward_gate_entry_id);
CREATE INDEX IF NOT EXISTS idx_quality_intake_inspector ON quality_intake_records(inspector_id);
CREATE INDEX IF NOT EXISTS idx_quality_intake_factory ON quality_intake_records(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_quality_intake_grade ON quality_intake_records(certified_grade);
CREATE INDEX IF NOT EXISTS idx_quality_intake_routing ON quality_intake_records(auto_assigned_routing);

-- ------------------------------------------------------------
-- 4. QUALITY APPROVAL WORKFLOW (Quality Manager)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS quality_approval_workflow (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    quality_intake_id UUID NOT NULL REFERENCES quality_intake_records(id) ON DELETE CASCADE,
    approver_id UUID REFERENCES users(id),
    action VARCHAR(50) NOT NULL CHECK (action IN (
        'APPROVED', 'REJECTED', 'HOLD', 'ESCALATED', 'OVERRIDDEN'
    )),
    from_status VARCHAR(50),
    to_status VARCHAR(50) NOT NULL,
    notes TEXT,
    override_reason TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quality_approval_intake ON quality_approval_workflow(quality_intake_id);
CREATE INDEX IF NOT EXISTS idx_quality_approval_approver ON quality_approval_workflow(approver_id);

-- ------------------------------------------------------------
-- 5. QUALITY CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS quality_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    quality_intake_id UUID NOT NULL REFERENCES quality_intake_records(id) ON DELETE CASCADE,
    inward_gate_entry_id UUID NOT NULL REFERENCES inward_gate_entries(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    certified_grade VARCHAR(10) NOT NULL,
    auto_assigned_routing VARCHAR(50) NOT NULL,
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

CREATE INDEX IF NOT EXISTS idx_quality_certificates_intake ON quality_certificates(quality_intake_id);
CREATE INDEX IF NOT EXISTS idx_quality_certificates_gate ON quality_certificates(inward_gate_entry_id);
CREATE INDEX IF NOT EXISTS idx_quality_certificates_hash ON quality_certificates(certificate_hash);

-- ------------------------------------------------------------
-- 6. TRIGGERS: Auto-calculate fields and routing
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION calculate_quality_intake_derived_fields()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.actual_scale_weight_kg IS NOT NULL AND NEW.live_moisture_reading_pct IS NOT NULL THEN
        NEW.calculated_conditioned_weight_kg := ROUND(
            NEW.actual_scale_weight_kg * (1 - (NEW.live_moisture_reading_pct / 100.0)) * 1.11,
            3
        );
        
        IF NEW.invoice_gross_weight_kg IS NOT NULL THEN
            NEW.billing_weight_discrepancy_kg := ROUND(
                NEW.invoice_gross_weight_kg - NEW.calculated_conditioned_weight_kg,
                3
            );
        END IF;
    END IF;
    
    -- Auto-assign routing based on grade and quality metrics
    IF NEW.certified_grade IN ('6A', '5A', '4A') 
       AND NEW.evenness_pct >= 95 
       AND NEW.cleanness_pct >= 95 
       AND NEW.neatness_pct >= 93 
       AND NEW.tenacity_gd >= 3.8 
       AND NEW.cohesion_strokes >= 60 
       AND NEW.live_moisture_reading_pct <= 11.0 
       AND NEW.size_deviation_pct < 4.0 THEN
        NEW.auto_assigned_routing := 'WARP_PREMIUM';
    ELSIF NEW.certified_grade IN ('3A', '2A', 'A') 
          AND NEW.live_moisture_reading_pct <= 11.0 
          AND NOT NEW.has_machine_oil_stains 
          AND NOT NEW.has_mixed_dye_lots THEN
        NEW.auto_assigned_routing := 'WEFT_ONLY';
    ELSE
        NEW.auto_assigned_routing := 'QC_REJECT_HOLD';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_calculate_quality_intake_derived_fields ON quality_intake_records;

CREATE TRIGGER trigger_calculate_quality_intake_derived_fields
    BEFORE INSERT OR UPDATE ON quality_intake_records
    FOR EACH ROW EXECUTE FUNCTION calculate_quality_intake_derived_fields();

-- ------------------------------------------------------------
-- 7. TRIGGER: Update timestamps
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_inward_gate_entries_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_inward_gate_entries_updated_at ON inward_gate_entries;

CREATE TRIGGER trigger_update_inward_gate_entries_updated_at
    BEFORE UPDATE ON inward_gate_entries
    FOR EACH ROW EXECUTE FUNCTION update_inward_gate_entries_updated_at();

CREATE OR REPLACE FUNCTION update_quality_intake_records_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_quality_intake_records_updated_at ON quality_intake_records;

CREATE TRIGGER trigger_update_quality_intake_records_updated_at
    BEFORE UPDATE ON quality_intake_records
    FOR EACH ROW EXECUTE FUNCTION update_quality_intake_records_updated_at();

-- ------------------------------------------------------------
-- 8. UPDATE ROLES: Expand Filature Supplier permissions
-- ------------------------------------------------------------

UPDATE roles 
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari","inward:gate:create","inward:gate:read","inward:quality:read","inward:lab:read"]',
    description = 'Source raw silk, manage supplier relationships, inward quality gate, lab certificate intake'
WHERE role_id = 'ROLE-FILATURE-SUPPLIER';
