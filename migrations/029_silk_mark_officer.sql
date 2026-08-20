-- ============================================================
-- Migration: 029_silk_mark_officer.sql
-- Silk Mark Officer module: government compliance audits,
-- silk purity certification, Zari metallurgical verification,
-- hologram tag serialization inventory, and Silk Mark
-- certification issuance.
-- ============================================================

-- ------------------------------------------------------------
-- 1. UPDATE ROLES: Expand permissions
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","quality:read","quality:certify","compliance:write","compliance:certify","silk-mark:read","silk-mark:write","sales:forecast:read","b2b:order:read"]',
    description = 'Silk Mark Officer (Material Authenticity, Silk Mark Certification & Traceability Specialist): government compliance audits, silk purity certification, Zari metallurgical verification, hologram tag serialization inventory, Silk Mark tag issuance, and anti-adulteration legal lock'
WHERE role_id = 'ROLE-SILK-MARK-OFFICER';

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","weaving:read","weaving:supervise","weaving:inspect","assistant:read","master-weaver:read","petni-master:read","warp-joiner:read","harness-setter:read","sales:forecast:read","environment:read","environment:write","oee:read","oee:write","quality:read","quality:write","dyeing:read","dyeing:write","compliance:read","compliance:write","silk-mark:read","silk-mark:write"]',
    description = 'SUP Loom Floor Supervisor (Weaving Shed Supervisor / Jacquard Floor Lead): oversees production, mechanical efficiency, quality compliance, and shift coordination across the weaving floor; monitors OEE, environmental controls, workflow handovers, first saree approval, root cause audits, batch release authorization, quality inspection clearance, dyeing quality verification, and Silk Mark compliance'
WHERE role_id = 'ROLE-SUP-LOOM-FLOOR-SUPERVISOR';

-- ------------------------------------------------------------
-- 2. SILK MARK OFFICER LOGS
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silk_mark_officer_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    audit_visit_id VARCHAR(100) UNIQUE NOT NULL,
    smoii_authorised_user_id VARCHAR(100),
    license_validity_start_date DATE,
    license_expiry_date DATE,
    assigned_silk_mark_officer_id VARCHAR(100),
    factory_node_id VARCHAR(50) NOT NULL,
    silk_mark_officer_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    quality_inspector_log_id UUID REFERENCES quality_inspector_logs(id),
    qa_dyeing_inspector_log_id UUID REFERENCES qa_dyeing_inspector_logs(id),
    sup_supervisor_log_id UUID REFERENCES sup_loom_floor_supervisor_logs(id),
    assistant_weaver_job_log_id UUID REFERENCES assistant_weaver_job_logs(id),
    master_weaver_job_id UUID REFERENCES master_weaver_jobs(id),
    sampled_production_run_ref VARCHAR(100),
    sample_extraction_weight_gm DECIMAL(8,2),
    lab_report_status VARCHAR(50) DEFAULT 'PENDING_LAB_ANALYSIS' CHECK (lab_report_status IN (
        'PENDING_LAB_ANALYSIS',
        'PASSED_100_PURE_SILK',
        'FAILED_ADULTERATION_ALERT'
    )),
    audit_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Category A: Material Origin & Sampling Setup
    saree_piece_serial_id VARCHAR(100),
    silk_yarn_denier_testing_report VARCHAR(50) DEFAULT 'PASSED_16_18D_MULBERRY_SILK' CHECK (silk_yarn_denier_testing_report IN (
        'PASSED_16_18D_MULBERRY_SILK',
        'PASSED_20_22D_RAW_SILK',
        'FAILED_DENIER_VARIANCE_OUT_OF_SPEC'
    )),
    burn_test_result_warp_weft VARCHAR(50) DEFAULT 'PASSED_CHAR_ASH_BURNT_HAIR_ODOR' CHECK (burn_test_result_warp_weft IN (
        'PASSED_CHAR_ASH_BURNT_HAIR_ODOR',
        'FAILED_MELTED_BEAD_SYNTHETIC_DETECTED',
        'INCONCLUSIVE_RETEST_REQUIRED'
    )),
    chemical_solubility_test_status VARCHAR(50) DEFAULT '100%_DISSOLVED_PURE_PROTEIN' CHECK (chemical_solubility_test_status IN (
        '100%_DISSOLVED_PURE_PROTEIN',
        'PARTIAL_RESIDUE_POLYESTER_BLEND',
        'FAILED_CELLULOSE_DETECTED'
    )),

    -- Category B: Zari Metallurgy & Purity Verification
    zari_purity_classification VARCHAR(50) DEFAULT 'PURE_GOLD_SILVER_TESTED_ZARI' CHECK (zari_purity_classification IN (
        'PURE_GOLD_SILVER_TESTED_ZARI',
        'HALF_FINE_ZARI',
        'METALLIC_PLASTIC_IMITATION_ZARI'
    )),
    xrf_silver_content_pct DECIMAL(5,2),
    xrf_gold_content_grams_per_kg DECIMAL(6,2),
    silk_core_zari_wrap_status VARCHAR(50) DEFAULT 'PASSED_PURE_SILK_CORE' CHECK (silk_core_zari_wrap_status IN (
        'PASSED_PURE_SILK_CORE',
        'FAILED_COTTON_CORE_DETECTED',
        'FAILED_VISCOSE_CORE_DETECTED'
    )),

    -- Category C: Certification Release & Serialization
    silk_mark_tag_serial_number VARCHAR(100),
    tag_application_status VARCHAR(50) DEFAULT 'HOLOGRAPHIC_TAG_AFFIXED_AND_SCANNED' CHECK (tag_application_status IN (
        'HOLOGRAPHIC_TAG_AFFIXED_AND_SCANNED',
        'TAG_PENDING_APPLICATON',
        'DAMAGED_TAG_VOIDED'
    )),
    silk_mark_officer_approval_state VARCHAR(50) DEFAULT 'CERTIFIED_GENUINE_SILK_MARK_RELEASED' CHECK (silk_mark_officer_approval_state IN (
        'CERTIFIED_GENUINE_SILK_MARK_RELEASED',
        'HOLD_PURITY_RETEST_REQUIRED',
        'REJECTED_COUNTERFEIT_OR_BLEND'
    )),

    -- High-Security Hologram Inventory Ledger
    hologram_consignment_invoice_no VARCHAR(100),
    hologram_serial_range_start INTEGER,
    hologram_serial_range_end INTEGER,
    total_tags_received_qty INTEGER,

    -- Validation metadata
    validation_errors JSONB DEFAULT '[]'::jsonb,
    validation_warnings JSONB DEFAULT '[]'::jsonb,
    auto_assigned_routing VARCHAR(50),
    certificate_hash VARCHAR(255) UNIQUE,
    qr_tag_id VARCHAR(100) UNIQUE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silk_mark_officer_logs_audit ON silk_mark_officer_logs(audit_visit_id);
CREATE INDEX IF NOT EXISTS idx_silk_mark_officer_logs_officer ON silk_mark_officer_logs(silk_mark_officer_id);
CREATE INDEX IF NOT EXISTS idx_silk_mark_officer_logs_factory ON silk_mark_officer_logs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_silk_mark_officer_logs_quality ON silk_mark_officer_logs(quality_inspector_log_id);
CREATE INDEX IF NOT EXISTS idx_silk_mark_officer_logs_approval ON silk_mark_officer_logs(silk_mark_officer_approval_state);
CREATE INDEX IF NOT EXISTS idx_silk_mark_officer_logs_serial ON silk_mark_officer_logs(silk_mark_tag_serial_number);

-- ------------------------------------------------------------
-- 3. SILK MARK OFFICER CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silk_mark_officer_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    silk_mark_officer_log_id UUID NOT NULL REFERENCES silk_mark_officer_logs(id) ON DELETE CASCADE,
    certificate_hash VARCHAR(255) UNIQUE NOT NULL,
    qr_tag_id VARCHAR(100) UNIQUE,
    audit_visit_id VARCHAR(100) NOT NULL,
    saree_piece_serial_id VARCHAR(100),
    silk_mark_tag_serial_number VARCHAR(100),
    smoii_authorised_user_id VARCHAR(100),
    assigned_silk_mark_officer_id VARCHAR(100),
    lab_report_status VARCHAR(50),
    burn_test_result_warp_weft VARCHAR(50),
    chemical_solubility_test_status VARCHAR(50),
    zari_purity_classification VARCHAR(50),
    xrf_silver_content_pct DECIMAL(5,2),
    xrf_gold_content_grams_per_kg DECIMAL(6,2),
    silk_core_zari_wrap_status VARCHAR(50),
    tag_application_status VARCHAR(50),
    silk_mark_officer_approval_state VARCHAR(50),
    hologram_serial_range_start INTEGER,
    hologram_serial_range_end INTEGER,
    total_tags_received_qty INTEGER,
    officer_id UUID REFERENCES users(id),
    approver_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50) NOT NULL,
    certification_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'REVOKED', 'EXPIRED', 'PENDING'
    )),
    certified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silk_mark_officer_certificates_log ON silk_mark_officer_certificates(silk_mark_officer_log_id);
CREATE INDEX IF NOT EXISTS idx_silk_mark_officer_certificates_hash ON silk_mark_officer_certificates(certificate_hash);
CREATE INDEX IF NOT EXISTS idx_silk_mark_officer_certificates_qr ON silk_mark_officer_certificates(qr_tag_id);
CREATE INDEX IF NOT EXISTS idx_silk_mark_officer_certificates_serial ON silk_mark_officer_certificates(silk_mark_tag_serial_number);

-- ------------------------------------------------------------
-- 4. SALES FORECAST MATERIAL PLAN (API Plugin)
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_forecast_silk_mark_officer_plan (
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

CREATE INDEX IF NOT EXISTS idx_sales_forecast_silk_mark_officer_plan_factory ON sales_forecast_silk_mark_officer_plan(factory_node_id);

-- ------------------------------------------------------------
-- 5. TRIGGERS: Validation and automation
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION validate_silk_mark_officer_log()
RETURNS TRIGGER AS $$
BEGIN
    -- Rule 1 (Synthetic Blend Rejection Interlock Guardrail)
    -- IF burn_test_result_warp_weft = FAILED_MELTED_BEAD_SYNTHETIC_DETECTED
    --    OR chemical_solubility_test_status = PARTIAL_RESIDUE_POLYESTER_BLEND
    -- → BLOCK: IMMEDIATE_CERTIFICATION_HALT
    -- → SET silk_mark_officer_approval_state = REJECTED_COUNTERFEIT_OR_BLEND
    IF NEW.burn_test_result_warp_weft = 'FAILED_MELTED_BEAD_SYNTHETIC_DETECTED'
       OR NEW.chemical_solubility_test_status = 'PARTIAL_RESIDUE_POLYESTER_BLEND' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'IMMEDIATE_CERTIFICATION_HALT',
                'message', 'SYNTHETIC_CONTAMINATION_DETECTED',
                'severity', 'BLOCK'
            ));
        NEW.silk_mark_officer_approval_state := 'REJECTED_COUNTERFEIT_OR_BLEND';
    END IF;

    -- Rule 2 (Pure Zari Metallurgical Gate)
    -- IF zari_purity_classification = PURE_GOLD_SILVER_TESTED_ZARI
    --    AND (xrf_silver_content_pct < 45.0
    --         OR silk_core_zari_wrap_status != PASSED_PURE_SILK_CORE)
    -- → BLOCK: REJECT_ZARI_PURITY_CLAIM
    -- → SET silk_mark_officer_approval_state = HOLD_PURITY_RETEST_REQUIRED
    IF NEW.zari_purity_classification = 'PURE_GOLD_SILVER_TESTED_ZARI' THEN
        IF NEW.xrf_silver_content_pct IS NOT NULL AND NEW.xrf_silver_content_pct < 45.0 THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'REJECT_ZARI_PURITY_CLAIM',
                    'message', 'ZARI_DOES_NOT_MEET_PURE_GOLD_SILVER_STANDARDS',
                    'severity', 'BLOCK'
                ));
            NEW.silk_mark_officer_approval_state := 'HOLD_PURITY_RETEST_REQUIRED';
        END IF;
        IF NEW.silk_core_zari_wrap_status IS NOT NULL AND NEW.silk_core_zari_wrap_status != 'PASSED_PURE_SILK_CORE' THEN
            NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
                jsonb_build_array(jsonb_build_object(
                    'code', 'REJECT_ZARI_PURITY_CLAIM',
                    'message', 'ZARI_DOES_NOT_MEET_PURE_GOLD_SILVER_STANDARDS',
                    'severity', 'BLOCK'
                ));
            NEW.silk_mark_officer_approval_state := 'HOLD_PURITY_RETEST_REQUIRED';
        END IF;
    END IF;

    -- Rule 3 (QA Grade Synchronization Requirement)
    -- IF qa_inspector_approval_state IN (REJECTED_RETURN_TO_SUPERVISOR, GRADE_C_RESERVE_DISCOUNT)
    -- → BLOCK: DENY_SILK_MARK_TAG_ISSUANCE
    IF EXISTS (
        SELECT 1 FROM quality_inspector_logs qil
        WHERE qil.id = NEW.quality_inspector_log_id
          AND qil.final_fabric_quality_grade IN ('GRADE_C_RESERVE_DISCOUNT', 'REJECTED_SCRAP')
    ) THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_SILK_MARK_TAG_ISSUANCE',
                'message', 'REJECTED_OR_GRADE_C_PIECES_CANNOT_BE_ISSUED_PREMIUM_SILK_MARK_TAGS',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Rule 4 (Warehouse Final Transfer Gate)
    -- IF silk_mark_officer_approval_state != CERTIFIED_GENUINE_SILK_MARK_RELEASED
    -- → BLOCK: DENY_FINISHED_GOODS_SALES_DISPATCH
    IF NEW.silk_mark_officer_approval_state IS NOT NULL
       AND NEW.silk_mark_officer_approval_state != 'CERTIFIED_GENUINE_SILK_MARK_RELEASED' THEN
        NEW.validation_errors := COALESCE(NEW.validation_errors, '[]'::jsonb) ||
            jsonb_build_array(jsonb_build_object(
                'code', 'DENY_FINISHED_GOODS_SALES_DISPATCH',
                'message', 'SILK_MARK_CERTIFICATION_NOT_RELEASED',
                'severity', 'BLOCK'
            ));
    END IF;

    -- Auto-assign routing
    IF NEW.auto_assigned_routing IS NULL THEN
        IF NEW.validation_errors IS NOT NULL AND jsonb_array_length(NEW.validation_errors) > 0 THEN
            NEW.auto_assigned_routing := 'SILK_MARK_QC_HOLD';
        ELSIF NEW.silk_mark_officer_approval_state = 'CERTIFIED_GENUINE_SILK_MARK_RELEASED' THEN
            NEW.auto_assigned_routing := 'READY_FOR_SALES_DISPATCH';
        ELSE
            NEW.auto_assigned_routing := 'CERTIFICATION_IN_PROGRESS';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_validate_silk_mark_officer_log ON silk_mark_officer_logs;

CREATE TRIGGER trigger_validate_silk_mark_officer_log
    BEFORE INSERT OR UPDATE ON silk_mark_officer_logs
    FOR EACH ROW EXECUTE FUNCTION validate_silk_mark_officer_log();

-- ------------------------------------------------------------
-- 6. TRIGGER: Auto-generate certificate on approval
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION generate_silk_mark_officer_certificate()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.silk_mark_officer_approval_state = 'CERTIFIED_GENUINE_SILK_MARK_RELEASED'
       AND OLD.silk_mark_officer_approval_state IS DISTINCT FROM NEW.silk_mark_officer_approval_state THEN
        NEW.certificate_hash := generate_certificate_hash(NEW.id, NEW.audit_visit_id);
        NEW.qr_tag_id := 'SMO-' || NEW.audit_visit_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_generate_silk_mark_officer_certificate ON silk_mark_officer_logs;

CREATE TRIGGER trigger_generate_silk_mark_officer_certificate
    BEFORE UPDATE OF silk_mark_officer_approval_state ON silk_mark_officer_logs
    FOR EACH ROW EXECUTE FUNCTION generate_silk_mark_officer_certificate();

-- ------------------------------------------------------------
-- 7. TRIGGER: Update timestamps
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_silk_mark_officer_logs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_silk_mark_officer_logs_updated_at ON silk_mark_officer_logs;

CREATE TRIGGER trigger_update_silk_mark_officer_logs_updated_at
    BEFORE UPDATE ON silk_mark_officer_logs
    FOR EACH ROW EXECUTE FUNCTION update_silk_mark_officer_logs_updated_at();
