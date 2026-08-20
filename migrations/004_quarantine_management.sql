-- ============================================================
-- Migration: 004_quarantine_management.sql
-- Quarantine tables and triggers
-- ============================================================

-- Release lock on failure/quarantine
CREATE OR REPLACE FUNCTION trg_release_lock_on_failure()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status IN ('Failed', 'Quarantined') THEN
        NEW.locked_by = NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER release_lock_on_failure
    BEFORE UPDATE ON production_lots
    FOR EACH ROW
    WHEN (OLD.status != NEW.status AND NEW.status IN ('Failed', 'Quarantined'))
    EXECUTE FUNCTION trg_release_lock_on_failure();

-- Duplicate scan check function
CREATE OR REPLACE FUNCTION fn_check_duplicate_scan(
    p_asset_id VARCHAR(100),
    p_factory_node_id VARCHAR(50)
) RETURNS BOOLEAN AS $$
DECLARE
    v_recent_scan_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_recent_scan_count
    FROM scanner_logs
    WHERE asset_id = p_asset_id
      AND factory_node_id = p_factory_node_id
      AND scan_type = 'input'
      AND scan_timestamp > NOW() - INTERVAL '5 minutes';
    
    RETURN v_recent_scan_count > 0;
END;
$$ LANGUAGE plpgsql;

-- Status transition validation
CREATE OR REPLACE FUNCTION fn_validate_status_transition(
    p_current_status VARCHAR(50),
    p_expected_status VARCHAR(50)
) RETURNS BOOLEAN AS $$
BEGIN
    IF p_current_status = p_expected_status THEN
        RETURN TRUE;
    END IF;
    
    IF p_current_status LIKE 'Certified:%' THEN
        RETURN TRUE;
    END IF;
    
    IF p_current_status = 'Queued' THEN
        RETURN TRUE;
    END IF;
    
    RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

-- Workflow transition validation
CREATE OR REPLACE FUNCTION fn_validate_workflow_transition(
    p_from_role_id UUID,
    p_to_role_id UUID,
    p_certificate_step VARCHAR(100)
) RETURNS BOOLEAN AS $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM workflow_transitions
    WHERE from_role_id = p_from_role_id
      AND to_role_id = p_to_role_id
      AND required_certificate_step = p_certificate_step
      AND allowed_verdicts @> '["PASS"]'::jsonb;
    
    RETURN v_count > 0;
END;
$$ LANGUAGE plpgsql;
