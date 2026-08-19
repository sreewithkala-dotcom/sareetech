-- ============================================================
-- Migration: 003_stored_procedures.sql
-- Core stored procedures for workflow enforcement
-- ============================================================

-- ------------------------------------------------------------
-- 1. Scanner Input Validation
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION sp_validate_scanner_input(
    p_asset_id VARCHAR(100),
    p_operator_id UUID,
    p_factory_node_id VARCHAR(50),
    p_expected_role_id UUID
) RETURNS TABLE (
    lot_id UUID,
    current_status VARCHAR(50),
    validation_passed BOOLEAN,
    error_message TEXT
) AS $$
DECLARE
    v_lot_id UUID;
    v_current_status VARCHAR(50);
    v_locked_by UUID;
BEGIN
    SELECT id, status, locked_by INTO v_lot_id, v_current_status, v_locked_by
    FROM production_lots
    WHERE asset_id = p_asset_id
      AND factory_node_id = p_factory_node_id
    LIMIT 1;
    
    IF v_lot_id IS NULL THEN
        RETURN QUERY SELECT NULL::UUID, NULL::VARCHAR(50), FALSE, 'Asset not found in system';
        RETURN;
    END IF;
    
    IF v_locked_by IS NOT NULL AND v_locked_by != p_operator_id THEN
        RETURN QUERY SELECT v_lot_id, v_current_status, FALSE, 'Asset is locked by another operator';
        RETURN;
    END IF;
    
    IF v_current_status NOT LIKE 'Certified:%' AND v_current_status != 'Queued' THEN
        RETURN QUERY SELECT v_lot_id, v_current_status, FALSE, 
            'PrerequisiteMissing: Expected Certified or Queued status, got ' || v_current_status;
        RETURN;
    END IF;
    
    UPDATE production_lots
    SET status = 'InputScanned',
        locked_by = p_operator_id,
        current_role_id = p_expected_role_id,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = v_lot_id;
    
    INSERT INTO scanner_logs (lot_id, scan_type, asset_id, factory_node_id, operator_id, status)
    VALUES (v_lot_id, 'input', p_asset_id, p_factory_node_id, p_operator_id, 'success');
    
    RETURN QUERY SELECT v_lot_id, v_current_status, TRUE, 'Validation passed';
END;
$$ LANGUAGE plpgsql;

-- ------------------------------------------------------------
-- 2. AI Certification Processing
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION sp_process_ai_certification(
    p_lot_id UUID,
    p_role_id UUID,
    p_step_name VARCHAR(100),
    p_certification_json JSONB,
    p_confidence_score DECIMAL(3,2),
    p_verdict VARCHAR(20),
    p_defect_classes JSONB,
    p_metrics JSONB,
    p_model_version VARCHAR(50),
    p_processing_time_ms INTEGER,
    p_cryptographic_check_token VARCHAR(255)
) RETURNS UUID AS $$
DECLARE
    v_certificate_id UUID;
BEGIN
    INSERT INTO ai_inspection_certificates (
        lot_id, role_id, step_name, certification_json,
        confidence_score, verdict, defect_classes, metrics,
        model_version, processing_time_ms, cryptographic_check_token
    ) VALUES (
        p_lot_id, p_role_id, p_step_name, p_certification_json,
        p_confidence_score, p_verdict, p_defect_classes, p_metrics,
        p_model_version, p_processing_time_ms, p_cryptographic_check_token
    ) RETURNING id INTO v_certificate_id;
    
    IF p_verdict = 'FAIL' THEN
        UPDATE production_lots
        SET status = 'Quarantined',
            locked_by = NULL,
            metadata = jsonb_set(COALESCE(metadata, '{}'), '{quarantine_reason}', 
                    '"AI Defect Failure: ' || p_step_name || ' returned FAIL"'),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = p_lot_id;
        
        INSERT INTO workflow_states (lot_id, role_id, previous_state, new_state, notes)
        VALUES (p_lot_id, p_role_id, 'InProgress', 'Quarantined', 
                'AI verdict: FAIL. Step: ' || p_step_name);
        
        INSERT INTO quarantined_lots (lot_id, reason, ai_certificate_id, quarantined_by)
        VALUES (p_lot_id, 'AI verdict FAIL: ' || p_step_name, v_certificate_id, 
                (SELECT id FROM users WHERE role_id = (SELECT id FROM roles WHERE role_id = 'ROLE-SUP-LOOM-FLOOR-SUPERVISOR') LIMIT 1));
    ELSE
        UPDATE production_lots
        SET status = 'Certified:' || p_step_name,
            certified_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = p_lot_id;
        
        UPDATE workflow_states
        SET exit_timestamp = CURRENT_TIMESTAMP
        WHERE lot_id = p_lot_id AND exit_timestamp IS NULL;
        
        INSERT INTO workflow_states (lot_id, role_id, previous_state, new_state)
        VALUES (p_lot_id, p_role_id, 'InProgress', 'Certified:' || p_step_name);
    END IF;
    
    INSERT INTO audit_logs (action, resource_type, resource_id, new_values)
    VALUES (
        'CERTIFICATION_ISSUED',
        'ai_inspection_certificate',
        v_certificate_id,
        jsonb_build_object(
            'lot_id', p_lot_id,
            'role_id', p_role_id,
            'step_name', p_step_name,
            'verdict', p_verdict,
            'timestamp', CURRENT_TIMESTAMP
        )
    );
    
    RETURN v_certificate_id;
END;
$$ LANGUAGE plpgsql;

-- ------------------------------------------------------------
-- 3. Scanner Output Processing
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION sp_process_scanner_output(
    p_lot_id UUID,
    p_asset_id VARCHAR(100),
    p_operator_id UUID,
    p_factory_node_id VARCHAR(50)
) RETURNS TABLE (
    success BOOLEAN,
    next_role_id UUID,
    error_message TEXT
) AS $$
DECLARE
    v_current_status VARCHAR(50);
    v_current_role_id UUID;
    v_next_role_id UUID;
BEGIN
    SELECT status, current_role_id INTO v_current_status, v_current_role_id
    FROM production_lots
    WHERE id = p_lot_id
    FOR UPDATE;
    
    IF NOT FOUND THEN
        RETURN QUERY SELECT FALSE, NULL::UUID, 'Lot not found';
        RETURN;
    END IF;
    
    IF v_current_status NOT LIKE 'Certified:%' THEN
        RETURN QUERY SELECT FALSE, NULL::UUID, 
            'Invalid status for output scan: ' || v_current_status;
        RETURN;
    END IF;
    
    SELECT wt.to_role_id INTO v_next_role_id
    FROM workflow_transitions wt
    WHERE wt.from_role_id = v_current_role_id
      AND wt.required_certificate_step = REPLACE(v_current_status, 'Certified:', '')
    LIMIT 1;
    
    IF v_next_role_id IS NULL THEN
        UPDATE production_lots
        SET status = 'Completed',
            completed_at = CURRENT_TIMESTAMP,
            locked_by = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = p_lot_id;
        
        INSERT INTO scanner_logs (lot_id, scan_type, asset_id, factory_node_id, operator_id, status)
        VALUES (p_lot_id, 'output', p_asset_id, p_factory_node_id, p_operator_id, 'success');
        
        RETURN QUERY SELECT TRUE, NULL::UUID, 'Lot completed successfully';
        RETURN;
    END IF;
    
    UPDATE production_lots
    SET status = 'Queued:' || v_next_role_id,
        current_role_id = v_next_role_id,
        locked_by = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = p_lot_id;
    
    INSERT INTO scanner_logs (lot_id, scan_type, asset_id, factory_node_id, operator_id, status)
    VALUES (p_lot_id, 'output', p_asset_id, p_factory_node_id, p_operator_id, 'success');
    
    INSERT INTO workflow_states (lot_id, role_id, previous_state, new_state)
    VALUES (p_lot_id, v_next_role_id, v_current_status, 'Queued');
    
    RETURN QUERY SELECT TRUE, v_next_role_id, 'Transition successful';
END;
$$ LANGUAGE plpgsql;

-- ------------------------------------------------------------
-- 4. Sync Pending Operations
-- ------------------------------------------------------------

CREATE OR REPLACE PROCEDURE sp_sync_pending_operations(p_factory_node_id VARCHAR(50))
AS $$
DECLARE
    v_operation RECORD;
BEGIN
    FOR v_operation IN 
        SELECT * FROM pending_sync_queue 
        WHERE factory_node_id = p_factory_node_id 
          AND status = 'PENDING'
          AND retry_count < max_retries
        ORDER BY created_at ASC
        FOR UPDATE SKIP LOCKED
    LOOP
        BEGIN
            PERFORM http_post(
                'https://central-server/api/v1/sync/' || v_operation.operation_type,
                v_operation.payload
            );
            
            UPDATE pending_sync_queue
            SET status = 'SYNCED',
                synced_at = CURRENT_TIMESTAMP
            WHERE id = v_operation.id;
            
        EXCEPTION WHEN OTHERS THEN
            UPDATE pending_sync_queue
            SET retry_count = retry_count + 1,
                error_message = SQLERRM
            WHERE id = v_operation.id;
            
            IF v_operation.retry_count + 1 >= v_operation.max_retries THEN
                UPDATE pending_sync_queue
                SET status = 'FAILED'
                WHERE id = v_operation.id;
                
                INSERT INTO audit_logs (action, resource_type, resource_id, notes, factory_node_id)
                VALUES ('SYNC_FAILED', 'pending_sync_queue', v_operation.id,
                        'Operation failed after max retries: ' || v_operation.error_message,
                        p_factory_node_id);
            END IF;
        END;
    END LOOP;
    
    COMMIT;
END;
$$ LANGUAGE plpgsql;

-- ------------------------------------------------------------
-- 5. Quarantine Override
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION sp_supervisor_override_quarantine(
    p_lot_id UUID,
    p_supervisor_id UUID,
    p_action VARCHAR(20),
    p_notes TEXT
) RETURNS BOOLEAN AS $$
DECLARE
    v_supervisor_role VARCHAR(50);
BEGIN
    SELECT r.role_id INTO v_supervisor_role
    FROM users u
    JOIN roles r ON u.role_id = r.id
    WHERE u.id = p_supervisor_id;
    
    IF v_supervisor_role NOT IN ('ROLE-SUP-LOOM-FLOOR-SUPERVISOR', 'ROLE-SYSTEM-ADMIN') THEN
        RAISE EXCEPTION 'Unauthorized: Only supervisors can override quarantine';
    END IF;
    
    UPDATE quarantined_lots
    SET reviewed_by = p_supervisor_id,
        review_notes = p_notes,
        status = p_action || '_COMPLETED',
        resolved_at = CURRENT_TIMESTAMP
    WHERE lot_id = p_lot_id AND status = 'PENDING_REVIEW';
    
    IF p_action = 'APPROVE' THEN
        UPDATE production_lots
        SET status = 'Queued:' || current_role_id,
            metadata = jsonb_set(metadata, '{override}', '"true"'),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = p_lot_id;
    ELSIF p_action = 'REWORK' THEN
        UPDATE production_lots
        SET status = 'Queued:' || current_role_id,
            metadata = jsonb_set(metadata, '{rework}', '"true"'),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = p_lot_id;
    ELSIF p_action = 'REJECT' THEN
        UPDATE production_lots
        SET status = 'Failed',
            updated_at = CURRENT_TIMESTAMP
        WHERE id = p_lot_id;
    END IF;
    
    INSERT INTO audit_logs (user_id, action, resource_type, resource_id, new_values, notes)
    VALUES (p_supervisor_id, 'QUARANTINE_OVERRIDE', 'quarantined_lots', p_lot_id,
            jsonb_build_object('action', p_action, 'notes', p_notes),
            'Supervisor override of quarantine');
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
