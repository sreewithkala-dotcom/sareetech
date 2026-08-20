-- ============================================================
-- AI-POWERED SILK & FABRIC MANUFACTURING ERP SYSTEM
-- Migration: 001_initial_schema.sql
-- PostgreSQL 14+ Production-Ready Schema
-- ============================================================

-- ------------------------------------------------------------
-- EXTENSIONS
-- ------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "btree_gist";

-- ------------------------------------------------------------
-- 1. ROLES & USERS (Authentication & Authorization)
-- ------------------------------------------------------------

CREATE TABLE roles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    role_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    permitted_operations JSONB NOT NULL DEFAULT '[]'::jsonb,
    factory_node_scope JSONB DEFAULT '[]'::jsonb,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_role_id_format CHECK (role_id ~* '^ROLE-[A-Z0-9-]+$')
);

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role_id UUID NOT NULL REFERENCES roles(id) ON DELETE RESTRICT,
    factory_node_id VARCHAR(50) NOT NULL,
    employee_id VARCHAR(50) UNIQUE,
    full_name VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    last_login TIMESTAMP WITH TIME ZONE,
    login_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

CREATE TABLE user_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    jti VARCHAR(255) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    ip_address INET,
    user_agent TEXT,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    revoked BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_users_role_id ON users(role_id);
CREATE INDEX idx_users_factory ON users(factory_node_id);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_sessions_jti ON user_sessions(jti);
CREATE INDEX idx_sessions_user ON user_sessions(user_id);
CREATE INDEX idx_sessions_expires ON user_sessions(expires_at);

-- ------------------------------------------------------------
-- 2. FACTORY NODES
-- ------------------------------------------------------------

CREATE TABLE factory_nodes (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255),
    timezone VARCHAR(50) DEFAULT 'UTC',
    region VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    config JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- 3. PRODUCTION LOTS & WORKFLOW STATES
-- ------------------------------------------------------------

CREATE TABLE production_lots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lot_number VARCHAR(50) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL REFERENCES factory_nodes(id),
    asset_id VARCHAR(100) UNIQUE NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'Queued',
    current_role_id UUID REFERENCES roles(id),
    locked_by UUID REFERENCES users(id),
    priority INTEGER DEFAULT 0,
    metadata JSONB DEFAULT '{}'::jsonb,
    started_at TIMESTAMP WITH TIME ZONE,
    certified_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_lot_status CHECK (status IN (
        'Queued',
        'InputScanned',
        'InProgress:AssistantWeaver',
        'InProgress:BobbinWinder',
        'InProgress:CardPuncher',
        'InProgress:FilatureSupplier',
        'InProgress:GraphDrafter',
        'InProgress:LogFinishing',
        'InProgress:LoomHarnessSetter',
        'InProgress:MasterColorist',
        'InProgress:MasterWeaver',
        'InProgress:PetniMaster',
        'InProgress:PirnWinders',
        'InProgress:QADyeingInspector',
        'InProgress:QualityInspector',
        'InProgress:SilkDegummingMaster',
        'InProgress:SilkGrader',
        'InProgress:SilkMarkOfficer',
        'InProgress:SkeinDyeMaster',
        'InProgress:StoreInventoryManager',
        'InProgress:SUPLoomFloorSupervisor',
        'InProgress:ThrowsterTwister',
        'InProgress:WarpBeamPreparation',
        'InProgress:WarpJoiner',
        'InProgress:ZariInspector',
        'Certified:ZariDefectDetection',
        'Certified:DyeColoringDefectDetection',
        'Certified:WarpDefectDetection',
        'Certified:FabricDefectDetection',
        'Certified:WeavingDefectDetection',
        'Certified:DemandForecasting',
        'Completed',
        'Failed',
        'Quarantined'
    ))
);

CREATE TABLE workflow_states (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lot_id UUID NOT NULL REFERENCES production_lots(id) ON DELETE CASCADE,
    role_id UUID NOT NULL REFERENCES roles(id),
    previous_state VARCHAR(50) NOT NULL,
    new_state VARCHAR(50) NOT NULL,
    entry_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    exit_timestamp TIMESTAMP WITH TIME ZONE,
    operator_id UUID REFERENCES users(id),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_production_lots_status ON production_lots(status);
CREATE INDEX idx_production_lots_factory ON production_lots(factory_node_id);
CREATE INDEX idx_production_lots_asset ON production_lots(asset_id);
CREATE INDEX idx_production_lots_role ON production_lots(current_role_id);
CREATE INDEX idx_workflow_states_lot ON workflow_states(lot_id);
CREATE INDEX idx_workflow_states_timestamp ON workflow_states(entry_timestamp);

-- ------------------------------------------------------------
-- 4. SCANNER LOGS
-- ------------------------------------------------------------

CREATE TABLE scanner_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lot_id UUID NOT NULL REFERENCES production_lots(id),
    scan_type VARCHAR(20) NOT NULL CHECK (scan_type IN ('input', 'output')),
    asset_id VARCHAR(100) NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    workstation_id VARCHAR(100),
    operator_id UUID REFERENCES users(id),
    scan_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL CHECK (status IN ('success', 'failed', 'partial', 'duplicate')),
    validation_result JSONB,
    error_message TEXT,
    latency_ms INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_scanner_logs_lot ON scanner_logs(lot_id);
CREATE INDEX idx_scanner_logs_asset ON scanner_logs(asset_id);
CREATE INDEX idx_scanner_logs_timestamp ON scanner_logs(scan_timestamp);
CREATE INDEX idx_scanner_logs_type_status ON scanner_logs(scan_type, status);

-- ------------------------------------------------------------
-- 5. AI INSPECTION CERTIFICATES
-- ------------------------------------------------------------

CREATE TABLE ai_inspection_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lot_id UUID NOT NULL REFERENCES production_lots(id) ON DELETE CASCADE,
    role_id UUID NOT NULL REFERENCES roles(id),
    step_name VARCHAR(100) NOT NULL,
    certification_json JSONB NOT NULL,
    confidence_score DECIMAL(3,2) NOT NULL CHECK (confidence_score BETWEEN 0.0 AND 1.0),
    verdict VARCHAR(20) NOT NULL CHECK (verdict IN ('PASS', 'FAIL', 'WARNING')),
    defect_classes JSONB DEFAULT '[]'::jsonb,
    metrics JSONB DEFAULT '{}'::jsonb,
    model_version VARCHAR(50),
    processing_time_ms INTEGER,
    cryptographic_check_token VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ai_certificates_lot ON ai_inspection_certificates(lot_id);
CREATE INDEX idx_ai_certificates_role ON ai_inspection_certificates(role_id);
CREATE INDEX idx_ai_certificates_step ON ai_inspection_certificates(step_name);
CREATE INDEX idx_ai_certificates_verdict ON ai_inspection_certificates(verdict);
CREATE INDEX idx_ai_certificates_created ON ai_inspection_certificates(created_at);

-- ------------------------------------------------------------
-- 6. WORKFLOW TRANSITIONS
-- ------------------------------------------------------------

CREATE TABLE workflow_transitions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    from_role_id UUID REFERENCES roles(id),
    to_role_id UUID REFERENCES roles(id),
    required_certificate_step VARCHAR(100),
    allowed_verdicts JSONB DEFAULT '["PASS"]'::jsonb,
    is_mandatory BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- 7. AUDIT LOGS
-- ------------------------------------------------------------

CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id),
    action VARCHAR(100) NOT NULL,
    resource_type VARCHAR(50),
    resource_id UUID,
    old_values JSONB,
    new_values JSONB,
    ip_address INET,
    user_agent TEXT,
    factory_node_id VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_logs_user ON audit_logs(user_id);
CREATE INDEX idx_audit_logs_resource ON audit_logs(resource_type, resource_id);
CREATE INDEX idx_audit_logs_timestamp ON audit_logs(created_at);
CREATE INDEX idx_audit_logs_action ON audit_logs(action);

-- ------------------------------------------------------------
-- 8. OFFLINE RESILIENCE
-- ------------------------------------------------------------

CREATE TABLE pending_sync_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    operation_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 10,
    status VARCHAR(20) DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    synced_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT
);

CREATE INDEX idx_pending_sync_status ON pending_sync_queue(status, factory_node_id);

-- ------------------------------------------------------------
-- 9. QUARANTINE MANAGEMENT
-- ------------------------------------------------------------

CREATE TABLE quarantined_lots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lot_id UUID NOT NULL REFERENCES production_lots(id),
    reason TEXT NOT NULL,
    ai_certificate_id UUID REFERENCES ai_inspection_certificates(id),
    quarantined_by UUID REFERENCES users(id),
    reviewed_by UUID REFERENCES users(id),
    review_notes TEXT,
    status VARCHAR(20) DEFAULT 'PENDING_REVIEW',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP WITH TIME ZONE
);

-- ------------------------------------------------------------
-- 10. MATERIALS & INVENTORY
-- ------------------------------------------------------------

CREATE TABLE materials (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sku VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(50),
    unit_of_measure VARCHAR(20),
    factory_node_id VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE inventory_movements (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    material_id UUID REFERENCES materials(id),
    lot_id UUID REFERENCES production_lots(id),
    movement_type VARCHAR(20) CHECK (movement_type IN ('IN', 'OUT', 'TRANSFER', 'ADJUST')),
    quantity DECIMAL(12,3) NOT NULL,
    unit_of_measure VARCHAR(20),
    from_location VARCHAR(100),
    to_location VARCHAR(100),
    operator_id UUID REFERENCES users(id),
    factory_node_id VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- 11. TRIGGERS: Auto-update timestamps
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_production_lots_updated_at BEFORE UPDATE ON production_lots
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ai_certificates_updated_at BEFORE UPDATE ON ai_inspection_certificates
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_roles_updated_at BEFORE UPDATE ON roles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ------------------------------------------------------------
-- 12. SAMPLE DATA: 24 Roles
-- ------------------------------------------------------------

INSERT INTO roles (role_id, name, permitted_operations, description) VALUES
('ROLE-ASSISTANT-WEAVER', 'Assistant Weaver', 
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:weaving"]',
 'Monitor weaving progress, coordinate with looms, report thread breaks'),

('ROLE-BOBBIN-WINDER', 'Bobbin Winder',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari"]',
 'Wind bobbins, manage spool inventory, tag output'),

('ROLE-CARD-PUNCHER', 'Card Puncher (Digital E-Jacquard Programmer)',
 '["scanner:input","scanner:output","lot:read","lot:update","design:read","design:write"]',
 'Program card punches, execute patterns, validate design files'),

('ROLE-FILATURE-SUPPLIER', 'Filature Supplier',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari","ai:trigger:dye"]',
 'Source raw silk, manage supplier relationships, quality intake'),

('ROLE-GRAPH-DRAFTER', 'Graph Drafter (2400 Hook)',
 '["scanner:input","scanner:output","lot:read","lot:update","design:read","design:write","forecasting:read"]',
 'Create fabric designs, draft patterns, configure 2400-hook setups'),

('ROLE-LOG-FINISHING', 'LOG Finishing Transit Specialist',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:fabric"]',
 'Handle finishing operations, transit logistics, temperature monitoring'),

('ROLE-LOOM-HARNESS-SETTER', 'Loom Harness Setter',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:warp"]',
 'Install and configure loom harnesses, tension calibration'),

('ROLE-MASTER-COLORIST', 'Master Colorist',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:dye","color:read","color:write"]',
 'Manage dyeing processes, color consistency, shade approval'),

('ROLE-MASTER-WEAVER', 'Master Weaver',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:fabric","ai:trigger:weaving"]',
 'Oversee master weaving operations, yield statistics, production planning'),

('ROLE-PETNI-MASTER', 'Petni Master',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari"]',
 'Lead petni (silk thread) production, thread count management'),

('ROLE-PIRN-WINDERS', 'Pirn Winders',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari"]',
 'Operate pirn winders, thread insertion, quality checks'),

('ROLE-QA-DYEING-INSPECTOR', 'QA Dyeing Inspector',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:dye","qc:read","qc:write"]',
 'Inspect dyed fabrics, quality assurance, batch approval'),

('ROLE-QUALITY-INSPECTOR', 'Quality Inspector',
 '["scanner:input","scanner:output","lot:read","lot:update","qc:read","qc:write"]',
 'Perform final quality inspections, defect categorization, pass/fail reporting'),

('ROLE-SILK-DEGUMMING-MASTER', 'Silk Degumming Master',
 '["scanner:input","scanner:output","lot:read","lot:update","process:control"]',
 'Process silk degumming, cleaning, impurity removal'),

('ROLE-SILK-GRADER', 'Silk Grader',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari","grading:read","grading:write"]',
 'Grade silk quality, sorting, A/B/C classification'),

('ROLE-SILK-MARK-OFFICER', 'Silk Mark Officer',
 '["scanner:input","scanner:output","lot:read","lot:update","traceability:read","traceability:write"]',
 'Apply markings, traceability tags, QR code generation'),

('ROLE-SYSTEM-ADMIN', 'System/Admin Superuser',
 '["*"]',
 'System administration, master configuration, user management, audit trails'),

('ROLE-SKEIN-DYE-MASTER', 'Skein Dye Master',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:dye"]',
 'Manage skein production, dyeing batches, color matching'),

('ROLE-STORE-INVENTORY-MANAGER', 'Store Inventory Manager',
 '["scanner:input","scanner:output","lot:read","lot:update","inventory:read","inventory:write"]',
 'Manage warehouse inventory, stock levels, reorder points, material tracking'),

('ROLE-SUP-LOOM-FLOOR-SUPERVISOR', 'SUP Loom Floor Supervisor',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:weaving","production:read","production:write"]',
 'Supervise loom floor operations, downtime tracking, production scheduling'),

('ROLE-THROWSTER-TWISTER', 'Throwster or Twister',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari"]',
 'Produce throw items, knitting operations, thread twisting'),

('ROLE-WARP-BEAM-PREPARATION', 'Warp Beam Preparation Specialist (80 Saree Length)',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:warp"]',
 'Prepare warp beams for weaving, alignment checks, tension setup'),

('ROLE-WARP-JOINER', 'Warp Joiner',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:warp"]',
 'Join warp beams, prepare for weaving, knot quality'),

('ROLE-ZARI-INSPECTOR', 'Zari Inspector / Zari Refinery Specialist',
 '["scanner:input","scanner:output","lot:read","lot:update","ai:trigger:zari"]',
 'Inspect zari (metallic thread) quality, refinery process control');

-- ------------------------------------------------------------
-- 13. SAMPLE FACTORY NODE
-- ------------------------------------------------------------

INSERT INTO factory_nodes (id, name, location, timezone, region, config) VALUES
('FACT-BLR-01', 'Bangalore Micro-Factory #1', 'Bangalore, Karnataka', 'Asia/Kolkata', 'ap-south-1',
 '{"loom_type": "mixed", "dyeing_model": "mixed", "scanner_adapters": {"input": "usb_hid", "output": "usb_hid"}, "edge_ai_enabled": true}');
