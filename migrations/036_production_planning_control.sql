-- ============================================================
-- Migration: 036_production_planning_control.sql
-- Production Planning & Control (PPC)
-- Cloud-based, mobile-enabled ERP module
--
-- Modules:
-- 1. Bill of Materials (BOM)
-- 2. Style & Order Planning
-- 3. Production Planning & Scheduling
-- 4. Production Tracking & Analysis
-- 5. Quality & Compliance Management
-- ============================================================

-- ------------------------------------------------------------
-- 1. BOM HEADER
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bom_headers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bom_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    sku_id VARCHAR(100) REFERENCES sku_catalog(sku_id),
    design_id VARCHAR(100) REFERENCES design_files(design_id),
    version VARCHAR(50) DEFAULT '1.0',
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'ARCHIVED', 'REPLACED')),
    total_material_cost DECIMAL(12,2),
    total_labor_cost DECIMAL(12,2),
    total_overhead_cost DECIMAL(12,2),
    total_cost_per_unit DECIMAL(12,2),
    approved_by UUID REFERENCES users(id),
    approved_at TIMESTAMP WITH TIME ZONE,
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_bom_headers_factory ON bom_headers(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_bom_headers_sku ON bom_headers(sku_id);
CREATE INDEX IF NOT EXISTS idx_bom_headers_status ON bom_headers(status);

-- ------------------------------------------------------------
-- 2. BOM LINES (Components / Raw Materials)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bom_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bom_id UUID REFERENCES bom_headers(id) ON DELETE CASCADE,
    factory_node_id VARCHAR(50) NOT NULL,
    line_no INTEGER NOT NULL,
    component_type VARCHAR(50) DEFAULT 'RAW_MATERIAL' CHECK (component_type IN ('RAW_MATERIAL', 'COMPONENT', 'CONSUMABLE', 'ZARI', 'DYE_CHEMICAL', 'PACKAGING')),
    item_code VARCHAR(100),
    item_description TEXT,
    uom VARCHAR(20) DEFAULT 'KG',
    quantity_per_unit DECIMAL(10,4) NOT NULL,
    unit_cost DECIMAL(10,2),
    total_cost DECIMAL(12,2),
    wastage_pct DECIMAL(5,2) DEFAULT 0,
    wastage_qty DECIMAL(10,4) DEFAULT 0,
    supplier_id VARCHAR(100),
    lead_time_days INTEGER,
    available_stock DECIMAL(12,2) DEFAULT 0,
    is_critical BOOLEAN DEFAULT FALSE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(bom_id, line_no)
);
CREATE INDEX IF NOT EXISTS idx_bom_lines_bom ON bom_lines(bom_id);
CREATE INDEX IF NOT EXISTS idx_bom_lines_factory ON bom_lines(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_bom_lines_component ON bom_lines(component_type);

-- ------------------------------------------------------------
-- 3. STYLES (Product Styles / Designs for Production)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS styles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    style_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    style_name VARCHAR(500) NOT NULL,
    style_code VARCHAR(100),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    hook_count INTEGER,
    silk_type VARCHAR(50),
    zari_type VARCHAR(50),
    color_family VARCHAR(50),
    target_price DECIMAL(10,2),
    target_market VARCHAR(50) DEFAULT 'EXPORT' CHECK (target_market IN ('EXPORT', 'DOMESTIC', 'BOTH')),
    season VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    launched_at DATE,
    discontinued_at DATE,
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_styles_factory ON styles(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_styles_market ON styles(target_market);
CREATE INDEX IF NOT EXISTS idx_styles_category ON styles(category);

-- ------------------------------------------------------------
-- 4. ORDERS (Export / Domestic)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    order_type VARCHAR(50) DEFAULT 'DOMESTIC' CHECK (order_type IN ('EXPORT', 'DOMESTIC', 'B2B', 'SAMPLE')),
    buyer_id VARCHAR(100),
    buyer_name VARCHAR(500),
    style_id UUID REFERENCES styles(id),
    sku_id VARCHAR(100) REFERENCES sku_catalog(sku_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'INR',
    order_date DATE NOT NULL,
    delivery_date DATE NOT NULL,
    status VARCHAR(50) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'CONFIRMED', 'IN_PRODUCTION', 'READY_FOR_DISPATCH', 'DISPATCHED', 'DELIVERED', 'CANCELLED')),
    priority VARCHAR(50) DEFAULT 'MEDIUM' CHECK (priority IN ('LOW', 'MEDIUM', 'HIGH', 'URGENT')),
    shipment_mode VARCHAR(50),
    destination_port VARCHAR(200),
    incoterms VARCHAR(50),
    payment_terms VARCHAR(100),
    assigned_production_line VARCHAR(100),
    notes TEXT,
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_orders_factory ON orders(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_orders_type ON orders(order_type);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_delivery ON orders(delivery_date);
CREATE INDEX IF NOT EXISTS idx_orders_style ON orders(style_id);

-- ------------------------------------------------------------
-- 5. PRODUCTION PLANS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_plans (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plan_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    plan_name VARCHAR(500) NOT NULL,
    plan_type VARCHAR(50) DEFAULT 'MONTHLY' CHECK (plan_type IN ('DAILY', 'WEEKLY', 'MONTHLY', 'SEASONAL')),
    status VARCHAR(50) DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'APPROVED', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED')),
    plan_start_date DATE NOT NULL,
    plan_end_date DATE NOT NULL,
    total_looms INTEGER,
    total_workers INTEGER,
    total_production_target DECIMAL(12,2),
    total_production_actual DECIMAL(12,2) DEFAULT 0,
    efficiency_pct DECIMAL(5,2),
    oee_score DECIMAL(5,2),
    approved_by UUID REFERENCES users(id),
    approved_at TIMESTAMP WITH TIME ZONE,
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_production_plans_factory ON production_plans(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_production_plans_status ON production_plans(status);
CREATE INDEX IF NOT EXISTS idx_production_plans_dates ON production_plans(plan_start_date, plan_end_date);

-- ------------------------------------------------------------
-- 6. PRODUCTION PLAN LINES
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_plan_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    plan_id UUID REFERENCES production_plans(id) ON DELETE CASCADE,
    factory_node_id VARCHAR(50) NOT NULL,
    line_no INTEGER NOT NULL,
    order_id UUID REFERENCES orders(id),
    style_id UUID REFERENCES styles(id),
    sku_id VARCHAR(100) REFERENCES sku_catalog(sku_id),
    loom_id VARCHAR(100),
    assigned_weaver_id UUID REFERENCES users(id),
    planned_qty DECIMAL(12,2) NOT NULL,
    actual_qty DECIMAL(12,2) DEFAULT 0,
    pending_qty DECIMAL(12,2) DEFAULT 0,
    rejected_qty DECIMAL(12,2) DEFAULT 0,
    start_date DATE,
    end_date DATE,
    status VARCHAR(50) DEFAULT 'PLANNED' CHECK (status IN ('PLANNED', 'IN_PROGRESS', 'COMPLETED', 'ON_HOLD', 'CANCELLED')),
    completion_pct DECIMAL(5,2) DEFAULT 0,
    notes TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(plan_id, line_no)
);
CREATE INDEX IF NOT EXISTS idx_production_plan_lines_plan ON production_plan_lines(plan_id);
CREATE INDEX IF NOT EXISTS idx_production_plan_lines_order ON production_plan_lines(order_id);
CREATE INDEX IF NOT EXISTS idx_production_plan_lines_status ON production_plan_lines(status);

-- ------------------------------------------------------------
-- 7. PRODUCTION TRACKING LOGS (Real-time)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_tracking_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tracking_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    plan_line_id UUID REFERENCES production_plan_lines(id),
    order_id UUID REFERENCES orders(id),
    loom_id VARCHAR(100),
    weaver_id UUID REFERENCES users(id),
    shift_id VARCHAR(100),
    operation_type VARCHAR(50) NOT NULL,
    picks_per_minute INTEGER,
    warp_tension_cn DECIMAL(6,2),
    temperature_celsius DECIMAL(5,2),
    humidity_pct DECIMAL(5,2),
    output_meters DECIMAL(10,2) DEFAULT 0,
    defect_count INTEGER DEFAULT 0,
    breakage_count INTEGER DEFAULT 0,
    downtime_minutes INTEGER DEFAULT 0,
    downtime_reason VARCHAR(200),
    ai_anomaly_score DECIMAL(5,4),
    ai_anomaly_flag BOOLEAN DEFAULT FALSE,
    supervisor_notes TEXT,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_production_tracking_logs_factory ON production_tracking_logs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_production_tracking_logs_plan_line ON production_tracking_logs(plan_line_id);
CREATE INDEX IF NOT EXISTS idx_production_tracking_logs_loom ON production_tracking_logs(loom_id);
CREATE INDEX IF NOT EXISTS idx_production_tracking_logs_recorded ON production_tracking_logs(recorded_at);

-- ------------------------------------------------------------
-- 8. QUALITY & COMPLIANCE CHECKS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quality_compliance_checks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    check_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    check_type VARCHAR(50) NOT NULL CHECK (check_type IN ('IN_PROCESS', 'FINAL', 'PRE_SHIPMENT', 'ZARI_AUDIT', 'DYE_FASTNESS', 'BURN_TEST', 'DIMENSIONAL')),
    plan_line_id UUID REFERENCES production_plan_lines(id),
    order_id UUID REFERENCES orders(id),
    sku_id VARCHAR(100) REFERENCES sku_catalog(sku_id),
    saree_serial_id VARCHAR(100),
    inspector_id UUID REFERENCES users(id),
    defect_type VARCHAR(100),
    defect_severity VARCHAR(50) CHECK (defect_severity IN ('MINOR', 'MAJOR', 'CRITICAL')),
    defect_count INTEGER DEFAULT 0,
    grade VARCHAR(50) CHECK (grade IN ('A_PLUS', 'A', 'B', 'C', 'REJECT')),
    status VARCHAR(50) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PASSED', 'FAILED', 'REWORK', 'CERTIFIED')),
    ai_defect_score DECIMAL(5,4),
    ai_grade_prediction VARCHAR(50),
    ai_confidence_score DECIMAL(5,4),
    corrective_action TEXT,
    certified_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_quality_compliance_checks_factory ON quality_compliance_checks(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_quality_compliance_checks_plan_line ON quality_compliance_checks(plan_line_id);
CREATE INDEX IF NOT EXISTS idx_quality_compliance_checks_status ON quality_compliance_checks(status);
CREATE INDEX IF NOT EXISTS idx_quality_compliance_checks_type ON quality_compliance_checks(check_type);

-- ------------------------------------------------------------
-- 9. COMPLIANCE CERTIFICATES
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS compliance_certificates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    certificate_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    certificate_type VARCHAR(50) NOT NULL CHECK (certificate_type IN ('SILK_MARK', 'SIZE_TEX', 'GI_TAG', 'OEKO_TEX', 'BIS', 'EXPORT_LICENSE')),
    order_id UUID REFERENCES orders(id),
    sku_id VARCHAR(100) REFERENCES sku_catalog(sku_id),
    saree_serial_id VARCHAR(100),
    issued_by UUID REFERENCES users(id),
    valid_from DATE,
    valid_until DATE,
    certificate_number VARCHAR(200),
    issuing_authority VARCHAR(500),
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'EXPIRED', 'REVOKED', 'RENEWED')),
    document_url VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_compliance_certificates_factory ON compliance_certificates(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_compliance_certificates_order ON compliance_certificates(order_id);
CREATE INDEX IF NOT EXISTS idx_compliance_certificates_status ON compliance_certificates(status);

-- ------------------------------------------------------------
-- 10. MOBILE SYNC LOG (for offline-first mobile apps)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mobile_sync_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sync_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    user_id UUID REFERENCES users(id),
    device_id VARCHAR(200),
    device_type VARCHAR(50) DEFAULT 'ANDROID' CHECK (device_type IN ('ANDROID', 'IOS', 'TABLET', 'WEB')),
    sync_type VARCHAR(50) DEFAULT 'FULL' CHECK (sync_type IN ('FULL', 'PARTIAL', 'PUSH', 'PULL')),
    entity_type VARCHAR(50),
    entity_id VARCHAR(100),
    operation VARCHAR(50) CHECK (operation IN ('CREATE', 'UPDATE', 'DELETE', 'READ')),
    sync_status VARCHAR(50) DEFAULT 'PENDING' CHECK (sync_status IN ('PENDING', 'SUCCESS', 'FAILED', 'CONFLICT')),
    payload JSONB DEFAULT '{}'::jsonb,
    error_message TEXT,
    synced_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_mobile_sync_log_factory ON mobile_sync_log(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_mobile_sync_log_user ON mobile_sync_log(user_id);
CREATE INDEX IF NOT EXISTS idx_mobile_sync_log_status ON mobile_sync_log(sync_status);
CREATE INDEX IF NOT EXISTS idx_mobile_sync_log_entity ON mobile_sync_log(entity_type, entity_id);

-- ------------------------------------------------------------
-- 11. PUSH NOTIFICATION LOG
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS push_notification_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    notification_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    user_id UUID REFERENCES users(id),
    device_id VARCHAR(200),
    platform VARCHAR(50) CHECK (platform IN ('ANDROID', 'IOS', 'WEB')),
    notification_type VARCHAR(50) NOT NULL,
    title VARCHAR(500),
    body TEXT,
    data_payload JSONB DEFAULT '{}'::jsonb,
    sent_status VARCHAR(50) DEFAULT 'PENDING' CHECK (sent_status IN ('PENDING', 'SENT', 'DELIVERED', 'FAILED', 'READ')),
    sent_at TIMESTAMP WITH TIME ZONE,
    read_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_push_notification_log_factory ON push_notification_log(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_push_notification_log_user ON push_notification_log(user_id);
CREATE INDEX IF NOT EXISTS idx_push_notification_log_status ON push_notification_log(sent_status);

-- ------------------------------------------------------------
-- 12. PRODUCTION EFFICIENCY ANALYTICS (Materialized View-like)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_efficiency_analytics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    factory_node_id VARCHAR(50) NOT NULL,
    plan_line_id UUID REFERENCES production_plan_lines(id),
    loom_id VARCHAR(100),
    weaver_id UUID REFERENCES users(id),
    shift_date DATE NOT NULL,
    planned_output_meters DECIMAL(10,2) DEFAULT 0,
    actual_output_meters DECIMAL(10,2) DEFAULT 0,
    efficiency_pct DECIMAL(5,2) DEFAULT 0,
    oee_score DECIMAL(5,2) DEFAULT 0,
    defect_rate_pct DECIMAL(5,2) DEFAULT 0,
    downtime_minutes INTEGER DEFAULT 0,
    utilization_pct DECIMAL(5,2) DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_production_efficiency_analytics_factory ON production_efficiency_analytics(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_production_efficiency_analytics_loom ON production_efficiency_analytics(loom_id);
CREATE INDEX IF NOT EXISTS idx_production_efficiency_analytics_date ON production_efficiency_analytics(shift_date);

-- ------------------------------------------------------------
-- 13. BOM CONSUMPTION TRACKING
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bom_consumption_tracking (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    consumption_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    bom_line_id UUID REFERENCES bom_lines(id),
    plan_line_id UUID REFERENCES production_plan_lines(id),
    order_id UUID REFERENCES orders(id),
    item_code VARCHAR(100),
    planned_qty DECIMAL(10,4) NOT NULL,
    actual_qty DECIMAL(10,4) NOT NULL,
    variance_qty DECIMAL(10,4) DEFAULT 0,
    variance_pct DECIMAL(5,2) DEFAULT 0,
    uom VARCHAR(20) DEFAULT 'KG',
    consumed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    recorded_by UUID REFERENCES users(id),
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_bom_consumption_tracking_factory ON bom_consumption_tracking(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_bom_consumption_tracking_bom_line ON bom_consumption_tracking(bom_line_id);
CREATE INDEX IF NOT EXISTS idx_bom_consumption_tracking_order ON bom_consumption_tracking(order_id);

-- ------------------------------------------------------------
-- 14. TRIGGERS
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_ppc_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_bom_headers_updated_at ON bom_headers;
CREATE TRIGGER trigger_update_bom_headers_updated_at
    BEFORE UPDATE ON bom_headers
    FOR EACH ROW EXECUTE FUNCTION update_ppc_timestamp();

DROP TRIGGER IF EXISTS trigger_update_styles_updated_at ON styles;
CREATE TRIGGER trigger_update_styles_updated_at
    BEFORE UPDATE ON styles
    FOR EACH ROW EXECUTE FUNCTION update_ppc_timestamp();

DROP TRIGGER IF EXISTS trigger_update_orders_updated_at ON orders;
CREATE TRIGGER trigger_update_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_ppc_timestamp();

DROP TRIGGER IF EXISTS trigger_update_production_plans_updated_at ON production_plans;
CREATE TRIGGER trigger_update_production_plans_updated_at
    BEFORE UPDATE ON production_plans
    FOR EACH ROW EXECUTE FUNCTION update_ppc_timestamp();

DROP TRIGGER IF EXISTS trigger_update_production_plan_lines_updated_at ON production_plan_lines;
CREATE TRIGGER trigger_update_production_plan_lines_updated_at
    BEFORE UPDATE ON production_plan_lines
    FOR EACH ROW EXECUTE FUNCTION update_ppc_timestamp();

DROP TRIGGER IF EXISTS trigger_update_quality_compliance_checks_updated_at ON quality_compliance_checks;
CREATE TRIGGER trigger_update_quality_compliance_checks_updated_at
    BEFORE UPDATE ON quality_compliance_checks
    FOR EACH ROW EXECUTE FUNCTION update_ppc_timestamp();

DROP TRIGGER IF EXISTS trigger_update_compliance_certificates_updated_at ON compliance_certificates;
CREATE TRIGGER trigger_update_compliance_certificates_updated_at
    BEFORE UPDATE ON compliance_certificates
    FOR EACH ROW EXECUTE FUNCTION update_ppc_timestamp();

-- ------------------------------------------------------------
-- 15. PERMISSIONS
-- ------------------------------------------------------------
UPDATE roles
SET permitted_operations = permitted_operations || '["ppc:plan:read","ppc:plan:write","ppc:plan:approve","ppc:schedule:read","ppc:schedule:write","bom:read","bom:write","bom:approve","order:read","order:write","order:schedule","style:read","style:write","tracking:read","tracking:write","quality:read","quality:write","compliance:read","compliance:write","compliance:certify","analytics:read","mobile:sync"]'
WHERE role_id IN (
    'ROLE-SUP-LOOM-FLOOR-SUPERVISOR',
    'ROLE-MASTER-WEAVER',
    'ROLE-STORE-INVENTORY-MANAGER',
    'ROLE-PETNI-MASTER',
    'ROLE-CARD-PUNCHER',
    'ROLE-GRAPH-DRAFTER',
    'ROLE-SKU-MANAGER',
    'ROLE-QUALITY-INSPECTOR',
    'ROLE-QA-DYEING-INSPECTOR',
    'ROLE-SILK-MARK-OFFICER',
    'ROLE-LOG-FINISHING',
    'ROLE-SYSTEM-ADMIN',
    'ROLE-FILATURE-SUPPLIER'
);
