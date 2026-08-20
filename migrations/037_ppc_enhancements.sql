-- ============================================================
-- Migration: 037_ppc_enhancements.sql
-- Enhancements for Production Planning & Control
--
-- Adds:
-- - production_lines
-- - shifts
-- - resource_allocation
-- - Widen production_tracking_logs with ERP-wide metrics
-- - Widen bom_consumption_tracking with ERP-wide references
-- ============================================================

-- ------------------------------------------------------------
-- 1. PRODUCTION LINES
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS production_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    line_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    line_name VARCHAR(200) NOT NULL,
    line_type VARCHAR(50) DEFAULT 'JACQUARD' CHECK (line_type IN ('JACQUARD', 'DOBBY', 'HANDLOOM', 'POWERLOOM', 'AUTO')),
    total_looms INTEGER DEFAULT 0,
    total_workers INTEGER DEFAULT 0,
    supervisor_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'MAINTENANCE', 'IDLE', 'SHUTDOWN')),
    efficiency_pct DECIMAL(5,2),
    oee_score DECIMAL(5,2),
    capacity_meters_per_day DECIMAL(10,2),
    current_utilization_pct DECIMAL(5,2),
    location_zone VARCHAR(100),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_production_lines_factory ON production_lines(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_production_lines_status ON production_lines(status);

-- ------------------------------------------------------------
-- 2. SHIFTS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shifts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    shift_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    line_id UUID REFERENCES production_lines(id),
    shift_name VARCHAR(100) NOT NULL,
    shift_type VARCHAR(50) DEFAULT 'DAY' CHECK (shift_type IN ('DAY', 'NIGHT', 'EVENING', 'FLEXI')),
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    total_workers_scheduled INTEGER,
    total_looms_scheduled INTEGER,
    actual_workers_present INTEGER,
    actual_looms_running INTEGER,
    supervisor_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'SCHEDULED' CHECK (status IN ('SCHEDULED', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED')),
    date DATE NOT NULL,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_shifts_factory ON shifts(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_shifts_line ON shifts(line_id);
CREATE INDEX IF NOT EXISTS idx_shifts_date ON shifts(date);

-- ------------------------------------------------------------
-- 3. RESOURCE ALLOCATION
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS resource_allocation (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    allocation_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    plan_line_id UUID REFERENCES production_plan_lines(id),
    order_id UUID REFERENCES orders(id),
    resource_type VARCHAR(50) NOT NULL CHECK (resource_type IN ('LOOM', 'WORKER', 'WARP_BEAM', 'PIRN', 'ZARI_SPOOL', 'DYE_BATH', 'FINISHING_MACHINE')),
    resource_id VARCHAR(100) NOT NULL,
    resource_name VARCHAR(200),
    allocated_qty DECIMAL(10,2) DEFAULT 1,
    unit_of_measure VARCHAR(20) DEFAULT 'UNIT',
    allocated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deallocated_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) DEFAULT 'ALLOCATED' CHECK (status IN ('ALLOCATED', 'IN_USE', 'RELEASED', 'MAINTENANCE')),
    allocated_by UUID REFERENCES users(id),
    released_by UUID REFERENCES users(id),
    notes TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_resource_allocation_factory ON resource_allocation(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_resource_allocation_plan_line ON resource_allocation(plan_line_id);
CREATE INDEX IF NOT EXISTS idx_resource_allocation_resource ON resource_allocation(resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_resource_allocation_status ON resource_allocation(status);

-- ------------------------------------------------------------
-- 4. WIDEN PRODUCTION_TRACKING_LOGS
-- ------------------------------------------------------------
ALTER TABLE production_tracking_logs
ADD COLUMN IF NOT EXISTS production_line_id UUID REFERENCES production_lines(id),
ADD COLUMN IF NOT EXISTS shift_id UUID REFERENCES shifts(id),
ADD COLUMN IF NOT EXISTS order_id UUID REFERENCES orders(id),
ADD COLUMN IF NOT EXISTS style_id UUID REFERENCES styles(id),
ADD COLUMN IF NOT EXISTS sku_id VARCHAR(100) REFERENCES sku_catalog(sku_id),
ADD COLUMN IF NOT EXISTS ai_efficiency_prediction DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS ai_downtime_prediction BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS ai_downtime_reason_suggestion VARCHAR(200),
ADD COLUMN IF NOT EXISTS environmental_temperature_celsius DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS environmental_humidity_pct DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS warp_tension_variance_cn DECIMAL(6,2),
ADD COLUMN IF NOT EXISTS picks_efficiency_pct DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS waste_generated_grams DECIMAL(10,2),
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- ------------------------------------------------------------
-- 5. WIDEN BOM_CONSUMPTION_TRACKING
-- ------------------------------------------------------------
ALTER TABLE bom_consumption_tracking
ADD COLUMN IF NOT EXISTS production_line_id UUID REFERENCES production_lines(id),
ADD COLUMN IF NOT EXISTS order_id UUID REFERENCES orders(id),
ADD COLUMN IF NOT EXISTS style_id UUID REFERENCES styles(id),
ADD COLUMN IF NOT EXISTS sku_id VARCHAR(100) REFERENCES sku_catalog(sku_id),
ADD COLUMN IF NOT EXISTS lot_batch_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS ai_consumption_forecast DECIMAL(10,4),
ADD COLUMN IF NOT EXISTS ai_anomaly_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS inventory_bin_id UUID,
ADD COLUMN IF NOT EXISTS warehouse_zone VARCHAR(50);

-- ------------------------------------------------------------
-- 6. WIDEN QUALITY_COMPLIANCE_CHECKS
-- ------------------------------------------------------------
ALTER TABLE quality_compliance_checks
ADD COLUMN IF NOT EXISTS production_line_id UUID REFERENCES production_lines(id),
ADD COLUMN IF NOT EXISTS shift_id UUID REFERENCES shifts(id),
ADD COLUMN IF NOT EXISTS style_id UUID REFERENCES styles(id),
ADD COLUMN IF NOT EXISTS sku_id VARCHAR(100) REFERENCES sku_catalog(sku_id),
ADD COLUMN IF NOT EXISTS order_id UUID REFERENCES orders(id),
ADD COLUMN IF NOT EXISTS diwali_facet_id UUID REFERENCES diwali_cultural_facets(id),
ADD COLUMN IF NOT EXISTS diwali_state_id UUID REFERENCES diwali_cultural_states(id),
ADD COLUMN IF NOT EXISTS cultural_compliance_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS environmental_temperature_celsius DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS environmental_humidity_pct DECIMAL(5,2);

-- ------------------------------------------------------------
-- 7. WIDEN COMPLIANCE_CERTIFICATES
-- ------------------------------------------------------------
ALTER TABLE compliance_certificates
ADD COLUMN IF NOT EXISTS production_line_id UUID REFERENCES production_lines(id),
ADD COLUMN IF NOT EXISTS style_id UUID REFERENCES styles(id),
ADD COLUMN IF NOT EXISTS sku_id VARCHAR(100) REFERENCES sku_catalog(sku_id),
ADD COLUMN IF NOT EXISTS order_id UUID REFERENCES orders(id);

-- ------------------------------------------------------------
-- 8. TRIGGERS
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_ppc_enhancements_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_production_lines_updated_at ON production_lines;
CREATE TRIGGER trigger_update_production_lines_updated_at
    BEFORE UPDATE ON production_lines
    FOR EACH ROW EXECUTE FUNCTION update_ppc_enhancements_timestamp();

DROP TRIGGER IF EXISTS trigger_update_shifts_updated_at ON shifts;
CREATE TRIGGER trigger_update_shifts_updated_at
    BEFORE UPDATE ON shifts
    FOR EACH ROW EXECUTE FUNCTION update_ppc_enhancements_timestamp();

DROP TRIGGER IF EXISTS trigger_update_resource_allocation_updated_at ON resource_allocation;
CREATE TRIGGER trigger_update_resource_allocation_updated_at
    BEFORE UPDATE ON resource_allocation
    FOR EACH ROW EXECUTE FUNCTION update_ppc_enhancements_timestamp();

-- ------------------------------------------------------------
-- 9. PERMISSIONS
-- ------------------------------------------------------------
UPDATE roles
SET permitted_operations = permitted_operations || '["ppc:line:read","ppc:line:write","ppc:shift:read","ppc:shift:write","ppc:resource:read","ppc:resource:write","ppc:allocate","ppc:deallocate"]'
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
    'ROLE-FILATURE-SUPPLIER',
    'ROLE-PRODUCTION-PLANNING'
);
