-- ============================================================
-- Migration: 032_enterprise_features.sql
-- Enterprise-wide AI/ML, Finance, HR, Procurement,
-- Supply Chain, IoT, B2B, and automation tables.
-- ============================================================

-- ------------------------------------------------------------
-- 1. ENTERPRISE CROSS-CUTTING TABLES
-- ------------------------------------------------------------

-- AI/ML Inference Logs
CREATE TABLE IF NOT EXISTS ai_inference_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    inference_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    service_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50),
    entity_type VARCHAR(50),
    entity_id VARCHAR(100),
    input_features JSONB DEFAULT '{}'::jsonb,
    prediction JSONB DEFAULT '{}'::jsonb,
    confidence_score DECIMAL(5,4),
    inference_latency_ms INTEGER,
    edge_deployed BOOLEAN DEFAULT FALSE,
    operator_id UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ai_inference_logs_service ON ai_inference_logs(service_name);
CREATE INDEX IF NOT EXISTS idx_ai_inference_logs_entity ON ai_inference_logs(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_ai_inference_logs_factory ON ai_inference_logs(factory_node_id);

-- AI Model Versions
CREATE TABLE IF NOT EXISTS ai_model_versions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    model_name VARCHAR(100) UNIQUE NOT NULL,
    version VARCHAR(50) NOT NULL,
    deployment_target VARCHAR(50) DEFAULT 'EDGE' CHECK (deployment_target IN ('EDGE', 'CLOUD', 'BOTH')),
    model_path VARCHAR(500),
    accuracy_score DECIMAL(5,4),
    training_dataset_size INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    deployed_at TIMESTAMP WITH TIME ZONE,
    retired_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ai_model_versions_name ON ai_model_versions(model_name);

-- Finance: Journal Entries
CREATE TABLE IF NOT EXISTS finance_journal_entries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    journal_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    entry_type VARCHAR(50) DEFAULT 'STANDARD' CHECK (entry_type IN ('STANDARD', 'PAYOUT', 'PENALTY', 'RESERVE', 'TAX', 'ADJUSTMENT')),
    debit_account VARCHAR(100),
    credit_account VARCHAR(100),
    amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'INR',
    reference_entity_type VARCHAR(50),
    reference_entity_id VARCHAR(100),
    supplier_id VARCHAR(100),
    weaver_id UUID REFERENCES users(id),
    description TEXT,
    posted_by UUID REFERENCES users(id),
    posted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_finance_journal_entries_type ON finance_journal_entries(entry_type);
CREATE INDEX IF NOT EXISTS idx_finance_journal_entries_entity ON finance_journal_entries(reference_entity_type, reference_entity_id);

-- Finance: Supplier Payouts
CREATE TABLE IF NOT EXISTS supplier_payouts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    payout_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    supplier_id VARCHAR(100) NOT NULL,
    raw_lot_id VARCHAR(100),
    payout_amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'INR',
    payout_type VARCHAR(50) DEFAULT 'FULL' CHECK (payout_type IN ('FULL', 'PARTIAL', 'HOLD', 'PENALTY')),
    payment_method VARCHAR(50) DEFAULT 'UPI' CHECK (payment_method IN ('UPI', 'IMPS', 'NEFT', 'CHEQUE', 'CASH')),
    transaction_ref VARCHAR(100),
    smart_contract_tx_hash VARCHAR(100),
    released_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    released_by UUID REFERENCES users(id),
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_supplier_payouts_supplier ON supplier_payouts(supplier_id);
CREATE INDEX IF NOT EXISTS idx_supplier_payouts_factory ON supplier_payouts(factory_node_id);

-- Finance: Payroll Runs
CREATE TABLE IF NOT EXISTS payroll_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    payroll_run_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    run_period_start DATE NOT NULL,
    run_period_end DATE NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'INR',
    status VARCHAR(50) DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'PROCESSED', 'PAID', 'FAILED')),
    processed_by UUID REFERENCES users(id),
    processed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_payroll_runs_factory ON payroll_runs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_payroll_runs_period ON payroll_runs(run_period_start, run_period_end);

-- Finance: Payroll Lines
CREATE TABLE IF NOT EXISTS payroll_lines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    payroll_run_id UUID REFERENCES payroll_runs(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id),
    saree_grade VARCHAR(50),
    base_amount DECIMAL(10,2) NOT NULL,
    penalty_amount DECIMAL(10,2) DEFAULT 0,
    bonus_amount DECIMAL(10,2) DEFAULT 0,
    net_amount DECIMAL(10,2) NOT NULL,
    payment_status VARCHAR(50) DEFAULT 'PENDING' CHECK (payment_status IN ('PENDING', 'PROCESSED', 'PAID', 'FAILED')),
    payment_ref VARCHAR(100),
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_payroll_lines_run ON payroll_lines(payroll_run_id);
CREATE INDEX IF NOT EXISTS idx_payroll_lines_user ON payroll_lines(user_id);

-- HR: Weaver Profiles
CREATE TABLE IF NOT EXISTS hr_weaver_profiles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID UNIQUE NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    factory_node_id VARCHAR(50) NOT NULL,
    guild_id UUID,
    skill_level VARCHAR(50) DEFAULT 'JOURNEYMAN' CHECK (skill_level IN ('APPRENTICE', 'JOURNEYMAN', 'EXPERT', 'MASTER')),
    primary_skill_set JSONB DEFAULT '[]'::jsonb,
    loom_type_preference JSONB DEFAULT '[]'::jsonb,
    max_loom_speed_ppm INTEGER,
    average_yield_score DECIMAL(5,2),
    defect_rate_pct DECIMAL(5,2),
    attendance_score DECIMAL(5,2),
    is_active BOOLEAN DEFAULT TRUE,
    joined_at DATE,
    certified_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_hr_weaver_profiles_user ON hr_weaver_profiles(user_id);
CREATE INDEX IF NOT EXISTS idx_hr_weaver_profiles_factory ON hr_weaver_profiles(factory_node_id);

-- HR: Guild Members
CREATE TABLE IF NOT EXISTS hr_guild_members (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    guild_id UUID NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    factory_node_id VARCHAR(50) NOT NULL,
    role_in_guild VARCHAR(50) DEFAULT 'MEMBER' CHECK (role_in_guild IN ('MEMBER', 'LEAD', 'TRAINER', 'ADMIN')),
    joined_at DATE NOT NULL,
    left_at DATE,
    performance_score DECIMAL(5,2),
    certification_level VARCHAR(50),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_hr_guild_members_guild ON hr_guild_members(guild_id);
CREATE INDEX IF NOT EXISTS idx_hr_guild_members_user ON hr_guild_members(user_id);

-- HR: Attendance & Shifts
CREATE TABLE IF NOT EXISTS hr_shift_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    shift_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id),
    shift_started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    shift_ended_at TIMESTAMP WITH TIME ZONE,
    loom_id VARCHAR(100),
    beam_id VARCHAR(100),
    output_picks INTEGER,
    defect_count INTEGER,
    breakage_count INTEGER,
    ai_attendance_flag BOOLEAN DEFAULT FALSE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_hr_shift_logs_user ON hr_shift_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_hr_shift_logs_factory ON hr_shift_logs(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_hr_shift_logs_started ON hr_shift_logs(shift_started_at);

-- Procurement: Purchase Requisitions
CREATE TABLE IF NOT EXISTS procurement_requisitions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    requisition_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    requested_by UUID REFERENCES users(id),
    item_category VARCHAR(50) NOT NULL,
    item_description TEXT,
    quantity_requested DECIMAL(10,2) NOT NULL,
    unit_of_measure VARCHAR(20) DEFAULT 'KG',
    estimated_unit_cost DECIMAL(10,2),
    total_estimated_cost DECIMAL(12,2),
    priority VARCHAR(50) DEFAULT 'MEDIUM' CHECK (priority IN ('LOW', 'MEDIUM', 'HIGH', 'URGENT')),
    status VARCHAR(50) DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'SUBMITTED', 'APPROVED', 'REJECTED', 'CONVERTED_TO_PO', 'CANCELLED')),
    approved_by UUID REFERENCES users(id),
    approved_at TIMESTAMP WITH TIME ZONE,
    auto_generated BOOLEAN DEFAULT FALSE,
    ai_confidence_score DECIMAL(5,4),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_procurement_requisitions_factory ON procurement_requisitions(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_procurement_requisitions_status ON procurement_requisitions(status);

-- Procurement: Purchase Orders
CREATE TABLE IF NOT EXISTS procurement_orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    po_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    requisition_id UUID REFERENCES procurement_requisitions(id),
    supplier_id VARCHAR(100) NOT NULL,
    item_category VARCHAR(50) NOT NULL,
    quantity_ordered DECIMAL(10,2) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'INR',
    payment_terms VARCHAR(50) DEFAULT 'NET_30',
    delivery_terms VARCHAR(50) DEFAULT 'FACTORY_GATE',
    expected_delivery_date DATE,
    status VARCHAR(50) DEFAULT 'ISSUED' CHECK (status IN ('DRAFT', 'ISSUED', 'ACKNOWLEDGED', 'PARTIAL_DELIVERY', 'FULLY_DELIVERED', 'CLOSED', 'CANCELLED')),
    issued_by UUID REFERENCES users(id),
    acknowledged_at TIMESTAMP WITH TIME ZONE,
    closed_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_procurement_orders_factory ON procurement_orders(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_procurement_orders_supplier ON procurement_orders(supplier_id);
CREATE INDEX IF NOT EXISTS idx_procurement_orders_status ON procurement_orders(status);

-- Procurement: Invoices
CREATE TABLE IF NOT EXISTS procurement_invoices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    invoice_id VARCHAR(100) UNIQUE NOT NULL,
    po_id UUID REFERENCES procurement_orders(id),
    factory_node_id VARCHAR(50) NOT NULL,
    supplier_id VARCHAR(100) NOT NULL,
    invoice_number VARCHAR(100) NOT NULL,
    invoice_date DATE NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'INR',
    match_status VARCHAR(50) DEFAULT 'PENDING' CHECK (match_status IN ('PENDING', 'MATCHED', 'MISMATCH', 'EXCEPTION')),
    matched_by UUID REFERENCES users(id),
    matched_at TIMESTAMP WITH TIME ZONE,
    paid_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_procurement_invoices_po ON procurement_invoices(po_id);
CREATE INDEX IF NOT EXISTS idx_procurement_invoices_status ON procurement_invoices(match_status);

-- Supply Chain: B2B Orders
CREATE TABLE IF NOT EXISTS b2b_orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    b2b_order_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    buyer_id VARCHAR(100) NOT NULL,
    sku_id VARCHAR(100),
    design_code VARCHAR(100),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'INR',
    grade_requirement VARCHAR(50) DEFAULT 'GRADE_A_EXPORT_PREMIUM',
    delivery_date DATE NOT NULL,
    status VARCHAR(50) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'CONFIRMED', 'IN_PRODUCTION', 'READY_FOR_DISPATCH', 'DISPATCHED', 'DELIVERED', 'CANCELLED')),
    matched_saree_ids JSONB DEFAULT '[]'::jsonb,
    dispatch_manifest_id UUID,
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_b2b_orders_factory ON b2b_orders(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_b2b_orders_buyer ON b2b_orders(buyer_id);
CREATE INDEX IF NOT EXISTS idx_b2b_orders_status ON b2b_orders(status);

-- Supply Chain: Dispatch Manifests
CREATE TABLE IF NOT EXISTS dispatch_manifests (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    manifest_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    b2b_order_id UUID REFERENCES b2b_orders(id),
    master_carton_box_id VARCHAR(100),
    consignment_airway_bill_no VARCHAR(100),
    logistics_carrier_name VARCHAR(100),
    tracking_id VARCHAR(100),
    total_sarees INTEGER NOT NULL,
    gross_shipping_weight_kg DECIMAL(10,2),
    customs_declaration_json JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'GENERATED' CHECK (status IN ('GENERATED', 'DISPATCHED', 'IN_TRANSIT', 'DELIVERED', 'CANCELLED')),
    generated_by UUID REFERENCES users(id),
    dispatched_at TIMESTAMP WITH TIME ZONE,
    delivered_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_dispatch_manifests_factory ON dispatch_manifests(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_dispatch_manifests_b2b ON dispatch_manifests(b2b_order_id);
CREATE INDEX IF NOT EXISTS idx_dispatch_manifests_status ON dispatch_manifests(status);

-- Supply Chain: Inventory Bins
CREATE TABLE IF NOT EXISTS inventory_bins (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bin_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    warehouse_zone VARCHAR(50) NOT NULL,
    aisle VARCHAR(50),
    shelf VARCHAR(50),
    bin_type VARCHAR(50) DEFAULT 'STANDARD' CHECK (bin_type IN ('STANDARD', 'VAULT_ZARI', 'CLIMATE_CONTROLLED', 'QUARANTINE', 'DISPATCH')),
    item_category VARCHAR(50),
    current_quantity DECIMAL(12,2) DEFAULT 0,
    unit_of_measure VARCHAR(20) DEFAULT 'KG',
    reorder_point DECIMAL(10,2),
    max_capacity DECIMAL(10,2),
    vault_climate_status VARCHAR(50) DEFAULT 'OPTIMAL_CLIMATE_LOCKED',
    current_humidity_pct DECIMAL(5,2),
    current_temperature_celsius DECIMAL(5,2),
    last_reconciled_at TIMESTAMP WITH TIME ZONE,
    managed_by UUID REFERENCES users(id),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_inventory_bins_factory ON inventory_bins(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_inventory_bins_zone ON inventory_bins(warehouse_zone);
CREATE INDEX IF NOT EXISTS idx_inventory_bins_item ON inventory_bins(item_category);

-- Supply Chain: Inventory Movements
CREATE TABLE IF NOT EXISTS inventory_movements (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    movement_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    bin_id UUID REFERENCES inventory_bins(id),
    item_type VARCHAR(50) NOT NULL,
    lot_batch_id VARCHAR(100),
    movement_type VARCHAR(50) NOT NULL CHECK (movement_type IN ('INWARD_GRN', 'OUTWARD_ISSUE', 'TRANSFER', 'RECONCILIATION', 'ADJUSTMENT', 'RETURN')),
    quantity DECIMAL(12,2) NOT NULL,
    unit_of_measure VARCHAR(20) DEFAULT 'KG',
    reference_document_type VARCHAR(50),
    reference_document_id VARCHAR(100),
    from_bin_id UUID REFERENCES inventory_bins(id),
    to_bin_id UUID REFERENCES inventory_bins(id),
    authorized_by UUID REFERENCES users(id),
    received_by UUID REFERENCES users(id),
    movement_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_inventory_movements_bin ON inventory_movements(bin_id);
CREATE INDEX IF NOT EXISTS idx_inventory_movements_factory ON inventory_movements(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_inventory_movements_type ON inventory_movements(movement_type);
CREATE INDEX IF NOT EXISTS idx_inventory_movements_lot ON inventory_movements(lot_batch_id);

-- Supply Chain: Reconciliation Batches
CREATE TABLE IF NOT EXISTS reconciliation_batches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    batch_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    reconciliation_type VARCHAR(50) DEFAULT 'DAILY' CHECK (reconciliation_type IN ('DAILY', 'WEEKLY', 'MONTHLY', 'CYCLE_COUNT', 'ANNUAL')),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    total_items_count INTEGER,
    variance_items_count INTEGER,
    total_variance_value DECIMAL(12,2),
    status VARCHAR(50) DEFAULT 'IN_PROGRESS' CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'EXCEPTION', 'APPROVED')),
    conducted_by UUID REFERENCES users(id),
    approved_by UUID REFERENCES users(id),
    approved_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_reconciliation_batches_factory ON reconciliation_batches(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_reconciliation_batches_period ON reconciliation_batches(period_start, period_end);

-- IoT: Device Registry
CREATE TABLE IF NOT EXISTS iot_device_registry (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    device_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    device_type VARCHAR(50) NOT NULL CHECK (device_type IN ('LOOM_CONTROLLER', 'SCALE', 'CLIMATE_SENSOR', 'CAMERA', 'SPECTROPHOTOMETER', 'XRF_MACHINE', 'EDGE_AI_BOX')),
    loom_id VARCHAR(100),
    location_description TEXT,
    firmware_version VARCHAR(50),
    mqtt_topic VARCHAR(200),
    is_active BOOLEAN DEFAULT TRUE,
    last_heartbeat_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_iot_device_registry_factory ON iot_device_registry(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_iot_device_registry_type ON iot_device_registry(device_type);
CREATE INDEX IF NOT EXISTS idx_iot_device_registry_loom ON iot_device_registry(loom_id);

-- IoT: Loom Telemetry
CREATE TABLE IF NOT EXISTS loom_telemetry (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    telemetry_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    loom_id VARCHAR(100) NOT NULL,
    device_id VARCHAR(100) REFERENCES iot_device_registry(device_id),
    picks_per_minute INTEGER,
    warp_tension_cn DECIMAL(6,2),
    temperature_celsius DECIMAL(5,2),
    humidity_pct DECIMAL(5,2),
    vibration_mm_s DECIMAL(6,2),
    motor_current_a DECIMAL(6,2),
    ai_anomaly_score DECIMAL(5,4),
    ai_anomaly_classification VARCHAR(100),
    ai_auto_paused BOOLEAN DEFAULT FALSE,
    metadata JSONB DEFAULT '{}'::jsonb,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_loom_telemetry_loom ON loom_telemetry(loom_id);
CREATE INDEX IF NOT EXISTS idx_loom_telemetry_factory ON loom_telemetry(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_loom_telemetry_recorded ON loom_telemetry(recorded_at);

-- IoT: Maintenance Work Orders
CREATE TABLE IF NOT EXISTS maintenance_work_orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    work_order_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    loom_id VARCHAR(100),
    device_id VARCHAR(100) REFERENCES iot_device_registry(device_id),
    predicted_failure_component VARCHAR(100),
    predicted_failure_date DATE,
    priority VARCHAR(50) DEFAULT 'MEDIUM' CHECK (priority IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    work_order_type VARCHAR(50) DEFAULT 'PREVENTIVE' CHECK (work_order_type IN ('PREVENTIVE', 'CORRECTIVE', 'EMERGENCY')),
    description TEXT,
    assigned_to UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED')),
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_maintenance_work_orders_factory ON maintenance_work_orders(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_maintenance_work_orders_loom ON maintenance_work_orders(loom_id);
CREATE INDEX IF NOT EXISTS idx_maintenance_work_orders_status ON maintenance_work_orders(status);

-- Buy-Back: Guarantees
CREATE TABLE IF NOT EXISTS buyback_guarantees (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    guarantee_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    saree_id VARCHAR(100) NOT NULL,
    buyer_id VARCHAR(100),
    purchase_date DATE NOT NULL,
    guarantee_period_months INTEGER DEFAULT 12,
    risk_score DECIMAL(5,2),
    premium_amount DECIMAL(10,2),
    reserve_provision DECIMAL(10,2),
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'CLAIMED', 'EXPIRED', 'VOID')),
    claim_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_buyback_guarantees_saree ON buyback_guarantees(saree_id);
CREATE INDEX IF NOT EXISTS idx_buyback_guarantees_factory ON buyback_guarantees(factory_node_id);

-- Notifications
CREATE TABLE IF NOT EXISTS enterprise_notifications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    notification_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50),
    recipient_user_id UUID REFERENCES users(id),
    recipient_role_id VARCHAR(50),
    channel VARCHAR(50) DEFAULT 'IN_APP' CHECK (channel IN ('IN_APP', 'SMS', 'EMAIL', 'KAFKA', 'MQTT')),
    notification_type VARCHAR(50) NOT NULL,
    priority VARCHAR(50) DEFAULT 'NORMAL' CHECK (priority IN ('LOW', 'NORMAL', 'HIGH', 'CRITICAL')),
    title VARCHAR(500),
    message TEXT,
    reference_entity_type VARCHAR(50),
    reference_entity_id VARCHAR(100),
    is_read BOOLEAN DEFAULT FALSE,
    read_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_enterprise_notifications_recipient ON enterprise_notifications(recipient_user_id);
CREATE INDEX IF NOT EXISTS idx_enterprise_notifications_factory ON enterprise_notifications(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_enterprise_notifications_read ON enterprise_notifications(is_read);

-- Workflow Automation Rules
CREATE TABLE IF NOT EXISTS workflow_automation_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50),
    rule_name VARCHAR(200) NOT NULL,
    rule_type VARCHAR(50) NOT NULL CHECK (rule_type IN ('GUARDRAIL', 'ROUTING', 'NOTIFICATION', 'CERTIFICATION', 'RECONCILIATION', 'REQUISITION')),
    trigger_entity_type VARCHAR(50),
    trigger_condition JSONB DEFAULT '{}'::jsonb,
    action_type VARCHAR(50) NOT NULL,
    action_payload JSONB DEFAULT '{}'::jsonb,
    is_active BOOLEAN DEFAULT TRUE,
    priority INTEGER DEFAULT 100,
    created_by UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_workflow_automation_rules_factory ON workflow_automation_rules(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_workflow_automation_rules_type ON workflow_automation_rules(rule_type);

-- SKU Catalog (Enterprise)
CREATE TABLE IF NOT EXISTS sku_catalog (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sku_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    sku_name VARCHAR(500) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    hook_count INTEGER,
    silk_type VARCHAR(50),
    zari_type VARCHAR(50),
    design_code VARCHAR(100),
    base_price DECIMAL(10,2),
    market_segment VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    launched_at DATE,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_sku_catalog_factory ON sku_catalog(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_sku_catalog_category ON sku_catalog(category);

-- SKU Production Mapping
CREATE TABLE IF NOT EXISTS sku_production_mapping (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sku_id VARCHAR(100) NOT NULL REFERENCES sku_catalog(sku_id),
    factory_node_id VARCHAR(50) NOT NULL,
    design_id VARCHAR(100),
    warp_beam_spec JSONB DEFAULT '{}'::jsonb,
    material_requirements JSONB DEFAULT '[]'::jsonb,
    estimated_production_time_hours DECIMAL(10,2),
    estimated_cost_per_unit DECIMAL(10,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_sku_production_mapping_sku ON sku_production_mapping(sku_id);
CREATE INDEX IF NOT EXISTS idx_sku_production_mapping_factory ON sku_production_mapping(factory_node_id);

-- Design Files (Enterprise)
CREATE TABLE IF NOT EXISTS design_files (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    design_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    sku_id VARCHAR(100) REFERENCES sku_catalog(sku_id),
    design_name VARCHAR(500),
    hook_count INTEGER NOT NULL,
    pattern_hash VARCHAR(100) NOT NULL,
    design_json JSONB DEFAULT '{}'::jsonb,
    image_url VARCHAR(500),
    ai_generated BOOLEAN DEFAULT FALSE,
    ai_model_version VARCHAR(50),
    status VARCHAR(50) DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'APPROVED', 'DEPLOYED', 'ARCHIVED')),
    approved_by UUID REFERENCES users(id),
    approved_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_design_files_factory ON design_files(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_design_files_sku ON design_files(sku_id);
CREATE INDEX IF NOT EXISTS idx_design_files_status ON design_files(status);

-- Guilds
CREATE TABLE IF NOT EXISTS guilds (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    guild_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    guild_name VARCHAR(500) NOT NULL,
    guild_type VARCHAR(50) DEFAULT 'WEAVER_GUILD' CHECK (guild_type IN ('WEAVER_GUILD', 'DYER_GUILD', 'ZARI_GUILD', 'MIXED')),
    region VARCHAR(100),
    lead_weaver_id UUID REFERENCES users(id),
    member_count INTEGER DEFAULT 0,
    certification_level VARCHAR(50) DEFAULT 'BRONZE' CHECK (certification_level IN ('BRONZE', 'SILVER', 'GOLD', 'PLATINUM')),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_guilds_factory ON guilds(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_guilds_type ON guilds(guild_type);

-- Guild Payments
CREATE TABLE IF NOT EXISTS guild_payments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    payment_id VARCHAR(100) UNIQUE NOT NULL,
    guild_id UUID REFERENCES guilds(id),
    factory_node_id VARCHAR(50) NOT NULL,
    user_id UUID REFERENCES users(id),
    payment_type VARCHAR(50) DEFAULT 'PIECE_RATE' CHECK (payment_type IN ('PIECE_RATE', 'INCENTIVE', 'BONUS', 'ADVANCE', 'SETTLEMENT')),
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'INR',
    period_start DATE,
    period_end DATE,
    status VARCHAR(50) DEFAULT 'CALCULATED' CHECK (status IN ('CALCULATED', 'APPROVED', 'PAID', 'FAILED')),
    paid_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_guild_payments_guild ON guild_payments(guild_id);
CREATE INDEX IF NOT EXISTS idx_guild_payments_user ON guild_payments(user_id);

-- Edge Controllers
CREATE TABLE IF NOT EXISTS edge_controllers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    controller_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    loom_id VARCHAR(100),
    device_type VARCHAR(50) DEFAULT 'ESP32_S3',
    firmware_version VARCHAR(50),
    mqtt_broker_url VARCHAR(500),
    mqtt_topic_prefix VARCHAR(200),
    is_online BOOLEAN DEFAULT FALSE,
    last_heartbeat_at TIMESTAMP WITH TIME ZONE,
    deployed_ai_models JSONB DEFAULT '[]'::jsonb,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_edge_controllers_factory ON edge_controllers(factory_node_id);
CREATE INDEX IF NOT EXISTS idx_edge_controllers_loom ON edge_controllers(loom_id);

-- i18n Keys
CREATE TABLE IF NOT EXISTS i18n_keys (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key_id VARCHAR(100) UNIQUE NOT NULL,
    module VARCHAR(50) NOT NULL,
    context TEXT,
    default_text TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_i18n_keys_module ON i18n_keys(module);

-- i18n Translations
CREATE TABLE IF NOT EXISTS i18n_translations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key_id UUID REFERENCES i18n_keys(id) ON DELETE CASCADE,
    language_code VARCHAR(10) NOT NULL CHECK (language_code IN ('en', 'te', 'ta', 'kn', 'hi', 'bn')),
    translated_text TEXT NOT NULL,
    translation_quality_score DECIMAL(5,2),
    ai_translated BOOLEAN DEFAULT TRUE,
    human_reviewed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_i18n_translations_key ON i18n_translations(key_id);
CREATE INDEX IF NOT EXISTS idx_i18n_translations_lang ON i18n_translations(language_code);

-- Voice Audio Cache
CREATE TABLE IF NOT EXISTS voice_audio_cache (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    audio_id VARCHAR(100) UNIQUE NOT NULL,
    text_hash VARCHAR(100) NOT NULL,
    language_code VARCHAR(10) NOT NULL,
    voice_model VARCHAR(100) DEFAULT 'NEURAL_TTS_V1',
    audio_url VARCHAR(500) NOT NULL,
    duration_ms INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_voice_audio_cache_hash ON voice_audio_cache(text_hash, language_code);

-- Kafka Event Log
CREATE TABLE IF NOT EXISTS kafka_event_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    topic VARCHAR(200) NOT NULL,
    partition_key VARCHAR(100),
    event_type VARCHAR(50) NOT NULL,
    entity_type VARCHAR(50),
    entity_id VARCHAR(100),
    payload JSONB DEFAULT '{}'::jsonb,
    trace_id VARCHAR(100),
    consumed BOOLEAN DEFAULT FALSE,
    consumed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_kafka_event_log_topic ON kafka_event_log(topic);
CREATE INDEX IF NOT EXISTS idx_kafka_event_log_entity ON kafka_event_log(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_kafka_event_log_consumed ON kafka_event_log(consumed);

-- Back-Trace Cache
CREATE TABLE IF NOT EXISTS back_trace_cache (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    saree_serial_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    lineage JSONB DEFAULT '{}'::jsonb,
    cache_hit BOOLEAN DEFAULT TRUE,
    computed_at_ms INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_back_trace_cache_saree ON back_trace_cache(saree_serial_id);
CREATE INDEX IF NOT EXISTS idx_back_trace_cache_factory ON back_trace_cache(factory_node_id);

-- ------------------------------------------------------------
-- 2. WIDEN EXISTING ROLE TABLES WITH ENTERPRISE COLUMNS
-- ------------------------------------------------------------

-- Add enterprise columns to assistant_weaver_job_logs
ALTER TABLE assistant_weaver_job_logs
ADD COLUMN IF NOT EXISTS ai_weaving_defect_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_anomaly_classification VARCHAR(100),
ADD COLUMN IF NOT EXISTS ai_auto_paused BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS finance_payroll_run_id UUID,
ADD COLUMN IF NOT EXISTS guild_id UUID,
ADD COLUMN IF NOT EXISTS skill_progression_score DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS environmental_temperature_celsius DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS environmental_humidity_pct DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to master_weaver_jobs
ALTER TABLE master_weaver_jobs
ADD COLUMN IF NOT EXISTS ai_yield_prediction DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS ai_defect_risk_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_skill_match_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS finance_payroll_run_id UUID,
ADD COLUMN IF NOT EXISTS guild_id UUID,
ADD COLUMN IF NOT EXISTS environmental_temperature_celsius DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS environmental_humidity_pct DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to quality_inspector_logs
ALTER TABLE quality_inspector_logs
ADD COLUMN IF NOT EXISTS ai_fabric_defect_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_grade_prediction VARCHAR(50),
ADD COLUMN IF NOT EXISTS ai_confidence_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS b2b_order_id UUID REFERENCES b2b_orders(id),
ADD COLUMN IF NOT EXISTS finance_payroll_run_id UUID,
ADD COLUMN IF NOT EXISTS guild_id UUID,
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to silk_mark_officer_logs
ALTER TABLE silk_mark_officer_logs
ADD COLUMN IF NOT EXISTS ai_purity_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_counterfeit_detection_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS finance_payout_id UUID REFERENCES supplier_payouts(id),
ADD COLUMN IF NOT EXISTS guild_id UUID,
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to log_finishing_transit_specialist_logs
ALTER TABLE log_finishing_transit_specialist_logs
ADD COLUMN IF NOT EXISTS ai_packaging_optimization_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_carrier_selection_recommendation VARCHAR(100),
ADD COLUMN IF NOT EXISTS ai_insurance_premium_estimate DECIMAL(10,2),
ADD COLUMN IF NOT EXISTS b2b_order_id UUID REFERENCES b2b_orders(id),
ADD COLUMN IF NOT EXISTS dispatch_manifest_id UUID REFERENCES dispatch_manifests(id),
ADD COLUMN IF NOT EXISTS finance_invoice_id UUID,
ADD COLUMN IF NOT EXISTS guild_id UUID,
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to store_inventory_manager_logs
ALTER TABLE store_inventory_manager_logs
ADD COLUMN IF NOT EXISTS ai_reorder_point_optimized DECIMAL(10,2),
ADD COLUMN IF NOT EXISTS ai_demand_forecast_30d DECIMAL(10,2),
ADD COLUMN IF NOT EXISTS ai_shrinkage_anomaly_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_shelf_life_remaining_days INTEGER,
ADD COLUMN IF NOT EXISTS finance_valuation_id UUID,
ADD COLUMN IF NOT EXISTS procurement_requisition_id UUID REFERENCES procurement_requisitions(id),
ADD COLUMN IF NOT EXISTS guild_id UUID,
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to master_weaver_jobs (AI scheduler fields)
ALTER TABLE master_weaver_jobs
ADD COLUMN IF NOT EXISTS ai_scheduler_assigned BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS ai_scheduler_confidence DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_energy_optimization_flag BOOLEAN DEFAULT FALSE;

-- Add enterprise columns to sup_loom_floor_supervisor_logs
ALTER TABLE sup_loom_floor_supervisor_logs
ADD COLUMN IF NOT EXISTS ai_floor_productivity_score DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS ai_predictive_downtime_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS ai_root_cause_suggestion VARCHAR(200),
ADD COLUMN IF NOT EXISTS finance_payroll_run_id UUID,
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100);

-- Add enterprise columns to warp_beam_production_logs
ALTER TABLE warp_beam_production_logs
ADD COLUMN IF NOT EXISTS ai_yield_prediction INTEGER,
ADD COLUMN IF NOT EXISTS ai_defect_risk_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_optimal_creel_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to skein_dye_jobs
ALTER TABLE skein_dye_jobs
ADD COLUMN IF NOT EXISTS ai_color_consistency_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_recipe_optimization_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS ai_mass_balance_variance DECIMAL(10,2),
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to zari_inspection_records
ALTER TABLE zari_inspection_records
ADD COLUMN IF NOT EXISTS ai_purity_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_counterfeit_detection_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS ai_xrf_confidence DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to throwster_production_records
ALTER TABLE throwster_production_records
ADD COLUMN IF NOT EXISTS ai_process_optimization_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS ai_quality_prediction_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to degumming_records
ALTER TABLE degumming_records
ADD COLUMN IF NOT EXISTS ai_process_optimization_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS ai_quality_prediction_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to pirn_winding_jobs
ALTER TABLE pirn_winding_jobs
ADD COLUMN IF NOT EXISTS ai_density_anomaly_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_hook_compatibility_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to warp_joining_jobs
ALTER TABLE warp_joining_jobs
ADD COLUMN IF NOT EXISTS ai_knot_quality_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_tension_prediction_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to harness_setup_logs
ALTER TABLE harness_setup_logs
ADD COLUMN IF NOT EXISTS ai_shed_quality_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_tension_prediction_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS maintenance_work_order_id UUID,
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to petni_master_jobs
ALTER TABLE petni_master_jobs
ADD COLUMN IF NOT EXISTS ai_contrast_inventory_depletion_prediction DECIMAL(10,2),
ADD COLUMN IF NOT EXISTS ai_alignment_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to qa_dyeing_inspector_logs
ALTER TABLE qa_dyeing_inspector_logs
ADD COLUMN IF NOT EXISTS ai_delta_e_prediction DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_fastness_prediction VARCHAR(50),
ADD COLUMN IF NOT EXISTS ai_b2b_match_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS b2b_order_id UUID REFERENCES b2b_orders(id),
ADD COLUMN IF NOT EXISTS guild_id UUID,
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to master_colorist_recipes
ALTER TABLE master_colorist_recipes
ADD COLUMN IF NOT EXISTS ai_recipe_optimization_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS ai_delta_e_forecast DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_fastness_forecast VARCHAR(50),
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS back_trace_ref VARCHAR(100);

-- Add enterprise columns to design_masters
ALTER TABLE design_masters
ADD COLUMN IF NOT EXISTS ai_design_suggestion_id UUID,
ADD COLUMN IF NOT EXISTS ai_defect_risk_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_hook_compatibility_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS sku_id UUID,
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100);

-- Add enterprise columns to programming_jobs
ALTER TABLE programming_jobs
ADD COLUMN IF NOT EXISTS ai_pattern_validation_flag BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS ai_defect_prediction_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS kafka_event_id VARCHAR(100);

-- Add enterprise columns to zari_lot_batches
ALTER TABLE zari_lot_batches
ADD COLUMN IF NOT EXISTS ai_purity_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_counterfeit_risk_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_refinery_recommendation JSONB DEFAULT '{}'::jsonb;

-- Add enterprise columns to inward_gate_entries
ALTER TABLE inward_gate_entries
ADD COLUMN IF NOT EXISTS ai_supplier_risk_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_quality_prediction_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS finance_payout_id UUID REFERENCES supplier_payouts(id);

-- Add enterprise columns to quality_intake_records
ALTER TABLE quality_intake_records
ADD COLUMN IF NOT EXISTS ai_grade_prediction VARCHAR(50),
ADD COLUMN IF NOT EXISTS ai_confidence_score DECIMAL(5,4),
ADD COLUMN IF NOT EXISTS ai_defect_detection_flag BOOLEAN DEFAULT FALSE;

-- ------------------------------------------------------------
-- 3. TRIGGERS: Auto-update timestamps for new tables
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_enterprise_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_hr_weaver_profiles_updated_at ON hr_weaver_profiles;
CREATE TRIGGER trigger_update_hr_weaver_profiles_updated_at
    BEFORE UPDATE ON hr_weaver_profiles
    FOR EACH ROW EXECUTE FUNCTION update_enterprise_timestamp();

DROP TRIGGER IF EXISTS trigger_update_workflow_automation_rules_updated_at ON workflow_automation_rules;
CREATE TRIGGER trigger_update_workflow_automation_rules_updated_at
    BEFORE UPDATE ON workflow_automation_rules
    FOR EACH ROW EXECUTE FUNCTION update_enterprise_timestamp();

DROP TRIGGER IF EXISTS trigger_update_b2b_orders_updated_at ON b2b_orders;
CREATE TRIGGER trigger_update_b2b_orders_updated_at
    BEFORE UPDATE ON b2b_orders
    FOR EACH ROW EXECUTE FUNCTION update_enterprise_timestamp();

-- ------------------------------------------------------------
-- 4. ROLE PERMISSIONS: Enterprise-wide expansion
-- ------------------------------------------------------------

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","weaving:read","weaving:supervise","weaving:inspect","assistant:read","master-weaver:read","petni-master:read","warp-joiner:read","harness-setter:read","sales:forecast:read","environment:read","environment:write","oee:read","oee:write","quality:read","quality:write","dyeing:read","dyeing:write","compliance:read","compliance:write","silk-mark:read","silk-mark:write","finishing:read","finishing:write","dispatch:read","dispatch:write","inventory:read","inventory:write","inventory:audit","inventory:reconcile","chemical:manage","sales:forecast:read","b2b:order:read","b2b:order:write","finance:read","finance:write","finance:payout","hr:read","hr:write","hr:payroll","procurement:read","procurement:write","procurement:approve","ai:inference:read","ai:model:read","iot:read","iot:write","iot:manage","guild:read","guild:write","localization:read","localization:write","sku:read","sku:write"]'
WHERE role_id IN ('ROLE-SUP-LOOM-FLOOR-SUPERVISOR');

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","weaving:read","weaving:supervise","weaving:inspect","assistant:read","master-weaver:read","petni-master:read","warp-joiner:read","harness-setter:read","sales:forecast:read","environment:read","environment:write","oee:read","oee:write","quality:read","quality:write","dyeing:read","dyeing:write","compliance:read","compliance:write","silk-mark:read","silk-mark:write","finishing:read","finishing:write","dispatch:read","dispatch:write","inventory:read","inventory:write","inventory:audit","inventory:reconcile","chemical:manage","sales:forecast:read","b2b:order:read","b2b:order:write","finance:read","finance:write","finance:payout","hr:read","hr:write","hr:payroll","procurement:read","procurement:write","procurement:approve","ai:inference:read","ai:model:read","iot:read","iot:write","iot:manage","guild:read","guild:write","localization:read","localization:write","sku:read","sku:write"]'
WHERE role_id IN ('ROLE-MASTER-WEAVER');

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","weaving:read","weaving:supervise","weaving:inspect","assistant:read","master-weaver:read","petni-master:read","warp-joiner:read","harness-setter:read","sales:forecast:read","environment:read","environment:write","oee:read","oee:write","quality:read","quality:write","dyeing:read","dyeing:write","compliance:read","compliance:write","silk-mark:read","silk-mark:write","finishing:read","finishing:write","dispatch:read","dispatch:write","inventory:read","inventory:write","inventory:audit","inventory:reconcile","chemical:manage","sales:forecast:read","b2b:order:read","b2b:order:write","finance:read","finance:write","finance:payout","hr:read","hr:write","hr:payroll","procurement:read","procurement:write","procurement:approve","ai:inference:read","ai:model:read","iot:read","iot:write","iot:manage","guild:read","guild:write","localization:read","localization:write","sku:read","sku:write"]'
WHERE role_id IN ('ROLE-STORE-INVENTORY-MANAGER');

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","weaving:read","weaving:supervise","weaving:inspect","assistant:read","master-weaver:read","petni-master:read","warp-joiner:read","harness-setter:read","sales:forecast:read","environment:read","environment:write","oee:read","oee:write","quality:read","quality:write","dyeing:read","dyeing:write","compliance:read","compliance:write","silk-mark:read","silk-mark:write","finishing:read","finishing:write","dispatch:read","dispatch:write","inventory:read","inventory:write","inventory:audit","inventory:reconcile","chemical:manage","sales:forecast:read","b2b:order:read","b2b:order:write","finance:read","finance:write","finance:payout","hr:read","hr:write","hr:payroll","procurement:read","procurement:write","procurement:approve","ai:inference:read","ai:model:read","iot:read","iot:write","iot:manage","guild:read","guild:write","localization:read","localization:write","sku:read","sku:write"]'
WHERE role_id IN ('ROLE-SILK-MARK-OFFICER');

UPDATE roles
SET permitted_operations = '["scanner:input","scanner:output","lot:read","lot:update","design:read","weaving:read","weaving:supervise","weaving:inspect","assistant:read","master-weaver:read","petni-master:read","warp-joiner:read","harness-setter:read","sales:forecast:read","environment:read","environment:write","oee:read","oee:write","quality:read","quality:write","dyeing:read","dyeing:write","compliance:read","compliance:write","silk-mark:read","silk-mark:write","finishing:read","finishing:write","dispatch:read","dispatch:write","inventory:read","inventory:write","inventory:audit","inventory:reconcile","chemical:manage","sales:forecast:read","b2b:order:read","b2b:order:write","finance:read","finance:write","finance:payout","hr:read","hr:write","hr:payroll","procurement:read","procurement:write","procurement:approve","ai:inference:read","ai:model:read","iot:read","iot:write","iot:manage","guild:read","guild:write","localization:read","localization:write","sku:read","sku:write"]'
WHERE role_id IN ('ROLE-QUALITY-INSPECTOR', 'ROLE-QA-DYEING-INSPECTOR', 'ROLE-LOG-FINISHING', 'ROLE-SKEIN-DYE-MASTER', 'ROLE-MASTER-COLORIST', 'ROLE-ZARI-INSPECTOR', 'ROLE-SILK-DEGUMMING-MASTER', 'ROLE-SILK-GRADER', 'ROLE-THROWSTER-TWISTER', 'ROLE-BOBBIN-WINDER', 'ROLE-PIRN-WINDERS', 'ROLE-WARP-BEAM-PREPARATION', 'ROLE-WARP-JOINER', 'ROLE-LOOM-HARNESS-SETTER', 'ROLE-PETNI-MASTER', 'ROLE-CARD-PUNCHER', 'ROLE-GRAPH-DRAFTER', 'ROLE-DESIGN-GENERATOR', 'ROLE-SKU-MANAGER', 'ROLE-BUY-BACK-MANAGER', 'ROLE-GUILD-MANAGER', 'ROLE-IOT-DEVICE-MANAGER', 'ROLE-LOCALIZATION-MANAGER', 'ROLE-FILATURE-SUPPLIER', 'ROLE-ASSISTANT-WEAVER');
