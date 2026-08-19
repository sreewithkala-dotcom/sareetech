-- SKU Product Catalog Table
CREATE TABLE IF NOT EXISTS sku_catalog (
    id SERIAL PRIMARY KEY,
    sku_ref_id VARCHAR(50) UNIQUE NOT NULL,
    geographic_hub VARCHAR(100) NOT NULL,
    weave_category VARCHAR(100) NOT NULL,
    jacquard_capacity VARCHAR(50) NOT NULL,
    zari_configuration VARCHAR(100) NOT NULL,
    warp_denier VARCHAR(20) NOT NULL,
    weft_denier VARCHAR(20) NOT NULL,
    zari_wire_denier VARCHAR(20) NOT NULL,
    warp_net_weight_g DECIMAL(10,2) NOT NULL,
    weft_net_weight_g DECIMAL(10,2) NOT NULL,
    zari_net_weight_g DECIMAL(10,2) NOT NULL,
    bobbin_waste_weight_g DECIMAL(10,2) NOT NULL,
    total_saree_weight_g DECIMAL(10,2) NOT NULL,
    yarn_raw_cost_inr DECIMAL(12,2) NOT NULL,
    zari_raw_cost_inr DECIMAL(12,2) NOT NULL,
    labor_surcharge_inr DECIMAL(12,2) NOT NULL,
    total_mfg_cost_inr DECIMAL(12,2) NOT NULL,
    mrp_inr DECIMAL(12,2) NOT NULL,
    selling_price_inr DECIMAL(12,2) NOT NULL,
    min_floor_price_inr DECIMAL(12,2) NOT NULL,
    weight_category_profile VARCHAR(100) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_sku_ref_id ON sku_catalog(sku_ref_id);
CREATE INDEX idx_geographic_hub ON sku_catalog(geographic_hub);
CREATE INDEX idx_weave_category ON sku_catalog(weave_category);
CREATE INDEX idx_jacquard_capacity ON sku_catalog(jacquard_capacity);
CREATE INDEX idx_weight_category ON sku_catalog(weight_category_profile);

CREATE TABLE IF NOT EXISTS sku_production_mapping (
    id SERIAL PRIMARY KEY,
    sku_id INTEGER REFERENCES sku_catalog(id),
    lot_id INTEGER REFERENCES production_lots(id),
    design_id INTEGER REFERENCES design_files(id),
    loom_id INTEGER REFERENCES loom_assignments(id),
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(50) DEFAULT 'ASSIGNED'
);

CREATE INDEX idx_sku_production_sku ON sku_production_mapping(sku_id);
CREATE INDEX idx_sku_production_lot ON sku_production_mapping(lot_id);
