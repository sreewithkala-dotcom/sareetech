-- ============================================================
-- Migration: 005_silk_saree_extensions.sql
-- Extended schema for silk saree manufacturing with guilds,
-- supply chain, buy-back guarantees, IoT, design files, and localization
-- ============================================================

-- ------------------------------------------------------------
-- 1. FACTORY NODE EXTENSIONS (already exists, adding new fields)
-- ------------------------------------------------------------

-- loom_type and dyeing_model already in config JSONB

-- ------------------------------------------------------------
-- 2. USER EXTENSIONS (guild, language, weaver ID)
-- ------------------------------------------------------------

ALTER TABLE users ADD COLUMN IF NOT EXISTS guild_id UUID;
ALTER TABLE users ADD COLUMN IF NOT EXISTS weaver_id VARCHAR(100);
ALTER TABLE users ADD COLUMN IF NOT EXISTS language_preferences JSONB DEFAULT '["en"]'::jsonb;
ALTER TABLE users ADD COLUMN IF NOT EXISTS dialect VARCHAR(50);
ALTER TABLE users ADD COLUMN IF NOT EXISTS performance_score DECIMAL(3,2) DEFAULT 0.0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS skill_level VARCHAR(50);
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_karigar BOOLEAN DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_users_guild ON users(guild_id);
CREATE INDEX IF NOT EXISTS idx_users_weaver ON users(weaver_id);

-- ------------------------------------------------------------
-- 3. GUILDS & ASSOCIATIONS
-- ------------------------------------------------------------

CREATE TABLE guilds (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    guild_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL CHECK (type IN (
        'WARP_VENDORS', 'ZARI_VENDORS', 'DYEING_WORKERS', 
        'WEAVING_GUILD', 'POST_MAKING', 'DESIGN_GUILD', 'OTHER'
    )),
    region VARCHAR(50) NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    config JSONB DEFAULT '{}'::jsonb,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE guild_members (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    guild_id UUID NOT NULL REFERENCES guilds(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_type VARCHAR(50) NOT NULL CHECK (role_type IN (
        'DEPENDENT_WEAVER', 'INDEPENDENT_WEAVER', 'SUPPLIER', 
        'WORKER', 'SUPERVISOR', 'ADMIN'
    )),
    skill_level VARCHAR(50),
    performance_score DECIMAL(3,2) DEFAULT 0.0,
    joined_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE(guild_id, user_id)
);

CREATE TABLE guild_payments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    guild_id UUID NOT NULL REFERENCES guilds(id),
    member_id UUID NOT NULL REFERENCES users(id),
    payment_type VARCHAR(50) NOT NULL CHECK (payment_type IN (
        'PIECE_RATE', 'BONUS', 'PENALTY', 'ESCROW', 'ADVANCE'
    )),
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'INR',
    status VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN (
        'PENDING', 'PROCESSED', 'PAID', 'FAILED', 'REVERSED'
    )),
    payment_data JSONB DEFAULT '{}'::jsonb,
    processed_by UUID REFERENCES users(id),
    processed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE guild_incentives (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    guild_id UUID NOT NULL REFERENCES guilds(id),
    rule_name VARCHAR(255) NOT NULL,
    rule_type VARCHAR(50) NOT NULL CHECK (rule_type IN (
        'ZERO_BREAKAGE_PREMIUM', 'SPEED_BONUS', 'SHADE_MATCH_BONUS', 
        'STAIN_PENALTY', 'QUALITY_BONUS'
    )),
    condition_formula JSONB NOT NULL,
    reward_amount DECIMAL(10,2),
    penalty_amount DECIMAL(10,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_guilds_type ON guilds(type);
CREATE INDEX idx_guilds_region ON guilds(region);
CREATE INDEX idx_guild_members_guild ON guild_members(guild_id);
CREATE INDEX idx_guild_members_user ON guild_members(user_id);
CREATE INDEX idx_guild_payments_guild ON guild_payments(guild_id);
CREATE INDEX idx_guild_payments_member ON guild_payments(member_id);

-- ------------------------------------------------------------
-- 4. SUPPLY CHAIN TABLES
-- ------------------------------------------------------------

CREATE TABLE yarn_batches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    batch_code VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    supplier_id UUID REFERENCES users(id),
    silk_type VARCHAR(50) NOT NULL CHECK (silk_type IN (
        'MULBERRY', 'TUSSAR', 'ERI', 'PAT', 'BANARASI', 'KANCHIPURAM', 'PAITHANI'
    )),
    denier DECIMAL(6,2),
    purity_score DECIMAL(3,2),
    protein_purity DECIMAL(3,2),
    xrf_verified BOOLEAN DEFAULT FALSE,
    qr_tag_id VARCHAR(100) UNIQUE,
    lab_test_data JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(50) DEFAULT 'PENDING' CHECK (status IN (
        'PENDING', 'CERTIFIED', 'QUARANTINED', 'CONSUMED', 'REJECTED'
    )),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    certified_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE zari_batches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    batch_code VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    supplier_id UUID REFERENCES users(id),
    zari_type VARCHAR(50) NOT NULL CHECK (zari_type IN (
        'PURE_ZARI', 'TESTED_ZARI', 'HALF_FINE', 'IMITATION'
    )),
    alloy_ratio JSONB DEFAULT '{}'::jsonb, -- {"gold": 0.05, "silver": 0.95}
    purity_cert JSONB DEFAULT '{}'::jsonb,
    xrf_scan_data JSONB DEFAULT '{}'::jsonb,
    weight_g DECIMAL(10,3),
    escrow_status VARCHAR(20) DEFAULT 'PENDING' CHECK (escrow_status IN (
        'PENDING', 'HELD', 'RELEASED', 'FAILED'
    )),
    qr_tag_id VARCHAR(100) UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    certified_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE dye_vats (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vat_code VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    batch_id UUID, -- references a conceptual batch, linked via metadata
    color_formula JSONB NOT NULL,
    ph_level DECIMAL(4,2),
    temperature_curve JSONB DEFAULT '[]'::jsonb,
    delta_e DECIMAL(4,2),
    metamerism_flag BOOLEAN DEFAULT FALSE,
    water_usage_liters DECIMAL(10,2),
    chemist_id UUID REFERENCES users(id),
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'COMPLETED', 'FAILED', 'MAINTENANCE'
    )),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE loom_assignments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    loom_id VARCHAR(100) NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    weaver_id UUID REFERENCES users(id),
    warp_batch_id UUID REFERENCES yarn_batches(id),
    zari_batch_id UUID REFERENCES zari_batches(id),
    design_file_id UUID,
    status VARCHAR(50) DEFAULT 'ASSIGNED' CHECK (status IN (
        'ASSIGNED', 'ACTIVE', 'PAUSED', 'COMPLETED', 'FLAGGED'
    )),
    assigned_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE finished_sarees (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lot_id UUID REFERENCES production_lots(id),
    factory_node_id VARCHAR(50) NOT NULL,
    nfc_id VARCHAR(100) UNIQUE NOT NULL,
    grade VARCHAR(10) CHECK (grade IN ('A', 'B', 'SECONDS')),
    buyback_value DECIMAL(10,2),
    certificate_hash VARCHAR(255),
    qr_tag_id VARCHAR(100) UNIQUE,
    status VARCHAR(50) DEFAULT 'CERTIFIED' CHECK (status IN (
        'CERTIFIED', 'SOLD', 'RETURNED', 'BUYBACK_PAID'
    )),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE buyback_guarantees (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    saree_id UUID NOT NULL REFERENCES finished_sarees(id),
    customer_id UUID,
    request_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    scan_data JSONB DEFAULT '{}'::jsonb,
    ai_valuation JSONB DEFAULT '{}'::jsonb,
    payout_amount DECIMAL(10,2),
    payout_status VARCHAR(20) DEFAULT 'PENDING' CHECK (payout_status IN (
        'PENDING', 'APPROVED', 'PAID', 'REJECTED'
    )),
    depreciation_breakdown JSONB DEFAULT '{}'::jsonb,
    processed_by UUID REFERENCES users(id),
    processed_at TIMESTAMP WITH TIME ZONE
);

-- ------------------------------------------------------------
-- 5. DESIGN FILES & GENERATION
-- ------------------------------------------------------------

CREATE TABLE design_files (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pattern_id VARCHAR(100) UNIQUE NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    file_hash VARCHAR(255) NOT NULL,
    file_size_gb DECIMAL(6,2),
    hook_count INTEGER NOT NULL,
    segment_map JSONB DEFAULT '{}'::jsonb, -- {"border_top": [1-400], "body": [401-1800], ...}
    design_json JSONB DEFAULT '{}'::jsonb,
    file_url TEXT,
    loom_profile_id VARCHAR(100),
    status VARCHAR(50) DEFAULT 'GENERATED' CHECK (status IN (
        'GENERATED', 'APPROVED', 'PUSHED', 'WOVEN', 'REJECTED'
    )),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    pushed_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE design_generations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pattern_id VARCHAR(100) NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    generation_type VARCHAR(50) NOT NULL CHECK (generation_type IN (
        'GAN', 'HUMAN_UPLOAD', 'MODIFIED'
    )),
    parent_pattern_id VARCHAR(100),
    viability_score DECIMAL(3,2),
    cost_margin DECIMAL(5,2),
    sales_velocity_score DECIMAL(3,2),
    status VARCHAR(50) DEFAULT 'PENDING' CHECK (status IN (
        'PENDING', 'APPROVED', 'REJECTED', 'WOVEN'
    )),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE design_sales_feedback (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pattern_id VARCHAR(100) NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    sales_units INTEGER DEFAULT 0,
    revenue DECIMAL(12,2) DEFAULT 0.0,
    return_rate DECIMAL(3,2) DEFAULT 0.0,
    feedback_score DECIMAL(3,2),
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- 6. IOT & EDGE CONTROLLERS
-- ------------------------------------------------------------

CREATE TABLE edge_controllers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    device_id VARCHAR(100) UNIQUE NOT NULL,
    loom_id VARCHAR(100) NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    firmware_version VARCHAR(50),
    storage_type VARCHAR(50) DEFAULT 'eMMC',
    storage_capacity_gb DECIMAL(6,2),
    mesh_node_id VARCHAR(100),
    status VARCHAR(50) DEFAULT 'ONLINE' CHECK (status IN (
        'ONLINE', 'OFFLINE', 'MAINTENANCE', 'RETIRED'
    )),
    last_seen TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE loom_telemetry (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    loom_id VARCHAR(100) NOT NULL,
    device_id VARCHAR(100) NOT NULL,
    factory_node_id VARCHAR(50) NOT NULL,
    current_picks INTEGER DEFAULT 0,
    target_picks INTEGER DEFAULT 0,
    pick_rate_per_minute DECIMAL(6,2),
    faults_detected INTEGER DEFAULT 0,
    efficiency_score DECIMAL(3,2),
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'FLAGGED_FOR_INSPECTION', 'COMPLETED', 'PAUSED'
    )),
    telemetry_data JSONB DEFAULT '{}'::jsonb,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE design_injections (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    loom_id VARCHAR(100) NOT NULL,
    device_id VARCHAR(100) NOT NULL,
    pattern_id VARCHAR(100) NOT NULL,
    file_hash VARCHAR(255) NOT NULL,
    injection_status VARCHAR(50) DEFAULT 'QUEUED' CHECK (injection_status IN (
        'QUEUED', 'STREAMING', 'CONFIRMED', 'FAILED'
    )),
    transfer_confirmed BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    confirmed_at TIMESTAMP WITH TIME ZONE
);

-- ------------------------------------------------------------
-- 7. LOCALIZATION (i18n & TTS)
-- ------------------------------------------------------------

CREATE TABLE i18n_keys (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key_code VARCHAR(100) UNIQUE NOT NULL,
    category VARCHAR(50) NOT NULL CHECK (category IN (
        'ALERT', 'BUTTON', 'STATUS', 'ERROR', 'VOICE_GUIDANCE', 'DASHBOARD'
    )),
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE i18n_translations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key_id UUID NOT NULL REFERENCES i18n_keys(id),
    language_code VARCHAR(10) NOT NULL CHECK (language_code IN (
        'en', 'te', 'ta', 'kn', 'hi', 'bn'
    )),
    translated_text TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(key_id, language_code)
);

CREATE TABLE voice_audio_cache (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key_code VARCHAR(100) NOT NULL,
    language_code VARCHAR(10) NOT NULL,
    audio_url TEXT NOT NULL,
    duration_ms INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(key_code, language_code)
);

-- ------------------------------------------------------------
-- 8. CERTIFICATE & BLOCKCHAIN LEDGER
-- ------------------------------------------------------------

CREATE TABLE certificate_ledger (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    nfc_id VARCHAR(100) UNIQUE NOT NULL,
    saree_id UUID REFERENCES finished_sarees(id),
    genesis_hash VARCHAR(255) NOT NULL,
    silk_proof JSONB DEFAULT '{}'::jsonb,
    zari_proof JSONB DEFAULT '{}'::jsonb,
    dye_proof JSONB DEFAULT '{}'::jsonb,
    weave_proof JSONB DEFAULT '{}'::jsonb,
    previous_hash VARCHAR(255),
    block_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_genesis BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE nfc_registry (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    nfc_id VARCHAR(100) UNIQUE NOT NULL,
    saree_id UUID REFERENCES finished_sarees(id),
    cryptographic_key VARCHAR(255) NOT NULL,
    embedded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'ACTIVE' CHECK (status IN (
        'ACTIVE', 'DEACTIVATED', 'REPLACED'
    ))
);

-- ------------------------------------------------------------
-- 9. MATERIALS & INVENTORY (already exists, extending)
-- ------------------------------------------------------------

-- Add new material categories specific to silk saree manufacturing
ALTER TABLE materials ADD COLUMN IF NOT EXISTS material_type VARCHAR(50) CHECK (material_type IN (
    'SILK_YARN', 'ZARI_THREAD', 'DYE_CHEMICAL', 'LOOM_PART', 'NFC_TAG', 'OTHER'
));

-- ------------------------------------------------------------
-- 10. TRIGGERS FOR NEW TABLES
-- ------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_guild_members_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_guilds_updated_at BEFORE UPDATE ON guilds
    FOR EACH ROW EXECUTE FUNCTION update_guild_members_updated_at();

CREATE TRIGGER update_i18n_translations_updated_at BEFORE UPDATE ON i18n_translations
    FOR EACH ROW EXECUTE FUNCTION update_guild_members_updated_at();

-- Auto-calculate performance score on guild_members update
CREATE OR REPLACE FUNCTION calculate_performance_score()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.performance_score IS NULL OR NEW.performance_score = 0 THEN
        -- Calculate from guild_payments and quality metrics
        NEW.performance_score := COALESCE((
            SELECT AVG(CASE 
                WHEN payment_type IN ('BONUS', 'PIECE_RATE') THEN 0.8 
                WHEN payment_type = 'PENALTY' THEN 0.3 
                ELSE 0.5 
            END)
            FROM guild_payments
            WHERE member_id = NEW.user_id
              AND created_at > NOW() - INTERVAL '30 days'
        ), 0.5);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER calc_performance_score BEFORE INSERT OR UPDATE ON guild_members
    FOR EACH ROW EXECUTE FUNCTION calculate_performance_score();

-- ------------------------------------------------------------
-- 11. SAMPLE DATA: GUILDS
-- ------------------------------------------------------------

INSERT INTO guilds (guild_id, name, type, region, factory_node_id, config) VALUES
('GUILD-WARP-BLR', 'Bangalore Warp Vendors Association', 'WARP_VENDORS', 'ap-south-1', 'FACT-BLR-01',
 '{"payment_rules": {"base_rate_per_kg": 450, "zero_breakage_premium": 0.15}, "quality_thresholds": {"tensile_strength": 3.5, "moisture_retention": 0.08}}'),
('GUILD-ZARI-BLR', 'Bangalore Zari Vendors Association', 'ZARI_VENDORS', 'ap-south-1', 'FACT-BLR-01',
 '{"payment_rules": {"escrow_release_on_xrf": true, "xrf_threshold": 0.95}, "quality_thresholds": {"gold_ratio": 0.05, "silver_ratio": 0.95}}'),
('GUILD-DYE-BLR', 'Bangalore Dyeing Workers Association', 'DYEING_WORKERS', 'ap-south-1', 'FACT-BLR-01',
 '{"payment_rules": {"piece_rate_per_kg": 120, "shade_match_bonus": 0.20}, "quality_thresholds": {"delta_e_max": 1.0, "wash_fastness": 4}}'),
('GUILD-WEAVE-BLR', 'Bangalore Weaving Guild', 'WEAVING_GUILD', 'ap-south-1', 'FACT-BLR-01',
 '{"payment_rules": {"dependent_base_wage": 8000, "speed_bonus_rate": 0.10, "independent_base_per_saree": 2500}, "quality_thresholds": {"picks_per_inch": 120, "selvage_alignment": 0.95}}'),
('GUILD-POST-BLR', 'Bangalore Post-Making Workers Association', 'POST_MAKING', 'ap-south-1', 'FACT-BLR-01',
 '{"payment_rules": {"piece_rate_per_saree": 150, "stain_penalty": 500}, "quality_thresholds": {"tassel_uniformity": 0.9, "stain_free": true}}');

-- ------------------------------------------------------------
-- 12. SAMPLE DATA: I18N KEYS & TRANSLATIONS
-- ------------------------------------------------------------

INSERT INTO i18n_keys (key_code, category, description) VALUES
('alert.loom.breakage', 'ALERT', 'Loom breakage detected'),
('alert.loom.breakage', 'ALERT', 'Loom breakage detected'),
('button.scan.input', 'BUTTON', 'Input scan'),
('button.scan.output', 'BUTTON', 'Output scan'),
('status.in_progress', 'STATUS', 'In progress'),
('status.certified', 'STATUS', 'Certified'),
('status.quarantined', 'STATUS', 'Quarantined'),
('voice.loom_assigned', 'VOICE_GUIDANCE', 'Loom assigned'),
('voice.design_ready', 'VOICE_GUIDANCE', 'Design ready'),
('voice.material_inspected', 'VOICE_GUIDANCE', 'Material inspected'),
('dashboard.input_queue', 'DASHBOARD', 'Input queue'),
('dashboard.production', 'DASHBOARD', 'Production status'),
('dashboard.payments', 'DASHBOARD', 'Payments');

INSERT INTO i18n_translations (key_id, language_code, translated_text) VALUES
((SELECT id FROM i18n_keys WHERE key_code = 'alert.loom.breakage'), 'en', 'Loom breakage detected'),
((SELECT id FROM i18n_keys WHERE key_code = 'alert.loom.breakage'), 'te', 'పీఠం విరామం కనిపించింది'),
((SELECT id FROM i18n_keys WHERE key_code = 'alert.loom.breakage'), 'ta', 'நாற்துணி உடன்நிறுத்தம் கண்டறியப்பட்டது'),
((SELECT id FROM i18n_keys WHERE key_code = 'button.scan.input'), 'en', 'Input Scan'),
((SELECT id FROM i18n_keys WHERE key_code = 'button.scan.input'), 'te', 'ఇన్‌పుట్ స్కాన్'),
((SELECT id FROM i18n_keys WHERE key_code = 'button.scan.input'), 'ta', 'உள்ளீடு ஸ்கேன்'),
((SELECT id FROM i18n_keys WHERE key_code = 'voice.loom_assigned'), 'en', 'Your loom has been assigned a new design'),
((SELECT id FROM i18n_keys WHERE key_code = 'voice.loom_assigned'), 'te', 'మీ పీఠానికి కొత్త డిజైన్ అందించబడింది'),
((SELECT id FROM i18n_keys WHERE key_code = 'voice.loom_assigned'), 'ta', 'உங்கள் நாற்துணிக்கு புதிய வடிவம் நியமிக்கப்பட்டது');
