# SKU Product Catalog Integration — Complete Setup Guide

## 1. SKU Data Overview

The SKU catalog contains **480 product variants** across the following dimensions:

| Dimension | Values |
|-----------|--------|
| Geographic Hubs | 18 (Varanasi, Kanchipuram, Arani, Dharmavaram, Surat, Yeola, Paithan, Bishnupur, Ilkal, Gadwal, Pochampally, Narayanpet, Venkatagiri, Uppada, Chanderi, Maheshwar, Sambalpur, Sualkuchi, Banaras Blend, Mysore) |
| Weave Categories | 14 (North Indian Katan, South Indian Heavy Pattu, Deccan Kasuti Silk, Deccan Zari Bordered, South Tied-Dye Ikat, Deccan Cotton-Silk, Deccan Fine Weave, South Jamdani Translucent, Central Sheer Fabric, Central Ribbed Weave, East Handloom Ikat, Northeast Assam Silk, North Silk-Cotton Mix, South Soft Crepe Silk) |
| Jacquard Capacities | 6 (640 Hooks, 1024 Hooks, 1534 Hooks, 2400 Hooks, 2688 Hooks, 5400 Hooks) |
| Zari Configurations | 4 (1Gram Gold Zari, 2G Gold Zari, 3G Gold Zari, 5G Gold Zari) |
| Weight Categories | 4 (Lightweight Dress Silk, Standard Mid-Weight Saree, Heavy Bridal Brocade, Ultra-Heavy Royal Heritage Pattu) |

## 2. SKU Data Fields

| Field | Type | Description |
|-------|------|-------------|
| sku_ref_id | VARCHAR(50) | Unique SKU identifier (SKU-0001 to SKU-0480) |
| geographic_hub | VARCHAR(100) | Production region/hub |
| weave_category | VARCHAR(100) | Type of weave pattern |
| jacquard_capacity | VARCHAR(50) | Number of hooks (640, 1024, 1534, 2400, 2688, 5400) |
| zari_configuration | VARCHAR(100) | Gold zari weight grade |
| warp_denier | VARCHAR(20) | Warp yarn thickness |
| weft_denier | VARCHAR(20) | Weft yarn thickness |
| zari_wire_denier | VARCHAR(20) | Zari wire thickness |
| warp_net_weight_g | DECIMAL(10,2) | Warp yarn weight in grams |
| weft_net_weight_g | DECIMAL(10,2) | Weft yarn weight in grams |
| zari_net_weight_g | DECIMAL(10,2) | Zari weight in grams |
| bobbin_waste_weight_g | DECIMAL(10,2) | Bobbin waste in grams |
| total_saree_weight_g | DECIMAL(10,2) | Total saree weight in grams |
| yarn_raw_cost_inr | DECIMAL(12,2) | Raw silk yarn cost in INR |
| zari_raw_cost_inr | DECIMAL(12,2) | Raw zari cost in INR |
| labor_surcharge_inr | DECIMAL(12,2) | Labor cost surcharge in INR |
| total_mfg_cost_inr | DECIMAL(12,2) | Total manufacturing cost in INR |
| mrp_inr | DECIMAL(12,2) | Maximum retail price (sticker price) in INR |
| selling_price_inr | DECIMAL(12,2) | Actual selling price in INR |
| min_floor_price_inr | DECIMAL(12,2) | Minimum floor price in INR |
| weight_category_profile | VARCHAR(100) | Weight category classification |

## 3. Costing Model

```
Total MFG Cost = Yarn Raw Cost + Zari Raw Cost + Labor Surcharge
```

Example (SKU-0001):
- Yarn Raw Cost: ₹542
- Zari Raw Cost: ₹1,800
- Labor Surcharge: ₹3,600
- **Total MFG Cost: ₹6,953**

### Pricing Tiers

| Price Field | Formula | Purpose |
|-------------|---------|---------|
| MRP (Sticker Price) | MFG Cost × 3.5 | Retail display price |
| Selling Price | MFG Cost × 2.45 | Actual transaction price |
| Min Floor Price | MFG Cost × 1.35 | Minimum authorized price |

## 4. Database Schema

### sku_catalog Table

```sql
CREATE TABLE sku_catalog (
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
```

### sku_production_mapping Table

```sql
CREATE TABLE sku_production_mapping (
    id SERIAL PRIMARY KEY,
    sku_id INTEGER REFERENCES sku_catalog(id),
    lot_id INTEGER REFERENCES production_lots(id),
    design_id INTEGER REFERENCES design_files(id),
    loom_id INTEGER REFERENCES loom_assignments(id),
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(50) DEFAULT 'ASSIGNED'
);
```

## 5. API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /api/v1/sku | List SKUs with pagination and filtering |
| GET | /api/v1/sku/{sku_id} | Get single SKU details |
| GET | /api/v1/sku/filters/options | Get distinct filter values |

### Query Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| skip | int | Pagination offset |
| limit | int | Page size (default: 100) |
| geographic_hub | string | Filter by hub |
| weave_category | string | Filter by weave category |
| jacquard_capacity | string | Filter by jacquard hooks |
| weight_category | string | Filter by weight category profile |

## 6. Frontend Dashboard

The SKU Manager dashboard (`DashboardSKUManager`) provides:

1. **Advanced Filters**: Search by SKU ID, hub, weave category, jacquard capacity, weight category
2. **Statistics Cards**: Total SKUs, filtered results, average MFG cost, average selling price
3. **Paginated Table**: SKU details with weight category chips
4. **Responsive Design**: MUI-based layout matching other dashboards

## 7. Integration with Production Workflow

```
[Design Generator] → Creates design file
         ↓
[SKU Selection] → Selects appropriate SKU based on design parameters
         ↓
[Lot Creation] → Assigns SKU to production lot
         ↓
[Loom Assignment] → Maps SKU → Lot → Design → Loom
         ↓
[Production] → Tracks actual vs. planned metrics
         ↓
[Quality] → Validates final product against SKU specs
         ↓
[Certificate] → Issues certificate with SKU reference
```

## 8. Seeding the SKU Data

To seed the 480 SKUs into the database:

```bash
# Ensure PostgreSQL is running
docker-compose up -d postgres

# Run the seed script
python seed_sku.py
```

The seed script will:
1. Clear existing SKU data
2. Insert all 480 SKU rows with accurate metrics
3. Commit the transaction

## 9. Service Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| DB_HOST | localhost | PostgreSQL host |
| DB_PORT | 5432 | PostgreSQL port |
| DB_NAME | silk_erp | Database name |
| DB_USER | postgres | Database user |
| DB_PASSWORD | postgres | Database password |

### Docker Service

The SKU service is defined in `docker-compose.yml`:

```yaml
sku-service:
  build:
    context: ./services/sku
    dockerfile: Dockerfile
  container_name: silk-erp-sku
  ports:
    - "5009:5009"
  environment:
    DB_HOST: postgres
    DB_NAME: silk_erp
    DB_USER: postgres
    DB_PASSWORD: postgres
  depends_on:
    postgres:
      condition: service_healthy
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:5009/api/v1/sku/health"]
    interval: 10s
    timeout: 10s
    retries: 3
```

## 10. Next Steps

1. Complete remaining 460 SKU rows in seed script (currently showing 16 sample rows)
2. Add SKU selection API to Design Generator service
3. Implement SKU-based cost calculation in Buy-Back valuation engine
4. Add SKU filtering to IoT design injection validation
5. Create SKU comparison tool for design feasibility analysis
