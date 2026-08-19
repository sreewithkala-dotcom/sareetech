# AI-Powered Silk Saree Manufacturing ERP System

## Overview

This is a production-ready implementation of an AI-Powered Silk Saree Manufacturing ERP system designed for a 2-million-weaver ecosystem producing luxury ethnic wear with Silk Purity Guarantee Certificates and Saree Buy-Back Guarantees. It supports 29 operational role profiles, automated quality inspection nodes, sequential workflow validations (Chain of Custody), dynamic dashboard routing, generative design engines, and self-improving AI algorithms.

## Key Features

- **100% Electronic Jacquard Integration**: Direct design file push to looms via USB emulator + Wi-Fi modules
- **Centralized Dyeing**: IoT spectrophotometer feedback, chemical/water automation, Delta-E tracking
- **NFC/RFID Certificates**: Embedded in saree pallu selvage for authenticity and buy-back guarantees
- **GAN Design Engine**: Generates thousands of daily design variations (StyleGAN-XL + regional motifs)
- **Self-Improving AI**: Reinforcement learning scheduling, auto-training CV, continuous optimization
- **Guild & Association Management**: Warp vendors, zari suppliers, dyeing workers, weaving guilds, post-making workers
- **Multi-Language Support**: Telugu, Tamil, Kannada, Hindi, Bengali with TTS voice guidance
- **Active-Active Multi-Region DB**: Synchronous cross-region replication with conflict resolution

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Frontend (React PWA)                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Login Page   │  │   Dashboard  │  │   Scanner UI        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      API Gateway (Kong/APISIX)               │
│                    RBAC + OIDC Validation                    │
└─────────────────────────────────────────────────────────────┘
                           │
           ┌───────────────┼───────────────┬───────────────┐
           ▼               ▼               ▼               ▼
┌─────────────────┐ ┌─────────────┐ ┌─────────────────┐ ┌─────────────────┐
│   Auth Service  │ │   Scanner   │ │   Workflow      │ │    IoT Service  │
│   (Port 5000)   │ │   Service   │ │   Service       │ │   (Port 5004)   │
└─────────────────┘ │  (Port 5001)│ │   (Port 5003)   │ └─────────────────┘
                    └─────────────┘ └─────────────────┘
                              │               │
                              ▼               ▼
                    ┌─────────────────┐ ┌─────────────────┐
                    │   AI Service    │ │   Design Service│
                    │   (Port 5002)   │ │   (Port 5005)   │
                    └─────────────────┘ └─────────────────┘
                              │               │
                              ▼               ▼
                    ┌─────────────────┐ ┌─────────────────┐
                    │  Buy-Back       │ │    Guild        │
                    │   Service       │ │   Service       │
                    │   (Port 5006)   │ │   (Port 5007)   │
                    └─────────────────┘ └─────────────────┘
                              │               │
                              ▼               ▼
                    ┌─────────────────┐ ┌─────────────────┐
                    │ Localization    │ │   PostgreSQL    │
                    │   Service       │ │   (Port 5432)   │
                    │   (Port 5008)   │ │                 │
                    └─────────────────┘ └─────────────────┘
                                              │
                                              ▼
                    ┌─────────────────────────────────────┐
                    │         Apache Kafka                │
                    │         (Port 9092)                 │
                    │                                     │
                    │  Topic: lot.{factory}.certified     │
                    │  Partitioned by lot_id for ordering  │
                    └─────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Node.js 18+ (for frontend development)
- Python 3.11+ (for backend development)
- PostgreSQL 14+ (if running without Docker)

### Option 1: Docker Compose (Recommended)

```bash
# Clone the repository
git clone <repository-url>
cd silk-erp

# Start all services
docker-compose up -d

# Check service health
docker-compose ps

# View logs
docker-compose logs -f
```

### Option 2: Manual Setup

#### 1. Database Setup

```bash
# Install PostgreSQL extensions
psql -U postgres -d silk_erp -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"
psql -U postgres -d silk_erp -c "CREATE EXTENSION IF NOT EXISTS \"pgcrypto\";"

# Run migrations in order
psql -U postgres -d silk_erp -f migrations/001_initial_schema.sql
psql -U postgres -d silk_erp -f migrations/002_workflow_transitions.sql
psql -U postgres -d silk_erp -f migrations/003_stored_procedures.sql
psql -U postgres -d silk_erp -f migrations/004_quarantine_management.sql
psql -U postgres -d silk_erp -f migrations/005_silk_saree_extensions.sql
```

#### 2. Start Kafka

```bash
# Start Zookeeper
zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka broker
kafka-server-start.sh config/server.properties

# Create certification topic
kafka-topics.sh --create --topic lot.FACT-BLR-01.certified --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

#### 3. Start Backend Services

```bash
# Auth Service
cd services/auth && pip install -r ../../requirements.txt && python app.py &

# Scanner Service
cd services/scanner && python app.py &

# AI Service
cd services/ai && python app.py &

# Workflow Service
cd services/workflow && python app.py &

# IoT Service
cd services/iot && python app.py &

# Design Service
cd services/design && python app.py &

# Buy-Back Service
cd services/buyback && python app.py &

# Guild Service
cd services/guild && python app.py &

# Localization Service
cd services/localization && python app.py &

# SKU Service
cd services/sku && pip install -r ../../requirements.txt && python main.py &

# Localization Service
cd services/localization && python app.py &
```

#### 4. Start Frontend

```bash
cd frontend
npm install
npm run dev
```

## Configuration

### Environment Variables

Create a `.env` file in the root directory:

```env
# Database
DB_HOST=localhost
DB_NAME=silk_erp
DB_USER=postgres
DB_PASSWORD=postgres

# JWT
JWT_SECRET_KEY=your-secret-key-change-in-production

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Redis (for session storage)
REDIS_URL=redis://localhost:6379/0
```

### Factory Node Configuration

Factory nodes are configured in the `factory_nodes.config` JSONB field:

```json
{
  "loom_type": "mixed",
  "dyeing_model": "mixed",
  "scanner_adapters": {
    "input": "usb_hid",
    "output": "usb_hid"
  },
  "edge_ai_enabled": true,
  "region": "ap-south-1",
  "languages": ["te", "ta", "kn", "hi", "bn", "en"],
  "ecu_firmware_version": "1.0.0",
  "mesh_network_enabled": true
}
```

## Extended Role Profiles (29 Roles)

| # | Role | RoleID | Dashboard | AI Service | Guild/Association |
|---|------|--------|-----------|------------|-------------------|
| 1 | Assistant Weaver | `ROLE-ASSISTANT-WEAVER` | DashboardWeavingMonitor | Weaving Defect Detection | Weaving Guild |
| 2 | Bobbin Winder | `ROLE-BOBBIN-WINDER` | DashboardBobbinWinder | Zari Defect Detection | - |
| 3 | Card Puncher | `ROLE-CARD-PUNCHER` | DashboardCardPuncher | N/A | - |
| 4 | Filature Supplier | `ROLE-FILATURE-SUPPLIER` | DashboardFilatureSupplier | Zari + Dye Detection | Warp Vendors Association |
| 5 | Graph Drafter | `ROLE-GRAPH-DRAFTER` | DashboardGraphDrafter | Demand Forecasting | Design Guild |
| 6 | LOG Finishing Specialist | `ROLE-LOG-FINISHING` | DashboardLogFinishing | Fabric Defect Detection | Post-Making Workers Association |
| 7 | Loom Harness Setter | `ROLE-LOOM-HARNESS-SETTER` | DashboardLoomHarnessSetter | Warp Defect Detection | - |
| 8 | Master Colorist | `ROLE-MASTER-COLORIST` | DashboardMasterColorist | Dye Coloring Detection | Dyeing Workers Association |
| 9 | Master Weaver | `ROLE-MASTER-WEAVER` | DashboardMasterWeaver | Fabric + Weaving Defect | Weaving Guild |
| 10 | Petni Master | `ROLE-PETNI-MASTER` | DashboardPetniMaster | Zari Defect Detection | - |
| 11 | Pirn Winders | `ROLE-PIRN-WINDERS` | DashboardPirnWinders | Zari Defect Detection | - |
| 12 | QA Dyeing Inspector | `ROLE-QA-DYEING-INSPECTOR` | DashboardQADyeingInspector | Dye Coloring Detection | Dyeing Workers Association |
| 13 | Quality Inspector | `ROLE-QUALITY-INSPECTOR` | DashboardQualityInspector | All Services | - |
| 14 | Silk Degumming Master | `ROLE-SILK-DEGUMMING-MASTER` | DashboardSilkDegummingMaster | N/A | - |
| 15 | Silk Grader | `ROLE-SILK-GRADER` | DashboardSilkGrader | Zari Defect Detection | Warp Vendors Association |
| 16 | Silk Mark Officer | `ROLE-SILK-MARK-OFFICER` | DashboardSilkMarkOfficer | N/A | - |
| 17 | System Admin | `ROLE-SYSTEM-ADMIN` | DashboardSystemAdmin | All Services | - |
| 18 | Skein Dye Master | `ROLE-SKEIN-DYE-MASTER` | DashboardSkeinDyeMaster | Dye Coloring Detection | Dyeing Workers Association |
| 19 | Store Inventory Manager | `ROLE-STORE-INVENTORY-MANAGER` | DashboardStoreInventoryManager | N/A | - |
| 20 | SUP Loom Floor Supervisor | `ROLE-SUP-LOOM-FLOOR-SUPERVISOR` | DashboardSUPLoomFloorSupervisor | Weaving Defect Detection | Weaving Guild |
| 21 | Throwster/Twister | `ROLE-THROWSTER-TWISTER` | DashboardThrowsterTwister | Zari Defect Detection | - |
| 22 | Warp Beam Preparation Specialist | `ROLE-WARP-BEAM-PREPARATION` | DashboardWarpBeamPreparation | Warp Defect Detection | Warp Vendors Association |
| 23 | Warp Joiner | `ROLE-WARP-JOINER` | DashboardWarpJoiner | Warp Defect Detection | Warp Vendors Association |
| 24 | Zari Inspector | `ROLE-ZARI-INSPECTOR` | DashboardZariInspector | Zari Defect Detection | Zari Vendors Association |
| 25 | Design Generator | `ROLE-DESIGN-GENERATOR` | DashboardDesignGenerator | GAN Design Generation | Design Guild |
| 26 | Buy-Back Manager | `ROLE-BUY-BACK-MANAGER` | DashboardBuyBackManager | Buy-Back Risk Predictor | - |
| 27 | Guild Manager | `ROLE-GUILD-MANAGER` | DashboardGuildManager | N/A | All Guilds |
| 28 | IoT Device Manager | `ROLE-IOT-DEVICE-MANAGER` | DashboardIoTDeviceManager | N/A | - |
| 29 | Localization Manager | `ROLE-LOCALIZATION-MANAGER` | DashboardLocalizationManager | N/A | - |

## Sequential Workflow (29 Roles)

```
Filature Supplier → Zari Inspector → Silk Grader → Skein Dye Master →
QA Dyeing Inspector → Silk Degumming Master → Silk Mark Officer →
Store Inventory Manager → Warp Beam Preparation Specialist → Warp Joiner →
Graph Drafter → Card Puncher → Loom Harness Setter → Bobbin Winder →
Pirn Winders → Petni Master → Throwster/Twister → Master Weaver →
LOG Finishing Specialist → Quality Inspector → QA Dyeing Inspector →
Master Colorist → Zari Inspector (Final) → SUP Loom Floor Supervisor →
Assistant Weaver → Design Generator → Buy-Back Manager → Guild Manager → System Admin
```

## API Endpoints

### Authentication
- `POST /api/v1/auth/login` - Login with email, password, factory_node_id
- `POST /api/v1/auth/refresh` - Refresh access token
- `POST /api/v1/auth/logout` - Logout (revoke token)
- `GET /api/v1/auth/me` - Get current user info

### Scanner Operations
- `POST /api/v1/scanner/input` - Input scan with pre-step validation
- `POST /api/v1/scanner/output` - Output scan with post-step propagation

### AI Inspection
- `POST /api/v1/ai/{service}/inspect` - Trigger AI inspection
  - Services: `zari`, `dye`, `warp`, `fabric`, `weaving`, `forecasting`
- `GET /api/v1/ai/health` - Health check

### Workflow Management
- `GET /api/v1/lots/{lot_id}/status` - Get lot status
- `GET /api/v1/dashboard/input-queue` - Get lots queued for current role
- `POST /api/v1/lots/{lot_id}/override` - Supervisor quarantine override
- `GET /api/v1/lots` - List lots with filtering

### IoT & Edge Controllers
- `POST /api/v1/iot/telemetry` - Ingest loom telemetry from ECU
- `POST /api/v1/iot/design/inject` - Inject design file to ECU via MQTT
- `GET /api/v1/iot/devices` - List all edge controllers
- `GET /api/v1/iot/health` - Health check

### Design Management
- `POST /api/v1/design/generate` - Generate GAN designs
- `GET /api/v1/designs` - List designs with filtering
- `POST /api/v1/designs/{pattern_id}/approve` - Approve design for production
- `GET /api/v1/design/health` - Health check

### Buy-Back Guarantees
- `POST /api/v1/buyback/valuate` - AI-powered buy-back valuation
- `POST /api/v1/buyback/{buyback_id}/approve` - Approve buy-back payout
- `GET /api/v1/buyback/health` - Health check

### Guild Management
- `GET /api/v1/guilds` - List all guilds
- `GET /api/v1/guilds/{guild_id}/members` - List guild members
- `POST /api/v1/guilds/{guild_id}/payments` - Create payment record
- `GET /api/v1/guilds/{guild_id}/payments` - List payments
- `GET /api/v1/guilds/health` - Health check

### Localization
- `GET /api/v1/i18n/translations` - Get translations for language
- `POST /api/v1/i18n/tts/generate` - Generate TTS audio
- `GET /api/v1/i18n/keys` - List all i18n keys
- `GET /api/v1/i18n/health` - Health check

## AI Microservices

### 1. Zari Defect Detection
- **Trigger**: Zari processing steps
- **Input**: Thread image, sensor data
- **Output**: Defect classes (break, tarnish, thickness_variation, purity_drop), confidence, purity score
- **Model**: zari-v2.3.1 (Edge ONNX/TensorRT)

### 2. Dye Coloring Defect Detection
- **Trigger**: Dyeing steps
- **Input**: Fabric swatch, CIELAB data
- **Output**: Delta E, metamerism flag, color coordinates
- **Model**: dye-color-v2.3.1 (Edge ONNX/TensorRT)

### 3. Warp Defect Detection
- **Trigger**: Warp beam preparation
- **Input**: Camera feed, tension sensors
- **Output**: Loose ends, cross-overs, tension drops
- **Model**: warp-defect-v2.3.1 (Edge ONNX/TensorRT)

### 4. Fabric Defect Detection
- **Trigger**: Post-weaving
- **Input**: Camera grid
- **Output**: Holes, stains, float errors, miss-picks, double picks
- **Model**: fabric-defect-v2.3.1 (Edge ONNX/TensorRT)

### 5. Demand Forecasting & Smart Cutting
- **Trigger**: Inventory review
- **Input**: Historical sales, global trends
- **Output**: Forecast, cutting layout, scrap reduction %
- **Model**: demand-forecast-lstm-v2.3.1 (Cloud GPU)

### 6. Automated Weaving Defect Detection
- **Trigger**: Active weaving
- **Input**: Jacquard acoustic/sensor data
- **Output**: Structural anomalies, pick rate
- **Model**: weaving-defect-v2.3.1 (Edge ONNX/TensorRT)

### 7. GAN Design Generation
- **Trigger**: Design request queue
- **Input**: Market demand, historical sales, festival calendars
- **Output**: 1000+ design variations daily
- **Model**: StyleGAN-XL + regional motif conditioning

### 8. Buy-Back Risk Predictor
- **Trigger**: Return request
- **Input**: NFC tag, multi-spectral scan
- **Output**: Dynamic buy-back value, depreciation breakdown
- **Model**: Financial time-series + depreciation curves

### 9. Dynamic Scheduler (PPO)
- **Trigger**: Production changes
- **Input**: Dye batch cycles, jacquard efficiencies, weaver speeds
- **Output**: Automated re-routing, schedule optimization
- **Model**: Proximal Policy Optimization (PPO)

### 10. Auto-Training Computer Vision
- **Trigger**: Inspector correction
- **Input**: Defect images, human overrides
- **Output**: Continuous model improvement
- **Model**: YOLOv8 + Autoencoder

## Error Handling

### AI Defect Failure
- System transitions lot to `Quarantined` status
- Lock released automatically
- Supervisor can override via `POST /api/v1/lots/{lot_id}/override`

### Out-of-Order Scanning
- HTTP 409 with `PrerequisiteMissing` error
- Asset must have correct precursor status (`Certified:*` or `Queued`)

### Network Loss
- Operations buffered in `pending_sync_queue`
- Automatic retry with exponential backoff
- Conflict resolution: server timestamp authoritative

## Testing

```bash
# Run database migrations
docker-compose exec postgres psql -U postgres -d silk_erp -f /docker-entrypoint-initdb.d/001_initial_schema.sql

# Test login
curl -X POST http://localhost:5000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@factory.com","password":"admin123","factory_node_id":"FACT-BLR-01"}'

# Test scanner input
curl -X POST http://localhost:5001/api/v1/scanner/input \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"asset_id":"ASSET-001","factory_node_id":"FACT-BLR-01"}'

# Test AI inspection
curl -X POST http://localhost:5002/api/v1/ai/zari/inspect \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"lot_id":"<lot-id>","role_id":"<role-id>","input_data":{}}'

# Test IoT telemetry
curl -X POST http://localhost:5004/api/v1/iot/telemetry \
  -H "Content-Type: application/json" \
  -d '{"loom_id":"LOOM-001","device_id":"ECU-001","current_picks":100,"target_picks":500,"faults_detected":0}'

# Test design generation
curl -X POST http://localhost:5005/api/v1/design/generate \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"count":10,"region":"ap-south-1","motif_style":"kanchipuram"}'

# Test buy-back valuation
curl -X POST http://localhost:5006/api/v1/buyback/valuate \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"nfc_id":"NFC-001","scan_data":{"fabric_thinning_pct":5,"gold_oxidation_pct":2}}'
```

## Scaling Considerations

- **Database**: Shard by `factory_node_id`; read replicas for analytics; active-active multi-region
- **AI Services**: Stateless; scale horizontally behind load balancer
- **Kafka**: Partition by `lot_id` for ordering; multiple brokers for throughput
- **Frontend**: CDN for static assets; WebSocket for real-time updates
- **IoT**: MQTT over TLS 1.3; local Wi-Fi mesh; edge buffering during outages

## Security

- JWT tokens with short expiry (1 hour)
- Refresh tokens for session persistence
- RBAC enforced at API gateway and database row-level security
- All AI certificates cryptographically signed (SHA-256)
- Complete audit trail in `audit_logs` table
- NFC cryptographic keys for buy-back authentication

## License

Proprietary - AI-Powered Silk Saree Manufacturing ERP System
