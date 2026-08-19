# AI-Powered Silk & Fabric Manufacturing ERP System

## Overview

This is a production-ready implementation of an AI-Powered Silk & Fabric Manufacturing Enterprise ERP System. It supports 24 distinct operational role profiles, automated quality inspection nodes, sequential workflow validations (Chain of Custody), and dynamic dashboard routing.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Frontend (React)                        │
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
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
┌─────────────────┐ ┌─────────────┐ ┌─────────────────────┐
│   Auth Service  │ │   Scanner   │ │   Workflow Service  │
│   (Port 5000)   │ │   Service   │ │     (Port 5003)     │
└─────────────────┘ │  (Port 5001)│ └─────────────────────┘
                    └─────────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │   AI Service        │
                    │   (Port 5002)       │
                    │  ┌───────────────┐  │
                    │  │ Zari Defect   │  │
                    │  │ Dye Coloring  │  │
                    │  │ Warp Defect   │  │
                    │  │ Fabric Defect │  │
                    │  │ Weaving Defect│  │
                    │  │ Forecasting   │  │
                    │  └───────────────┘  │
                    └─────────────────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │   PostgreSQL        │
                    │   (Port 5432)       │
                    │                     │
                    │  Core Tables:       │
                    │  - users, roles     │
                    │  - production_lots  │
                    │  - workflow_states  │
                    │  - scanner_logs     │
                    │  - ai_certificates  │
                    │  - audit_logs       │
                    └─────────────────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │   Apache Kafka      │
                    │   (Port 9092)       │
                    │                     │
                    │  Topic:             │
                    │  lot.{factory}.certified│
                    └─────────────────────┘
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
cd services/auth
pip install -r ../../requirements.txt
python app.py

# Scanner Service (new terminal)
cd services/scanner
python app.py

# AI Service (new terminal)
cd services/ai
python app.py

# Workflow Service (new terminal)
cd services/workflow
python app.py
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
  "region": "ap-south-1"
}
```

## 24 Role Profiles

| # | Role | Dashboard | AI Service |
|---|------|-----------|------------|
| 1 | Assistant Weaver | DashboardWeavingMonitor | Weaving Defect Detection |
| 2 | Bobbin Winder | DashboardBobbinWinder | Zari Defect Detection |
| 3 | Card Puncher | DashboardCardPuncher | N/A |
| 4 | Filature Supplier | DashboardFilatureSupplier | Zari + Dye Detection |
| 5 | Graph Drafter | DashboardGraphDrafter | Demand Forecasting |
| 6 | LOG Finishing Specialist | DashboardLogFinishing | Fabric Defect Detection |
| 7 | Loom Harness Setter | DashboardLoomHarnessSetter | Warp Defect Detection |
| 8 | Master Colorist | DashboardMasterColorist | Dye Coloring Detection |
| 9 | Master Weaver | DashboardMasterWeaver | Fabric + Weaving Defect |
| 10 | Petni Master | DashboardPetniMaster | Zari Defect Detection |
| 11 | Pirn Winders | DashboardPirnWinders | Zari Defect Detection |
| 12 | QA Dyeing Inspector | DashboardQADyeingInspector | Dye Coloring Detection |
| 13 | Quality Inspector | DashboardQualityInspector | All Services |
| 14 | Silk Degumming Master | DashboardSilkDegummingMaster | N/A |
| 15 | Silk Grader | DashboardSilkGrader | Zari Defect Detection |
| 16 | Silk Mark Officer | DashboardSilkMarkOfficer | N/A |
| 17 | System Admin | DashboardSystemAdmin | All Services |
| 18 | Skein Dye Master | DashboardSkeinDyeMaster | Dye Coloring Detection |
| 19 | Store Inventory Manager | DashboardStoreInventoryManager | N/A |
| 20 | SUP Loom Floor Supervisor | DashboardSUPLoomFloorSupervisor | Weaving Defect Detection |
| 21 | Throwster/Twister | DashboardThrowsterTwister | Zari Defect Detection |
| 22 | Warp Beam Preparation Specialist | DashboardWarpBeamPreparation | Warp Defect Detection |
| 23 | Warp Joiner | DashboardWarpJoiner | Warp Defect Detection |
| 24 | Zari Inspector | DashboardZariInspector | Zari Defect Detection |

## Sequential Workflow (24 Roles)

```
Filature Supplier → Zari Inspector → Silk Grader → Skein Dye Master →
QA Dyeing Inspector → Silk Degumming Master → Silk Mark Officer →
Store Inventory Manager → Warp Beam Preparation Specialist → Warp Joiner →
Graph Drafter → Card Puncher → Loom Harness Setter → Bobbin Winder →
Pirn Winders → Petni Master → Throwster/Twister → Master Weaver →
LOG Finishing Specialist → Quality Inspector → QA Dyeing Inspector →
Master Colorist → Zari Inspector (Final) → SUP Loom Floor Supervisor →
Assistant Weaver → Completed
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

# Test login (replace with actual user)
curl -X POST http://localhost:5000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@factory.com","password":"password","factory_node_id":"FACT-BLR-01"}'

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
```

## Scaling Considerations

- **Database**: Shard by `factory_node_id`; read replicas for analytics
- **AI Services**: Stateless; scale horizontally behind load balancer
- **Kafka**: Partition by `lot_id` for ordering; multiple brokers for throughput
- **Frontend**: CDN for static assets; WebSocket for real-time updates

## Security

- JWT tokens with short expiry (1 hour)
- Refresh tokens for session persistence
- RBAC enforced at API gateway and database row-level security
- All AI certificates cryptographically signed (SHA-256)
- Complete audit trail in `audit_logs` table

## License

Proprietary - AI-Powered Silk & Fabric Manufacturing ERP System
