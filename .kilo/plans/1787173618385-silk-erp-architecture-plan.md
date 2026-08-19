# Silk & Fabric Manufacturing ERP — Implementation Plan

## Objective
Deliver an AI-Powered Silk Saree Manufacturing ERP system for a 2-million-weaver ecosystem producing luxury ethnic wear with Silk Purity Guarantee Certificates and Saree Buy-Back Guarantees. The system supports 29 role profiles, 100% electronic jacquard integration, centralized dyeing, NFC/RFID certificates, generative design engines, self-improving AI, guild management, and multi-language support.

## Scope
- Unified auth with JWT role claims
- 29 role-specific dashboards (24 original + 5 new: Design Generator, Buy-Back Manager, Guild Manager, IoT Device Manager, Localization Manager)
- Sequential state machine for production lots through 24+ roles
- 12 AI microservices (6 original + 6 new: GAN Design, Buy-Back Risk, Dynamic Scheduler PPO, Auto-Training CV, TTS, Festival Forecasting)
- PostgreSQL schema with supply chain, guild, buy-back, IoT, design, localization tables
- Real-time event propagation between roles via Apache Kafka
- Self-improving AI with reinforcement learning and continuous training loops
- Multi-language support (Telugu, Tamil, Kannada, Hindi, Bengali) with TTS
- NFC/RFID embedded certificates with blockchain-style ledger
- Active-active multi-region database

## Out of Scope
- Physical scanner hardware procurement
- AI model training (assume pre-trained models provided)
- Native mobile apps (PWA-only in this phase)
- ESP32 ECU hardware manufacturing ($1M budget allocated for prototyping)

## Key Decisions

### 1. Hardware & IoT Infrastructure
- **Loom Type**: 100% electronic jacquards with USB emulators + Wi-Fi modules
- **Edge Controller**: Custom ESP32-S3 based industrial ECU with 4GB eMMC storage
- **Connectivity**: Local Wi-Fi mesh + MQTT over TLS 1.3 to cloud
- **Power**: Local power lines with isolation transformers and surge protection
- **Budget**: $1M allocated for hardware prototyping
- **NFC**: Embedded in saree pallu selvage during final weaving picks

### 2. Authentication & Authorization
- **Single login endpoint**: `POST /api/v1/auth/login`
- **Multi-tenant OAuth2/OIDC**: Use Keycloak or IdentityServer per factory node
- **JWT claims**: `RoleID`, `FactoryNodeID`, `PermittedOperations[]`
- **Stateful sessions**: Redis store with JTI blacklist
- **Dynamic dashboard routing**: Client reads `RoleID` from JWT and renders role-specific component; server enforces RBAC on every API call

### 2. Role Profiles (24 total)
- Each role has a dedicated dashboard module
- `PermittedOperations[]` controls API access
- Cross-role data isolation enforced at API gateway and DB row-level security
- Admin role (`ROLE-SYSTEM-ADMIN`) has wildcard `*` permissions

### 3. Sequential Workflow State Machine
- **24-step linear pipeline**: Filature Supplier → Zari Inspector → ... → Assistant Weaver → Completed
- **New extended roles**: Design Generator, Buy-Back Manager, Guild Manager, IoT Device Manager, Localization Manager
- **State enforced in DB**: `production_lots.status` uses CHECK constraint with all valid states
- **Pre-step validation**: Input scan must match exact precursor `Certified:*` or `Queued` status
- **Locking**: Asset locked to operator workstation during `InProgress:*`
- **Post-step propagation**: Digital certification required before status advances; Kafka topic `lot.{factory}.certified` notifies next role dashboard

### 4. Event Streaming
- **Message broker**: Apache Kafka
- **Topic naming**: `lot.{factory_node_id}.certified` for certification events
- **Consumer groups**: Each role's dashboard subscribes to its factory's topic
- **Retention**: Log-based retention supports replay for offline factory nodes
- **Ordering**: Partitioned by `lot_id` to preserve sequential workflow order

### 5. Multi-Region Database Strategy
- **Architecture**: Active-active multi-region
- **Regions**: At least 2 geographic regions with writable primaries
- **Factory node routing**: Each factory node assigned to nearest region via `factory_nodes.config`
- **Conflict resolution**: Last-write-wins with server timestamp authority
- **Replication**: Synchronous cross-region replication for critical tables; eventual consistency for analytics
- **Failover**: Automatic redirect to secondary region if primary becomes unavailable

### 6. AI Model Deployment Strategy
| Service | Trigger Step | Model Input | Output |
|---|---|---|---|
| Zari Defect Detection | Zari processing | Thread image/sensor data | Defect classes, confidence, purity score |
| Dye Coloring Defect Detection | Dyeing | Swatch image, CIELAB data | Color delta, metamerism flag |
| Warp Defect Detection | Beam prep | Camera feed, tension data | Loose ends, cross-overs |
| Fabric Defect Detection | Post-weaving | Camera grid | Holes, stains, picks |
| Demand Forecasting & Smart Cutting | Inventory | Historical sales, trends | Forecast, cutting layout |
| Automated Weaving Defect Detection | Active weaving | Jacquard acoustic/sensor data | Structural anomalies |

- **Edge services**: Zari Defect Detection, Dye Coloring Defect Detection, Warp Defect Detection, Fabric Defect Detection, Automated Weaving Defect Detection deployed as ONNX/TensorRT models on local edge hardware per workstation
- **Cloud service**: Demand Forecasting & Smart Cutting Optimization deployed centrally on GPU servers; batch async processing
- **Self-improving services**: GAN Design Generation, Buy-Back Risk Predictor, Dynamic Scheduler (PPO), Auto-Training CV (YOLOv8) deployed centrally with edge fallback
- **Fallback**: If edge hardware unavailable, cloud fallback with cached model; degraded mode with manual inspection prompts

### 7. Database Schema (PostgreSQL)
Core tables:
- `users`, `roles`, `user_sessions`
- `factory_nodes`
- `production_lots`, `workflow_states`, `workflow_transitions`
- `scanner_logs`
- `ai_inspection_certificates`
- `audit_logs`
- `pending_sync_queue` (offline resilience)
- `quarantined_lots`
- `materials`, `inventory_movements`
- `yarn_batches`, `zari_batches`, `dye_vats`, `loom_assignments`, `finished_sarees`, `buyback_guarantees`
- `design_files`, `design_generations`, `design_sales_feedback`
- `guilds`, `guild_members`, `guild_payments`, `guild_incentives`
- `edge_controllers`, `loom_telemetry`, `design_injections`
- `i18n_keys`, `i18n_translations`, `voice_audio_cache`
- `certificate_ledger`, `nfc_registry`
- `sku_catalog`, `sku_production_mapping`

Key constraints:
- UUID primary keys
- Foreign keys enforce referential integrity
- `production_lots.status` CHECK constraint
- Triggers auto-update `updated_at`
- Indexes on all foreign keys and frequently queried columns
- Blockchain-style immutability via cryptographic signatures on certificates

### 8. Edge-Case Error Handling
| Scenario | Detection | Action | Recovery |
|---|---|---|---|
| AI verdict FAIL | Certificate inserted with FAIL | Lot → `Quarantined`, lock released | Supervisor override or rework |
| Out-of-order scan | Status mismatch | HTTP 409, no state change | Operator must obtain correct precursor |
| Network loss at factory node | Sync queue backlog | Buffer locally, retry with backoff | Replay when online; conflict resolution by server timestamp |
| Duplicate scan | `fn_check_duplicate_scan` | HTTP 409 | Wait 5 min or supervisor override |

### 9. Implementation Tasks

### Task 1: Database Schema & Migrations
- Create `migrations/001_initial_schema.sql` with all tables, indexes, triggers, and sample role data
- Create `migrations/002_workflow_transitions.sql` with sequential role transitions
- Create `migrations/003_stored_procedures.sql` with:
  - `sp_validate_scanner_input`
  - `sp_process_ai_certification`
  - `sp_process_scanner_output`
  - `sp_sync_pending_operations`
- Create `migrations/004_quarantine_management.sql` with quarantine tables and supervisor override function
- Validation: Run migrations against PostgreSQL 14+; verify all constraints and indexes

### Task 2: Auth Service
- Implement `/api/v1/auth/login` with OAuth2/OIDC validation
- JWT payload includes `RoleID`, `FactoryNodeID`, `PermittedOperations[]`
- Session store in Redis with JTI blacklist
- Validation: Postman collection for login, token refresh, logout

### Task 3: API Gateway & RBAC Middleware
- Kong/APISIX with OIDC plugin
- Route `/api/v1/*` to backend services
- RBAC middleware checks `PermittedOperations[]` per endpoint
- Validation: Test 403 responses for unauthorized access

### Task 4: Scanner Service
- `POST /api/v1/scanner/input` — validates precursor status, locks lot, logs scan
- `POST /api/v1/scanner/output` — validates certification, advances state, publishes Kafka event
- Both call stored procedures for atomic DB operations
- Validation: Simulate full lot progression through all 24 roles

### Task 5: AI Inspection Service
- 6 endpoints: `/api/v1/ai/{service_name}/inspect`
- Accepts lot_id, input data, threshold config
- Calls AI model (mock for now, replace with real ONNX/TensorRT later)
- Returns verdict, confidence, defects, metrics
- Triggers digital certification on PASS
- Validation: Unit tests for each service with mock AI responses

### Task 6: Workflow State Machine Service
- `GET /api/v1/lots/{lot_id}/status` — current state, next role
- `GET /api/v1/dashboard/input-queue` — lots queued for current role
- `POST /api/v1/lots/{lot_id}/override` — supervisor quarantine override
- Publishes Kafka events on state transitions
- Validation: End-to-end test simulating 24-role progression

### Task 7: Frontend Dashboard Router
- JWT-based role detection on login
- Dynamic component injection based on `DASHBOARD_REGISTRY`
- Each dashboard subscribes to Kafka topic for its role's input queue
- Scanner UI with input/output scan buttons and real-time status
- Multi-language support with TTS audio playback
- Validation: Manual QA for each of the 29 role dashboards

### Task 7.5: SKU Product Catalog Service
- `GET /api/v1/sku` — list SKUs with pagination and filtering
- `GET /api/v1/sku/{sku_id}` — get single SKU details
- `GET /api/v1/sku/filters/options` — get distinct filter values
- Filter by geographic hub, weave category, jacquard capacity, weight category
- 480 SKUs across 18 hubs, 14 weave categories, 6 jacquard capacities, 4 zari configs
- Costing model: yarn + zari + labor = total MFG cost
- Pricing: MRP, selling price, min floor price with dynamic margins
- Production mapping: SKU → Lot → Design → Loom assignment
- Frontend: SKU Manager dashboard with advanced filters, stats, and pagination
- Validation: Seed 480 SKUs, verify filters return correct results

### Task 7.5: SKU Product Catalog Service
- Create `migrations/006_sku_catalog.sql` with `sku_catalog` and `sku_production_mapping` tables
- Implement FastAPI service on port 5009 with filtering, pagination, and filter-options endpoints
- Seed 480 SKUs via CSV (`services/sku/sku_catalog.csv`) + `seed_sku.py`
- Add `DashboardSKUManager` frontend with advanced filters and stats
- Integrate SKU selection into Design Generator dashboard and backend generate endpoint
- Validation: Verify 480 SKUs load correctly, filters return expected subsets, design generation accepts SKU reference

### Task 8: IoT Service
- `POST /api/v1/iot/telemetry` — ingest MQTT telemetry from ECU edge controllers
- `POST /api/v1/iot/design/inject` — inject design files to ECU via MQTT
- `GET /api/v1/iot/devices` — list all edge controllers
- Validates loom availability and device online status before injection

### Task 9: Design Service
- `POST /api/v1/design/generate` — GAN design generation
- `GET /api/v1/designs` — list designs with filtering
- `POST /api/v1/designs/{pattern_id}/approve` — approve design for production
- Stores design files with hook count, segment mapping, and file metadata

### Task 10: Buy-Back Service
- `POST /api/v1/buyback/valuate` — AI-powered buy-back valuation
- `POST /api/v1/buyback/{buyback_id}/approve` — approve buy-back payout
- Calculates depreciation based on fabric thinning, gold oxidation, stains
- Links to NFC registry for cryptographic authentication

### Task 11: Guild Service
- `GET /api/v1/guilds` — list all guilds
- `GET /api/v1/guilds/{guild_id}/members` — list guild members
- `POST /api/v1/guilds/{guild_id}/payments` — create payment record
- `GET /api/v1/guilds/{guild_id}/payments` — list payments
- Manages piece-rate calculations, escrow releases, performance scores

### Task 12: Localization Service
- `GET /api/v1/i18n/translations` — get translations for language
- `POST /api/v1/i18n/tts/generate` — generate TTS audio
- `GET /api/v1/i18n/keys` — list all i18n keys
- Supports Telugu, Tamil, Kannada, Hindi, Bengali, English

### Task 13: Offline Resilience & Sync
- Local PostgreSQL at each factory node
- `pending_sync_queue` for buffering during network loss
- Background sync service with exponential backoff
- Conflict resolution: server timestamp authoritative
- Validation: Simulate network partition, verify data consistency after restore

### Task 14: Monitoring & Observability
- Metrics: lot throughput, AI latency, certification pass/fail rates, scanner success rates, ECU telemetry
- Alerts: quarantine events, sync failures, high error rates, loom faults
- Audit log for all state transitions
- Validation: Load test with 1000+ concurrent users

## Rollout Plan
1. **Phase 1** (Week 1-2): Database schema, migrations, stored procedures
2. **Phase 2** (Week 3-4): Auth service, API gateway, RBAC
3. **Phase 3** (Week 5-6): Scanner service, workflow service, Kafka integration
4. **Phase 4** (Week 7-8): AI service stubs, digital certification
5. **Phase 5** (Week 9-10): IoT service, design service, buy-back service
6. **Phase 6** (Week 11-12): Guild service, localization service, SKU catalog service, frontend dashboards
7. **Phase 7** (Week 13-14): Offline resilience, sync, monitoring
8. **Phase 8** (Week 15-16): Load testing, security audit, pilot deployment

## Risks & Mitigations
| Risk | Mitigation |
|---|---|
| AI model latency > 5s | Async processing with status polling; cache frequent queries |
| Network partition data loss | Local DB + pending_sync_queue + idempotent replay |
| Cross-role data leak | Row-level security + API gateway RBAC + client-side bundle isolation |
| Scanner hardware incompatibility | Abstract scanner interface; support Barcode/QR/RFID adapters |
| 24-role state machine complexity | DB CHECK constraints + stored procedures enforce valid transitions |

## Validation Checklist
- [ ] All 29 roles can log in and see their dashboard
- [ ] Full lot progression through all 24+ roles without manual intervention
- [ ] AI service returns PASS/FAIL and triggers correct state transition
- [ ] Quarantine workflow functions correctly with supervisor override
- [ ] Network loss simulation: data persists locally and syncs correctly
- [ ] Load test: 1000 concurrent users, <200ms API latency P95
- [ ] Security audit: no cross-role data access, JWT properly validated
- [ ] Database rollback tested for all failure scenarios
- [ ] IoT telemetry ingestion and design injection work correctly
- [ ] Buy-back valuation engine returns accurate depreciation values
- [ ] Guild payment calculations match piece-rate formulas
- [ ] TTS generates audio in all 5 regional dialects
- [ ] NFC certificate ledger links to finished sarees correctly

## Open Questions for Implementation Team
1. **Kafka vs RabbitMQ**: ~~Which message broker is preferred for event streaming?~~ → **Decided: Apache Kafka** (log retention supports offline replay, partitioned by `lot_id` for ordering)
2. **AI Model Deployment**: ONNX on edge devices or cloud-hosted TensorRT? (Impacts latency, connectivity requirements, and hardware costs)
3. **Scanner Hardware**: Specific models/vendors already selected, or need recommendation?
4. **Multi-region Strategy**: Active-active or active-passive for central DB?
5. **Compliance**: Any specific textile industry regulations (e.g., GOTS, Oeko-Tex) that require certification tracking?
6. **ESP32 ECU**: Custom PCB design vendor selected, or need recommendation?
7. **NFC Tag Vendor**: Specific NFC tag manufacturer for embedding in saree selvage?
8. **GAN Design Engine**: StyleGAN-XL training data source and compute budget?
