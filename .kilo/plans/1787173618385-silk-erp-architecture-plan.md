# Silk & Fabric Manufacturing ERP — Implementation Plan

## Objective
Deliver an AI-Powered Silk & Fabric Manufacturing Enterprise ERP system with 24 role profiles, scanner-driven sequential workflow, AI inspection nodes, and dynamic dashboard routing.

## Scope
- Unified auth with JWT role claims
- 24 role-specific dashboards
- Sequential state machine for production lots
- 6 AI inspection microservices
- PostgreSQL schema + rollback/edge-case rules
- Real-time event propagation between roles

## Out of Scope
- Physical scanner hardware procurement
- AI model training (assume pre-trained models provided)
- Mobile app (web-only in this phase)

## Key Decisions

### 1. Authentication & Authorization
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
- **State enforced in DB**: `production_lots.status` uses CHECK constraint with all valid states
- **Pre-step validation**: Input scan must match exact precursor `Certified:*` or `Queued` status
- **Locking**: Asset locked to operator workstation during `InProgress:*`
- **Post-step propagation**: Digital certification required before status advances; Kafka/RabbitMQ event notifies next role dashboard

### 4. AI Inspection Microservices (6)
| Service | Trigger Step | Model Input | Output |
|---|---|---|---|
| Zari Defect Detection | Zari processing | Thread image/sensor data | Defect classes, confidence, purity score |
| Dye Coloring Defect Detection | Dyeing | Swatch image, CIELAB data | Color delta, metamerism flag |
| Warp Defect Detection | Beam prep | Camera feed, tension data | Loose ends, cross-overs |
| Fabric Defect Detection | Post-weaving | Camera grid | Holes, stains, picks |
| Demand Forecasting & Smart Cutting | Inventory | Historical sales, trends | Forecast, cutting layout |
| Automated Weaving Defect Detection | Active weaving | Jacquard acoustic/sensor data | Structural anomalies |

- All services return JSON verdict: `PASS | FAIL | WARNING`
- Certificates stored in `ai_inspection_certificates` with SHA-256 cryptographic token
- Fail verdict → quarantine → manual review queue

### 5. Database Schema (PostgreSQL)
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

Key constraints:
- UUID primary keys
- Foreign keys enforce referential integrity
- `production_lots.status` CHECK constraint
- Triggers auto-update `updated_at`
- Indexes on all foreign keys and frequently queried columns

### 6. Edge-Case Error Handling
| Scenario | Detection | Action | Recovery |
|---|---|---|---|
| AI verdict FAIL | Certificate inserted with FAIL | Lot → `Quarantined`, lock released | Supervisor override or rework |
| Out-of-order scan | Status mismatch | HTTP 409, no state change | Operator must obtain correct precursor |
| Network loss at factory node | Sync queue backlog | Buffer locally, retry with backoff | Replay when online; conflict resolution by server timestamp |
| Duplicate scan | `fn_check_duplicate_scan` | HTTP 409 | Wait 5 min or supervisor override |

## Implementation Tasks

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
- Validation: Manual QA for each of the 24 role dashboards

### Task 8: Offline Resilience & Sync
- Local PostgreSQL at each factory node
- `pending_sync_queue` for buffering during network loss
- Background sync service with exponential backoff
- Conflict resolution: server timestamp authoritative
- Validation: Simulate network partition, verify data consistency after restore

### Task 9: Monitoring & Observability
- Metrics: lot throughput, AI latency, certification pass/fail rates, scanner success rates
- Alerts: quarantine events, sync failures, high error rates
- Audit log for all state transitions
- Validation: Load test with 1000+ concurrent users

## Rollout Plan
1. **Phase 1** (Week 1-2): Database schema, migrations, stored procedures
2. **Phase 2** (Week 3-4): Auth service, API gateway, RBAC
3. **Phase 3** (Week 5-6): Scanner service, workflow service, Kafka integration
4. **Phase 4** (Week 7-8): AI service stubs, digital certification
5. **Phase 5** (Week 9-10): Frontend dashboards, scanner UI
6. **Phase 6** (Week 11-12): Offline resilience, sync, monitoring
7. **Phase 7** (Week 13-14): Load testing, security audit, pilot deployment

## Risks & Mitigations
| Risk | Mitigation |
|---|---|
| AI model latency > 5s | Async processing with status polling; cache frequent queries |
| Network partition data loss | Local DB + pending_sync_queue + idempotent replay |
| Cross-role data leak | Row-level security + API gateway RBAC + client-side bundle isolation |
| Scanner hardware incompatibility | Abstract scanner interface; support Barcode/QR/RFID adapters |
| 24-role state machine complexity | DB CHECK constraints + stored procedures enforce valid transitions |

## Validation Checklist
- [ ] All 24 roles can log in and see their dashboard
- [ ] Full lot progression through all 24 roles without manual intervention
- [ ] AI service returns PASS/FAIL and triggers correct state transition
- [ ] Quarantine workflow functions correctly with supervisor override
- [ ] Network loss simulation: data persists locally and syncs correctly
- [ ] Load test: 1000 concurrent users, <200ms API latency P95
- [ ] Security audit: no cross-role data access, JWT properly validated
- [ ] Database rollback tested for all failure scenarios

## Open Questions for Implementation Team
1. **Kafka vs RabbitMQ**: Which message broker is preferred for event streaming?
2. **AI Model Deployment**: ONNX on edge devices or cloud-hosted TensorRT? (Impacts latency and connectivity requirements)
3. **Scanner Hardware**: Specific models/vendors already selected, or need recommendation?
4. **Multi-region Strategy**: Active-active or active-passive for central DB?
5. **Compliance**: Any specific textile industry regulations (e.g., GOTS, Oeko-Tex) that require certification tracking?
