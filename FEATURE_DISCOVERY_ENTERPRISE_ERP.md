# Silk Saree Manufacturing ERP — Enterprise Feature Discovery & AI-Native Blueprint

**Target Scale:** Millions of users, vendors, and loom operators across 100,000+ artisan households
**Architecture:** Event-driven CQRS microservices with deeply embedded generative AI, predictive ML, and automated workflows
**Primary Storage:** CockroachDB (OLTP) | Neo4j (lineage graph) | Redis (cache/trace) | S3 (binary artifacts)
**Identity:** 64-bit Snowflake IDs with functional prefixes
**Integration:** Apache Kafka event streaming, MQTT IoT, OAuth2/OIDC, JWT

---

## 1. Enterprise Module Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SILK SAREE ERP — 9 CORE MODULES                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             │
│  │   FINANCE    │  │  MANUFACTURING│  │   SUPPLY     │             │
│  │   MODULE     │  │    MODULE     │  │   CHAIN      │             │
│  │  (AI/ML +    │  │ (AI/ML +      │  │  MODULE      │             │
│  │  Automation) │  │  Automation)  │  │(AI/ML +      │             │
│  └──────┬───────┘  └──────┬───────┘  │  Automation) │             │
│         │                 │            └──────┬───────┘             │
│         ▼                 ▼                   ▼                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             │
│  │      HR      │  │  PROCUREMENT │  │   QUALITY    │             │
│  │   MODULE     │  │    MODULE     │  │   MODULE     │             │
│  │ (AI/ML +     │  │ (AI/ML +      │  │ (AI/ML +     │             │
│  │  Automation) │  │  Automation)  │  │  Automation) │             │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘             │
│         │                 │                   │                     │
│         └─────────────────┼───────────────────┘                     │
│                           ▼                                         │
│                  ┌──────────────────┐                               │
│                  │  UNIFIED AI/ML   │                               │
│                  │  SERVICES LAYER  │                               │
│                  │  (Cross-Module)  │                               │
│                  └──────────────────┘                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. AI/ML/Automation Service Catalog

### 2.1 Embedded AI Services (Edge + Cloud)

| Service ID | Service Name | Deployment | Trigger | Input | Output | Consumes |
|---|---|---|---|---|---|---|
| `AI-ZARI-DEFECT` | Zari Defect Detection | Edge (ONNX) | Zari processing | Thread image/sensor | Defect classes, confidence, purity score | `RAW-LOT-*`, `ZARI-*` |
| `AI-DYE-DEFECT` | Dye Coloring Defect Detection | Edge (ONNX) | Skein dyeing | Swatch image, CIELAB | Color delta, metamerism flag | `DYE-LOT-*` |
| `AI-WARP-DEFECT` | Warp Defect Detection | Edge (ONNX) | Beam preparation | Camera feed, tension | Loose ends, cross-overs | `WARP-BCH-*` |
| `AI-FABRIC-DEFECT` | Fabric Defect Detection | Edge (ONNX) | Post-weaving QA | Camera grid | Holes, stains, picks | `SAREE-*` |
| `AI-WEAVING-DEFECT` | Automated Weaving Defect Detection | Edge (ONNX) | Active weaving | Jacquard acoustic/sensor | Structural anomalies | `LOOM-*` telemetry |
| `AI-DEMAND-FCAST` | Demand Forecasting & Smart Cutting | Cloud (GPU) | Daily batch | Historical sales, trends | Forecast, cutting layout | `B2B-ORDER-*`, sales |
| `AI-GAN-DESIGN` | GAN Design Generation | Cloud | On-demand | SKU, trend data | New Jacquard patterns | `SKU-*`, `DES-*` |
| `AI-BUYBACK-RISK` | Buy-Back Risk Predictor | Cloud | Dispatch | Saree grade, history | Risk score, premium | `CERT-*`, `SMK-*` |
| `AI-SCHEDULER` | Dynamic Scheduler (PPO) | Cloud | Batch scheduling | Loom state, orders | Optimal schedule | `LOOM-*`, `ORDER-*` |
| `AI-AUTO-TRAIN` | Auto-Training CV (YOLOv8) | Cloud | Continuous | Defect images | Retrained model | All defect images |
| `AI-TTS` | Text-to-Speech (Multi-lang) | Cloud | UI interaction | Text input | Telugu/Tamil/Kannada/Hindi/Bengali audio | `I18N-*` |
| `AI-FESTIVAL-FCAST` | Festival Forecasting | Cloud | Monthly | Calendar, history | Demand surge | Calendar, sales |

### 2.2 Workflow Automation Engine

| Engine | Trigger | Action | Target Module |
|---|---|---|---|
| `Guardrail Engine` | Any field input | Validate against business rules | All modules |
| `Routing Engine` | State change | Auto-assign next role/queue | Manufacturing |
| `Notification Engine` | Event | Push to Kafka, SMS, email | All modules |
| `Certificate Engine` | Approval | Generate hash + QR + NFT | QA, Compliance, Inventory |
| `Reconciliation Engine` | Daily batch | Match physical vs digital | Finance, Inventory |
| `Requisition Engine` | Stock below threshold | Auto-generate PR | Procurement, Inventory |
| `Payment Engine` | Certification | Release supplier payout | Finance |
| `Shipping Engine` | Manifest complete | Generate docs, track | LOG Finishing |
| `Compliance Engine` | Audit trigger | Lock/unlock batches | QA, Silk Mark |
| `Back-Trace Engine` | Saree scan | Full lineage in <15ms | All modules |

---

## 3. End-to-End Feature Flow: Filature to Saree Inventory

### Phase 0: Sales Forecast → Design Generation

**Trigger:** Sales team uploads forecast
**Automated Actions:**
- `AI-DEMAND-FCAST` predicts 80-saree warp requirements by SKU
- `AI-GAN-DESIGN` generates new Jacquard patterns for trending SKUs
- `AI-FESTIVAL-FCAST` queues festival-specific designs
- Workflow auto-creates `SKU-*` and `DES-*` records

**Roles Involved:** SKU Manager, Design Generator, Sales (external)

---

### Phase 1: Raw Material Intake & QA-1

**Trigger:** Filature Supplier delivers raw silk bales / Zari spools
**Automated Actions:**
- Gate Clerk scans supplier ID → auto-generates `RAW-LOT-{VENDOR_ID}-{TIMESTAMP}`
- QC Inspector inputs moisture %, tenacity, size deviation %
- System calculates `Conditioned Weight = Actual Weight * (1 - Moistage%/100) * 1.11`
- `AI-ZARI-DEFECT` runs on Zari spool images
- Guardrail: IF moisture > 11% OR size deviation > 4% OR tenacity < 3.8 g/d → `QC_REJECT_HOLD`
- `Silk Grader` grades A/B/C → auto-routes to QA-1_CERTIFIED or rejection
- `Zari Inspector` runs XRF → certifies purity class
- `Store Inventory Manager` receives into vault/bin
- `AI-SCHEDULER` updates procurement forecast

**Roles Involved:** Filature Supplier, Silk Grader, QC Inspector, Zari Inspector, Store Inventory Manager, Gate Clerk

**Key AI/ML:**
- Image-based defect detection on raw silk
- Weight reconciliation ML (theft detection)
- Auto-grading confidence scoring

---

### Phase 2: Degumming & Throwing

**Trigger:** Raw silk lot released from inventory
**Automated Actions:**
- `Silk Degumming Master` inputs alkali pH, temp, sericin loss %
- Guardrail: IF sericin loss < 20% OR > 25% → auto-hold for re-processing
- `Throwster/Twister` inputs TPI, S/Z direction, ply count
- System generates `THROWN-SILK-{SNOWFLAKE}` linked upstream to `RAW-LOT-ID`
- `AI-DEMAND-FCAST` updates yarn availability forecast

**Roles Involved:** Silk Degumming Master, Throwster/Twister, Store Inventory Manager

**Key AI/ML:**
- Process parameter optimization (pH/temp/TPI recommendations)
- Quality prediction based on raw material grade

---

### Phase 3: Skein Dyeing & QA-2

**Trigger:** Thrown silk lot + dye recipe ready
**Automated Actions:**
- `Master Colorist` inputs Pantone code, spectrophotometer ΔE
- `AI-DYE-DEFECT` runs on swatch images → Delta-E, metamerism flag
- `Skein Dye Master` inputs temp profile, chemical additives
- Guardrail: IF target_machine == 2400_HOOK AND (ΔE > 0.5 OR post-dye tenacity < 3.8 g/d) → BLOCK
- System issues `DYE-LOT-{COLOR_CODE}-{TIMESTAMP}` with status `QA2_CERTIFIED`
- `AI-SCHEDULER` updates dye house queue

**Roles Involved:** Master Colorist, Skein Dye Master, QA Dyeing Inspector

**Key AI/ML:**
- Color consistency prediction (ΔE forecasting)
- Chemical recipe optimization
- Dye batch affinity scoring

---

### Phase 4: Yarn Winding & Preparation

**Trigger:** Dyed skeins + thrown lots ready
**Automated Actions:**
- `Bobbin Winder` / `Pirn Winders` inputs winding speed, jointing method, tension
- Guardrail: IF target_machine == 2400_HOOK AND joint_method == Standard Weaver's Knot → BLOCK (air splicing mandatory)
- Guardrail: IF winding_speed > 180 m/min → BLOCK
- `AI-WARP-DEFECT` validates bobbin/pirn quality
- System generates `BOBBIN-BCH-{SNOWFLAKE}` / `PIRN-BCH-{SNOWFLAKE}`
- 1536/2400 hook validation auto-runs

**Roles Involved:** Bobbin Winder, Pirn Winders, QA Dyeing Inspector

**Key AI/ML:**
- Joint quality classification (ML vision)
- Tension anomaly detection
- Hook count compatibility scoring

---

### Phase 5-6: Jacquard Compilation, Beam Creation & Saree Instantiation

**Trigger:** Design files + wound yarns ready
**Automated Actions:**
- `Design Generator` (GAN) creates new patterns → `DES-{HOOKS}-{PATTERN_HASH}`
- `Graph Drafter` configures hook mapping, weave structure, color separation
- `Card Puncher` compiles CAD-to-CAM, programs controller
- `Warp Beam Preparation` creels 14,400 ends, winds 504m beam
- System auto-generates `WARP-BCH-{LOOM_ID}-{DATE}-{SEQ}`
- **Auto-instantiation:** System spawns 80 child Saree IDs (`{WARP_BATCH_ID}-S01` through `S80`)
- `AI-SCHEDULER` assigns beam to optimal loom based on OEE, skill match
- `AI-AUTO-TRAIN` validates design against defect history

**Roles Involved:** Design Generator, SKU Manager, Graph Drafter, Card Puncher, Warp Beam Preparation

**Key AI/ML:**
- GAN pattern generation with style transfer
- Hook mapping optimization
- Loom assignment PPO
- Defect risk prediction per design

---

### Phase 7: Petni Contrast Splicing & Weaving Execution

**Trigger:** Warp beam + harness ready
**Automated Actions:**
- `Warp Joiner` aligns warp sheet, knots, draws through
- `Petni Master` splices body/border warps
- Guardrail: IF crossed_ends_count > 0 → BLOCK LOOM START
- Guardrail: IF EPI >= 140 AND dropper_wire_weight > 0.3g → BLOCK
- Guardrail: IF warp_tension NOT IN (110-150 cN) → AUTO-PAUSE
- Guardrail: IF section == PALLU_ZARI AND loom_speed > 165 PPM → AUTO-THROTTLE
- `Master Weaver` operates loom, first-pick verification
- `Assistant Weaver` manages shift, breakage alarms
- `SUP Loom Floor Supervisor` monitors floor, releases batch
- `AI-WEAVING-DEFECT` runs real-time on Jacquard acoustic/sensor data
- `AI-SCHEDULER` dynamically adjusts loom speed/priority

**Roles Involved:** Warp Joiner, Petni Master, Loom Harness Setter, Master Weaver, Assistant Weaver, SUP Loom Floor Supervisor

**Key AI/ML:**
- Real-time anomaly detection (acoustic/sensor fusion)
- Dynamic speed optimization (PPO)
- Breakage prediction (time-series)
- Skill-to-loom matching (collaborative filtering)

---

### Phase 8: QA-3 Final Inspection & Grading

**Trigger:** Saree completion (80th pick done)
**Automated Actions:**
- `Quality Inspector` scans saree barcode
- `AI-FABRIC-DEFECT` runs camera grid inspection
- Guardrail: IF length >= 6.30m AND defects == 0 → `GRADE_A_PREMIUM`
- Guardrail: IF length 6.25-6.35m AND defects <= 2 → `GRADE_B`
- Guardrail: IF defects > 2 → `GRADE_C` or `REJECTED_SCRAP`
- Piece-rate penalty auto-triggered (GRADE_A=100%, GRADE_B=20%, GRADE_C/REJECTED=100% frozen)
- Auto B2B order matcher for `GRADE_A_EXPORT_PREMIUM`
- `QA Dyeing Inspector` verifies color consistency (Delta-E > 1.0 → BLOCK)
- `Silk Mark Officer` conducts burn test, XRF Zari audit
- Guardrail: IF synthetic detected → `IMMEDIATE_CERTIFICATION_HALT`
- System generates `SMK-{SNOWFLAKE}` holographic tag
- Certificate hash + QR tag auto-generated

**Roles Involved:** Quality Inspector, QA Dyeing Inspector, Silk Mark Officer, SUP Loom Floor Supervisor

**Key AI/ML:**
- Multi-class defect classification (YOLOv8)
- Grade prediction confidence scoring
- B2B order matching (collaborative filtering)
- Purity verification (spectroscopy + burn test ML)

---

### Phase 9: Finishing, Packaging & Dispatch

**Trigger:** Silk Mark certified + QA cleared
**Automated Actions:**
- `LOG Finishing Transit Specialist` selects finishing machine
- Guardrail: IF Zari == PURE_GOLD_SILVER AND packaging == Standard Polybag → BLOCK
- Guardrail: IF Rotary Roller Calender AND warp_density >= 140 EPI → WARNING
- Anti-tarnish vacuum pack + desiccant auto-verified
- Dispatch manifest auto-generated with weight breakdown
- `consignment_airway_bill_no` triggers inventory state → `IN_TRANSIT_REVENUE`
- Certificate hash + QR tag bound to saree
- `Store Inventory Manager` receives into dispatch warehouse
- `Buy-Back Manager` registers warranty

**Roles Involved:** LOG Finishing Transit Specialist, Store Inventory Manager, Buy-Back Manager

**Key AI/ML:**
- Packaging optimization (ML-based space/cost)
- Carrier selection (cost-time optimization)
- Insurance premium calculation (risk ML)

---

### Phase 10: Inventory Management & Reconciliation

**Trigger:** Dispatch warehouse receipt / inward stock
**Automated Actions:**
- `Store Inventory Manager` scans barcode → auto-bin assignment
- Guardrail: IF dye_lot_mismatch → `DYE_LOT_MIXING_PROHIBITED`
- Guardrail: IF Zari vault climate excursion → `VAULT_ISSUE_HALTED`
- Guardrail: IF quarantine_hold != cleared → `DENY_UNCERTIFIED_STOCK_RELEASE`
- `Smart Reorder Engine` monitors stock → auto-PR if below threshold
- `Anti-Theft Engine` computes variance → alert if >0.2%
- `FEFO Engine` flags near-expiry chemicals
- `AI-SCHEDULER` updates material availability

**Roles Involved:** Store Inventory Manager, Procurement Manager, Finance

**Key AI/ML:**
- Demand-aware reorder point optimization
- Anomaly detection for shrinkage (unsupervised)
- Shelf-life prediction (survival analysis)

---

## 4. Cross-Module AI-Native Feature Matrix

### 4.1 Finance Module

| Feature | AI/ML Component | Automation | Roles |
|---|---|---|---|
| **Supplier Payout Automation** | Predictive cash flow ML | Auto-release on QA-1/QA-2 certification | System Admin, Finance |
| **Piece-Rate Wage Calculation** | ML-based yield prediction | Auto-calc per weaver shift | Master Weaver, Assistant Weaver, SUP Loom Floor Supervisor, Store Inventory Manager |
| **B2B Invoice Generation** | Dynamic pricing ML | Auto-generate on dispatch | LOG Finishing Transit Specialist, Finance |
| **Procurement PR Automation** | Demand forecasting | Auto-create PR when stock < threshold | Store Inventory Manager, Procurement |
| **Tax & GST Compliance** | Rule engine + ML | Auto-calculate, file returns | Finance, System Admin |
| **Buy-Back Reserve Provisioning** | Risk prediction ML | Auto-set aside reserves | Buy-Back Manager, Finance |
| **Fraud Detection** | Anomaly detection | Real-time alert on suspicious transactions | System Admin, Finance |
| **Working Capital Optimization** | Time-series forecasting | Suggest optimal payment cycles | Finance |

### 4.2 Manufacturing Module

| Feature | AI/ML Component | Automation | Roles |
|---|---|---|---|
| **Loom Scheduling (PPO)** | Reinforcement learning | Auto-assign beams to looms | SUP Loom Floor Supervisor, Master Weaver |
| **Yield Prediction** | Regression models | Predict saree count per beam | Warp Beam Preparation, Master Weaver |
| **Defect Prevention** | Real-time sensor ML | Auto-pause loom on anomaly | Master Weaver, Assistant Weaver, Loom Harness Setter |
| **Process Optimization** | Multi-armed bandit | Suggest optimal temp/speed/TPI | Silk Degumming Master, Skein Dye Master, Throwster/Twister |
| **Maintenance Prediction** | Time-series anomaly | Alert before loom failure | Loom Harness Setter, IoT Device Manager |
| **Energy Optimization** | RL + IoT data | Auto-adjust climate controls | IoT Device Manager, SUP Loom Floor Supervisor |
| **Quality Prediction** | Classification models | Predict grade before weaving | Quality Inspector, QA Dyeing Inspector |
| **Design Recommendation** | GAN + collaborative filtering | Suggest designs per SKU | Design Generator, Graph Drafter, SKU Manager |
| **Waste Minimization** | Optimization ML | Auto-calculate optimal wastage | Warp Beam Preparation, Petni Master |

### 4.3 Supply Chain Module

| Feature | AI/ML Component | Automation | Roles |
|---|---|---|---|
| **Demand Forecasting** | Time-series + festival ML | 30/60/90-day forecast | SKU Manager, Sales, Store Inventory Manager |
| **Smart Reorder** | Demand-aware ML | Auto-PR when stock < threshold | Store Inventory Manager, Procurement |
| **Route Optimization** | Graph ML | Suggest best logistics carrier | LOG Finishing Transit Specialist |
| **Inventory Shrinkage Detection** | Anomaly detection | Real-time alert on variance | Store Inventory Manager, Finance |
| **Supplier Risk Scoring** | Classification ML | Auto-score new suppliers | Filature Supplier, Procurement, System Admin |
| **Lot Matching** | Constraint satisfaction | Enforce single-dye-lot protocol | Store Inventory Manager, Master Weaver |
| **Cold Chain Monitoring** | IoT + threshold | Auto-alert on climate excursion | Store Inventory Manager, IoT Device Manager |
| **Traceability (<15ms)** | Neo4j + Redis cache | Full lineage on barcode scan | All roles |

### 4.4 HR Module

| Feature | AI/ML Component | Automation | Roles |
|---|---|---|---|
| **Weaver Skill Matching** | Collaborative filtering | Match weaver to loom/SKU | Guild Manager, SUP Loom Floor Supervisor, Master Weaver |
| **Guild Performance Analytics** | Clustering + regression | Identify top performers, training needs | Guild Manager |
| **Payroll Automation** | ML-based piece-rate | Auto-calc wages per shift | Master Weaver, Assistant Weaver, Finance |
| **Attendance & Shift Scheduling** | Optimization ML | Auto-schedule based on demand | SUP Loom Floor Supervisor, Guild Manager |
| **Training Recommendation** | Content-based filtering | Suggest courses per skill gap | Guild Manager, Master Weaver |
| **Contractor Onboarding** | NLP + OCR | Auto-verify documents, create user | System Admin, Guild Manager |
| ** attrition Prediction** | Classification ML | Flag at-risk weavers | Guild Manager, HR |
| **Multi-language Payslips** | TTS + i18n | Auto-generate in local language | Finance, Localization Manager |

### 4.5 Procurement Module

| Feature | AI/ML Component | Automation | Roles |
|---|---|---|---|
| **Supplier Selection** | Multi-criteria decision ML | Rank suppliers by quality/price/delivery | Procurement, Filature Supplier |
| **Purchase Order Automation** | Demand forecast + reorder ML | Auto-create PO when PR approved | Procurement, Store Inventory Manager |
| **Invoice Matching** | OCR + NLP | Auto-match PO/GRN/invoice | Finance, Procurement |
| **Price Forecasting** | Time-series ML | Predict raw silk/Zari price trends | Procurement, Finance |
| **Contract Renewal Alerts** | Rule engine + ML | Alert 60 days before expiry | Procurement, System Admin |
| **Quality-linked Payment** | Smart contract | Release payment only on QA certification | Finance, System Admin |
| **Supplier Diversity Tracking** | Classification | Track women/SC/ST/OBC vendors | Guild Manager, Procurement |
| **Sustainability Scoring** | Multi-criteria ML | Score suppliers on environmental metrics | Procurement, System Admin |

---

## 5. Role-to-Feature Mapping Matrix

### 5.1 Full Feature Assignment by Role

| Role ID | Role Name | Core Features | AI/ML Features | Automation Triggers | ERP Modules |
|---|---|---|---|---|---|
| `ROLE-SYSTEM-ADMIN` | System/Admin Superuser | User management, role config, audit trails, factory node setup | All AI services deployment, model retraining triggers | All workflows, all guardrails | All |
| `ROLE-ASSISTANT-WEAVER` | Assistant Weaver | Shift logs, breakage alarms, wage splits, loom assistance | `AI-WEAVING-DEFECT` (real-time alerts), skill progression ML | Auto-shift handover, auto-wage calc | Manufacturing, HR |
| `ROLE-BOBBIN-WINDER` | Bobbin Winder | Bobbin build, quality audit, 1536/2400 hook validation | `AI-WARP-DEFECT` (bobbin quality), hook compatibility scoring | Auto-certification on pass, auto-route to Warp Beam Prep | Manufacturing, Supply Chain |
| `ROLE-CARD-PUNCHER` | Card Puncher | CAD-to-CAM compilation, controller programming, verification/simulation | `AI-AUTO-TRAIN` (pattern validation), design defect prediction | Auto-certification, auto-deploy to loom | Manufacturing, Design |
| `ROLE-FILATURE-SUPPLIER` | Filature Supplier | Raw silk intake, GRN logging, supplier payout | `AI-ZARI-DEFECT` (raw quality), supplier risk scoring | Auto-payout on QA-1 cert, auto-reorder trigger | Supply Chain, Finance, Procurement |
| `ROLE-GRAPH-DRAFTER` | Graph Drafter | Jacquard graph design, hook mapping, weave structure, color separation | `AI-GAN-DESIGN` (pattern suggestions), design defect prediction | Auto-certification, auto-route to Card Puncher | Manufacturing, Design |
| `ROLE-LOG-FINISHING` | LOG Finishing Transit Specialist | Finishing ops, anti-tamper barcoding, moisture-controlled packaging, B2B consolidation, freight forwarding | Packaging optimization ML, carrier selection ML, insurance premium calc | Auto-manifest generation, auto-inventory state transition, auto-certification | Manufacturing, Supply Chain, Finance |
| `ROLE-LOOM-HARNESS-SETTER` | Loom Harness Setter | Harness assembly, comber board positioning, mail eye leveling, shed alignment | `AI-WEAVING-DEFECT` (shed quality), tension prediction ML | Auto-certification, auto-route to Master Weaver | Manufacturing |
| `ROLE-MASTER-COLORIST` | Master Colorist | Shade matching, chemical recipe formulation, spectrophotometer QA, recipe approval | `AI-DYE-DEFECT` (color consistency), recipe optimization ML, ΔE forecasting | Auto-certification, auto-route to Skein Dye Master | Manufacturing, Supply Chain |
| `ROLE-MASTER-WEAVER` | Master Weaver | Loom operation, quality troubleshooting, material allocation, yield calc, first-pick verification | `AI-WEAVING-DEFECT` (real-time), yield prediction ML, skill-to-loom matching | Auto-yield calc, auto-wage split, auto-trigger QA-3 | Manufacturing, HR |
| `ROLE-PETNI-MASTER` | Petni Master | Petni warp transition, reed denting, dropper pinning, lease verification | `AI-WARP-DEFECT` (reed quality), contrast inventory depletion prediction | Auto-certification, auto-route to Master Weaver | Manufacturing, Supply Chain |
| `ROLE-PIRN-WINDERS` | Pirn Winders | Weft pirn winding, taper control, density monitoring, quality audit | `AI-WARP-DEFECT` (pirn quality), density anomaly detection | Auto-certification, auto-route to Master Weaver | Manufacturing, Supply Chain |
| `ROLE-QA-DYEING-INSPECTOR` | QA Dyeing Inspector | Post-dye color consistency, Delta-E measurement, color fastness, defect classification | `AI-DYE-DEFECT` (automated ΔE, fastness prediction), B2B order matcher | Auto-certification, auto-trigger Silk Mark, auto-piece-rate penalty | Manufacturing, Quality, Sales |
| `ROLE-QUALITY-INSPECTOR` | Quality Inspector | Finished saree inspection, dimensional audit, defect classification, commercial grading | `AI-FABRIC-DEFECT` (automated inspection), grade prediction ML, B2B auto-matcher | Auto-grade, auto-piece-rate penalty, auto-trigger next role | Manufacturing, Quality, Sales |
| `ROLE-SILK-DEGUMMING-MASTER` | Silk Degumming Master | Thermal-chemical process control, sericin loss, certification | Process optimization ML, quality prediction from raw grade | Auto-certification, auto-route to Throwster | Manufacturing, Supply Chain |
| `ROLE-SILK-GRADER` | Silk Grader | Raw silk grading, A/B/C classification, neatness/tenacity testing | ML-based grade prediction from images, auto-sorting | Auto-certification, auto-route to Degumming/Inventory | Supply Chain, Manufacturing |
| `ROLE-SILK-MARK-OFFICER` | Silk Mark Officer | Burn test, chemical solubility, XRF Zari audit, hologram tag serialization, anti-adulteration lock | Spectroscopy ML (purity verification), counterfeit detection, serialization uniqueness check | Auto-certification, auto-block on failure, auto-trigger Finishing | Quality, Compliance, Manufacturing |
| `ROLE-SKEIN-DYE-MASTER` | Skein Dye Master | Physical dye bath execution, temperature profiling, chemical additives, post-dye QA | `AI-DYE-DEFECT` (temp/chemical optimization), mass balance ML | Auto-certification, auto-route to QA Dyeing Inspector | Manufacturing, Supply Chain |
| `ROLE-STORE-INVENTORY-MANAGER` | Store Inventory Manager | Inward stock placement, outward requisition, lot segregation, climate control, vault management | Smart reorder ML, shrinkage anomaly detection, FEFO automation, demand-aware bin optimization | Auto-reorder PR, auto-quarantine, auto-state transition, back-trace <15ms | Supply Chain, Finance, Procurement |
| `ROLE-SUP-LOOM-FLOOR-SUPERVISOR` | SUP Loom Floor Supervisor | Floor monitoring, environmental control, quality escapes, root cause audits, batch release, shift handover | `AI-SCHEDULER` (production optimization), `AI-WEAVING-DEFECT` (floor-wide monitoring), predictive downtime | Auto-batch release, auto-shift handover, auto-root-cause escalation | Manufacturing, HR, Quality |
| `ROLE-THROWSTER-TWISTER` | Throwster/Twister | Ply doubling, TPI control, twist setting/steaming, certification | Process optimization ML, TPI/ply quality prediction | Auto-certification, auto-route to Bobbin/Pirn/Master Colorist | Manufacturing, Supply Chain |
| `ROLE-WARP-BEAM-PREPARATION` | Warp Beam Preparation | Sectional warping, creel tension sync, beaming-off, mass balance, 80-saree instantiation | `AI-WARP-DEFECT` (tension/ends monitoring), yield prediction, optimal creel ML | Auto-80-saree instantiation, auto-certification, auto-route to Warp Joiner | Manufacturing, Supply Chain |
| `ROLE-WARP-JOINER` | Warp Joiner | Warp sheet alignment, precision knotting, drawing-through, lease correction | `AI-WARP-DEFECT` (knot quality), tension prediction ML | Auto-certification, auto-route to Harness Setter/Petni Master | Manufacturing |
| `ROLE-ZARI-INSPECTOR` | Zari Inspector | Zari assay, XRF audit, purity verification, refinery certification | `AI-ZARI-DEFECT` (purity scoring), XRF ML model, counterfeit detection | Auto-certification, auto-route to Store Inventory/Throwster | Supply Chain, Quality |
| `ROLE-DESIGN-GENERATOR` | Design Generator | GAN-based Jacquard design, pattern generation, style transfer, sales feedback loop | GAN models, style transfer ML, trend prediction, sales-feedback RL | Auto-certification, auto-route to Graph Drafter/SKU Manager | Design, Sales |
| `ROLE-BUY-BACK-MANAGER` | Buy-Back Manager | Buy-back risk assessment, warranty claims, refund processing | `AI-BUYBACK-RISK` (risk scoring), claim fraud detection | Auto-risk assessment, auto-claim approval (low risk), auto-escalation (high risk) | Finance, Quality, Sales |
| `ROLE-GUILD-MANAGER` | Guild Manager | Guild certification, weaver skill verification, payment splits, incentive distribution | Skill progression ML, performance clustering, demand-aware scheduling | Auto-skill certification, auto-payment split, auto-incentive calc | HR, Manufacturing, Finance |
| `ROLE-IOT-DEVICE-MANAGER` | IoT Device Manager | Edge controller management, loom telemetry, edge AI model deployment, MQTT monitoring | Anomaly detection on telemetry, predictive maintenance ML, model drift detection | Auto-alert on device failure, auto-model update, auto-telemetry routing | Manufacturing, All |
| `ROLE-LOCALIZATION-MANAGER` | Localization Manager | Multi-language translation (Telugu/Tamil/Kannada/Hindi/Bengali), TTS audio generation, UI localization | `AI-TTS` (neural TTS), MT quality scoring, context-aware translation | Auto-translation on content update, auto-TTS generation, auto-UI language switch | All |
| `ROLE-SKU-MANAGER` | SKU Manager | SKU definition, product catalog, B2B order linkage, design-to-SKU mapping | `AI-DEMAND-FCAST` (SKU-level forecast), design recommendation ML | Auto-SKU creation from design, auto-B2B linkage, auto-forecast update | Design, Sales, Supply Chain |

---

## 6. Detailed Feature Breakdown by ERP Module

### 6.1 Finance Module

#### 6.1.1 Supplier Payout Automation
- **Trigger:** QA-1 or QA-2 certification issued
- **Automated Flow:**
  1. System captures conditioned weight at certification
  2. ML model predicts final yield (based on raw material grade, process parameters)
  3. Smart contract releases % payout to supplier wallet
  4. Remaining % held as quality guarantee (released after 30 days if no buy-back)
- **AI/ML:** Yield prediction regression, fraud detection anomaly scoring
- **Roles:** System Admin, Finance, Filature Supplier

#### 6.1.2 Piece-Rate Wage Automation
- **Trigger:** Saree grade finalized by Quality Inspector
- **Automated Flow:**
  1. System reads grade (A/B/C/REJECTED)
  2. Applies piece-rate: GRADE_A=100%, GRADE_B=80% (20% penalty), GRADE_C/REJECTED=0% (frozen)
  3. ML model splits wage across Master Weaver, Assistant Weaver, Petni Master based on shift data
  4. Auto-posts to payroll, generates digital payslip
- **AI/ML:** Wage split optimization, performance clustering
- **Roles:** Master Weaver, Assistant Weaver, Petni Master, Guild Manager, Finance

#### 6.1.3 B2B Invoice & Payment Automation
- **Trigger:** LOG Finishing certifies + dispatch manifest complete
- **Automated Flow:**
  1. System auto-generates encrypted B2B packing list PDF
  2. ML model suggests dynamic pricing (based on grade, design complexity, market demand)
  3. Invoice auto-sent to buyer via API
  4. Payment gateway integration auto-reconciles
- **AI/ML:** Dynamic pricing RL, fraud detection
- **Roles:** LOG Finishing Transit Specialist, Finance, SKU Manager

#### 6.1.4 Procurement PR Automation
- **Trigger:** Stock level falls below ML-predicted reorder point
- **Automated Flow:**
  1. Smart Reorder Engine monitors inventory in real-time
  2. ML model predicts demand for next 30 days (considering festivals, B2B orders)
  3. Auto-generates Purchase Requisition with suggested quantity, supplier, price
  4. Auto-routes to Procurement Manager for approval
- **AI/ML:** Demand forecasting, reorder point optimization, supplier selection ML
- **Roles:** Store Inventory Manager, Procurement Manager, System Admin

#### 6.1.5 Buy-Back Reserve Provisioning
- **Trigger:** Monthly batch
- **Automated Flow:**
  1. `AI-BUYBACK-RISK` scores all dispatched sarees
  2. Finance module auto-sets aside reserve % per risk tier
  3. Auto-adjusts quarterly financial statements
- **AI/ML:** Risk scoring classification, time-series loss forecasting
- **Roles:** Buy-Back Manager, Finance, System Admin

---

### 6.2 Manufacturing Module

#### 6.2.1 Loom Scheduling (PPO)
- **Trigger:** Warp beam certification issued
- **Automated Flow:**
  1. `AI-SCHEDULER` (PPO) receives beam + design + loom availability
  2. Considers: OEE history, weaver skill match, order priority, festival demand
  3. Auto-assigns beam to optimal loom
  4. Auto-notifies Master Weaver + Assistant Weaver
- **AI/ML:** PPO reinforcement learning, collaborative filtering (skill matching)
- **Roles:** SUP Loom Floor Supervisor, Master Weaver, Assistant Weaver, IoT Device Manager

#### 6.2.2 Yield Prediction
- **Trigger:** Warp beam creation
- **Automated Flow:**
  1. ML model trained on historical beam data (yarn grade, design complexity, loom type)
  2. Predicts expected saree count, defect rate, grade distribution
  3. Updates production forecast and material requirements
- **AI/ML:** Regression (XGBoost/LightGBM), uncertainty quantification
- **Roles:** Warp Beam Preparation, Master Weaver, SUP Loom Floor Supervisor

#### 6.2.3 Real-Time Defect Prevention
- **Trigger:** Active weaving (every pick)
- **Automated Flow:**
  1. `AI-WEAVING-DEFECT` (ONNX) runs on edge controller
  2. Analyzes Jacquard acoustic + vibration + tension sensor data
  3. IF anomaly score > threshold → AUTO-PAUSE loom
  4. Alert sent to Master Weaver + SUP Loom Floor Supervisor
  5. Root cause suggestion from ML model (tension? thread break? design error?)
- **AI/ML:** Real-time anomaly detection (Isolation Forest + LSTM), multi-modal fusion
- **Roles:** Master Weaver, Assistant Weaver, SUP Loom Floor Supervisor, IoT Device Manager

#### 6.2.4 Process Optimization
- **Trigger:** Process parameter change (temp, TPI, pH, etc.)
- **Automated Flow:**
  1. Multi-armed bandit algorithm explores parameter space
  2. Recommends optimal settings based on historical yield/quality
  3. Auto-applies within safety bounds
  4. Logs outcome for continuous improvement
- **AI/ML:** Multi-armed bandit, Bayesian optimization
- **Roles:** Silk Degumming Master, Skein Dye Master, Throwster/Twister, Master Colorist

#### 6.2.5 Maintenance Prediction
- **Trigger:** Continuous loom telemetry
- **Automated Flow:**
  1. `IoT Device Manager` streams vibration, temperature, motor current
  2. `AI-AUTO-TRAIN` model predicts component failure
  3. Auto-creates maintenance work order 72 hours before predicted failure
  4. Auto-schedules during next low-priority beam
- **AI/ML:** Time-series forecasting (Prophet/LSTM), survival analysis
- **Roles:** Loom Harness Setter, IoT Device Manager, SUP Loom Floor Supervisor

---

### 6.3 Supply Chain Module

#### 6.3.1 Demand Forecasting
- **Trigger:** Daily batch
- **Automated Flow:**
  1. `AI-DEMAND-FCAST` ingests: historical sales, B2B orders, festival calendar, trend data
  2. Forecasts demand at SKU + design + color level for 30/60/90 days
  3. Auto-creates production plan → triggers material requisitions
  4. Auto-updates `sales_forecast_*_plan` tables for each role
- **AI/ML:** Temporal fusion transformer, ensemble methods
- **Roles:** SKU Manager, Store Inventory Manager, Sales (external), SUP Loom Floor Supervisor

#### 6.3.2 Smart Reorder
- **Trigger:** Continuous inventory monitoring
- **Automated Flow:**
  1. Smart Reorder Engine calculates dynamic reorder point (not static)
  2. Considers: forecasted demand, lead time, supplier reliability, seasonality
  3. WHEN stock < reorder_point → auto-generate PR with optimal quantity
  4. Auto-suggests supplier based on risk score + price + lead time
- **AI/ML:** Demand-aware inventory optimization, supplier ranking ML
- **Roles:** Store Inventory Manager, Procurement Manager

#### 6.3.3 Route Optimization
- **Trigger:** Dispatch manifest complete
- **Automated Flow:**
  1. `LOG Finishing Transit Specialist` enters consignment details
  2. ML model evaluates: cost, transit time, customs complexity, carrier reliability
  3. Suggests optimal carrier (DHL/FedEx/Bluedart/Local)
  4. Auto-generates airway bill + customs docs
- **AI/ML:** Graph-based route optimization, cost-time trade-off RL
- **Roles:** LOG Finishing Transit Specialist

#### 6.3.4 Shrinkage Detection
- **Trigger:** Daily reconciliation batch
- **Automated Flow:**
  1. `Reconciliation Engine` compares physical count vs theoretical stock
  2. Anomaly detection ML flags items with >0.2% variance
  3. Auto-locks warehouse ledger row
  4. Escalates security alert to internal auditor + Finance
- **AI/ML:** Unsupervised anomaly detection (Isolation Forest), autoencoder
- **Roles:** Store Inventory Manager, Finance, System Admin

---

### 6.4 HR Module

#### 6.4.1 Weaver Skill Matching
- **Trigger:** New beam assignment needed
- **Automated Flow:**
  1. `Guild Manager` maintains skill matrix per weaver (design type, hook count, defect rate)
  2. Collaborative filtering ML matches weaver to optimal beam/SKU
  3. Considers: availability, historical yield, quality grade, location
  4. Auto-suggests assignment to SUP Loom Floor Supervisor
- **AI/ML:** Collaborative filtering, matrix factorization
- **Roles:** Guild Manager, SUP Loom Floor Supervisor, Master Weaver

#### 6.4.2 Payroll Automation
- **Trigger:** Saree grade finalized + shift data complete
- **Automated Flow:**
  1. Piece-rate engine calculates base wage per grade
  2. ML model splits across weaver, assistant, petni based on shift logs
  3. Auto-deducts penalties (defect-caused rework)
  4. Auto-generates payslip in local language (TTS + i18n)
  5. Auto-posts to bank via UPI/IMPS
- **AI/ML:** Wage split optimization, fraud detection
- **Roles:** Master Weaver, Assistant Weaver, Guild Manager, Finance

#### 6.4.3 Attendance & Shift Scheduling
- **Trigger:** Weekly batch
- **Automated Flow:**
  1. Optimization ML considers: demand forecast, weaver availability, skill requirements, festival overtime rules
  2. Auto-generates optimal shift schedule
  3. Auto-notifies weavers via SMS/voice call (TTS)
  4. Auto-adjusts for absenteeism (real-time rescheduling)
- **AI/ML:** Constraint satisfaction, RL for dynamic rescheduling
- **Roles:** SUP Loom Floor Supervisor, Guild Manager

---

### 6.5 Procurement Module

#### 6.5.1 Supplier Selection
- **Trigger:** PR approved
- **Automated Flow:**
  1. ML model scores all qualified suppliers on: price, quality history, delivery reliability, sustainability, diversity
  2. Ranks suppliers with confidence intervals
  3. Auto-suggests top 3 to Procurement Manager
  4. Auto-creates PO on approval
- **AI/ML:** Multi-criteria decision analysis, ranking SVM
- **Roles:** Procurement Manager, System Admin, Store Inventory Manager

#### 6.5.2 Invoice Matching
- **Trigger:** Invoice received (OCR scan or email)
- **Automated Flow:**
  1. NLP + OCR extracts: PO number, quantity, price, supplier
  2. Auto-matches against PO + GRN in CockroachDB
  3. IF match → auto-posts to Finance
  4. IF mismatch → auto-flags for manual review with discrepancy details
- **AI/ML:** NLP entity extraction, similarity scoring
- **Roles:** Finance, Procurement, System Admin

---

## 7. Complete Role Profile: Enterprise Feature Assignments

### 7.1 Role 1: ROLE-SYSTEM-ADMIN

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Finance | Payout config, tax rules, chart of accounts | Model deployment triggers | All workflows | None | All |
| Manufacturing | Loom config, guardrail rules, model deployment | AI service orchestration | All guardrails | None | All |
| Supply Chain | Factory node setup, route config | Demand forecast integration | All workflows | None | All |
| HR | User creation, role assignment, guild config | Skill model training | Onboarding automation | None | All |
| Procurement | Supplier scoring config, PR approval limits | Supplier ML model training | PO automation | None | All |

**Key Dashboards:**
- `DashboardSystemAdmin` — Factory overview, AI model health, user management, audit trails
- Cross-module: All admin functions accessible via unified admin panel

---

### 7.2 Role 2: ROLE-ASSISTANT-WEAVER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Shift logs, breakage alarms, loom assistance | `AI-WEAVING-DEFECT` real-time alerts | Auto-shift handover, auto-wage calc | Master Weaver job assignment | Shift audit, wage distribution |
| HR | Attendance, skill progression tracking | Skill gap ML | Auto-payslip generation | Guild Manager certification | Shift completion |
| Quality | Defect reporting, first-pick verification | Defect classification assist | Auto-escalation to QA | Master Weaver approval | QA-3 trigger |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Master Weaver:** Approved job cards, beam assignments
- **Tab 2 — Loom Operations:** Real-time defect alerts, pick counter, speed monitor
- **Tab 3 — Shift & Wage:** Shift logs, breakage alarms, wage split preview
- **Tab 4 — Certificates:** Shift audit certificates, wage distribution proof

---

### 7.3 Role 3: ROLE-BOBBIN-WINDER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Bobbin build, quality audit, hook validation | `AI-WARP-DEFECT` bobbin quality | Auto-certification on pass | Throwster/Twister thrown silk | Bobbin batch cert, auto-route to Warp Beam Prep |
| Supply Chain | Inventory issue/return | Hook compatibility scoring | Auto-bin assignment | Store Inventory Manager issue | Stock update |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Throwster/Twister:** Thrown silk lot details, TPI/ply info
- **Tab 2 — Bobbin Winding:** Machine setup, tension settings, joint quality
- **Tab 3 — Quality Audit:** Hook validation results, defect images
- **Tab 4 — Certificates:** Bobbin batch certificates, hook count proof

---

### 7.4 Role 4: ROLE-CARD-PUNCHER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | CAD-to-CAM, controller programming, simulation | `AI-AUTO-TRAIN` pattern validation | Auto-certification, auto-deploy | Graph Drafter design file | Card punch cert, auto-route to Loom |
| Design | Design file validation, hook mapping check | Design defect prediction | Auto-simulation pass/fail | SKU Manager / Design Generator | Design deployment |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Graph Drafter:** Design file preview, hook mapping
- **Tab 2 — Programming:** CAD editor, controller config, simulation
- **Tab 3 — Verification:** Simulation results, hook validation, error detection
- **Tab 4 — Certificates:** Programming certificates, deployment proof

---

### 7.5 Role 5: ROLE-FILATURE-SUPPLIER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Supply Chain | GRN logging, raw lot creation, supplier payout | `AI-ZARI-DEFECT` raw quality, supplier risk scoring | Auto-payout on QA-1 cert, auto-reorder trigger | External delivery | Raw lot cert, auto-route to QC/Gate |
| Finance | Payment tracking, payout history | Yield prediction ML | Smart contract payout | Gate Clerk scan | Payment release |
| Procurement | Supplier scorecard, diversity tracking | Supplier ranking ML | Auto-PR on low stock | Procurement PR | Material delivery |

**Key Dashboards:**
- **Tab 1 — Pre-Process (External):** Delivery schedule, PO details
- **Tab 2 — Intake & GRN:** Bale count, scale weight, supplier ID, barcode generation
- **Tab 3 — Quality & Certification:** QA-1 results, conditioned weight, payout status
- **Tab 4 — Payout & History:** Payment ledger, grade history, supplier scorecard

---

### 7.6 Role 6: ROLE-GRAPH-DRAFTER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Jacquard graph design, hook mapping, weave structure | `AI-GAN-DESIGN` suggestions, design defect prediction | Auto-certification, auto-route to Card Puncher | Design Generator / SKU Manager | Design cert, hook mapping proof |
| Design | Color separation, card-punch coding | Style transfer ML | Auto-simulation pass/fail | Sales forecast, trend data | Design deployment |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Design Generator/SKU Manager:** SKU brief, trend data, design suggestions
- **Tab 2 — Graph Design:** Hook mapping editor, weave structure, color separation
- **Tab 3 — Simulation & QA:** Simulation results, defect prediction, hook validation
- **Tab 4 — Certificates:** Design certificates, hook count proof, deployment status

---

### 7.7 Role 7: ROLE-LOG-FINISHING

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Finishing ops, calendering, edge fringing, packaging | Packaging optimization ML, carrier selection ML | Auto-manifest, auto-certification, auto-inventory transition | QA Inspector, QA Dyeing Inspector, Silk Mark Officer | Dispatch cert, auto-route to Store Inventory |
| Supply Chain | B2B consolidation, freight forwarding | Insurance premium calc ML | Auto-AWB generation, auto-tracking | B2B Sales Order | Shipment tracking |
| Finance | Invoice generation, payment tracking | Dynamic pricing ML | Auto-invoice on dispatch | Store Inventory Manager receipt | B2B payment |

**Key Dashboards:**
- **Tab 1 — Pre-Process from QA/Silk Mark:** Approved lots, grade details, Silk Mark tag status
- **Tab 2 — Finishing & Packaging:** Machine selection, temp control, packaging audit
- **Tab 3 — Dispatch Release:** Manifest generation, carrier selection, AWB tracking
- **Tab 4 — Certificates:** Finishing certificates, packaging proof, dispatch status

---

### 7.8 Role 8: ROLE-LOOM-HARNESS-SETTER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Harness assembly, comber board, mail eye, shed alignment | `AI-WEAVING-DEFECT` shed quality, tension prediction | Auto-certification, auto-route to Master Weaver | Warp Joiner, Warp Beam Prep | Harness cert, shed alignment proof |
| Quality | Preventive maintenance tracking | Maintenance prediction ML | Auto-work order on anomaly | Loom Harness Setter history | Maintenance log |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Warp Joiner/Warp Beam Prep:** Warp details, beam specs, design requirements
- **Tab 2 — Harness Setup:** Comber board config, mail eye leveling, lingo weight
- **Tab 3 — Shed Alignment & QA:** Alignment verification, tension check, simulation
- **Tab 4 — Certificates:** Harness certificates, maintenance history

---

### 7.9 Role 9: ROLE-MASTER-COLORIST

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Shade matching, recipe formulation, spectrophotometer QA | `AI-DYE-DEFECT` color consistency, recipe optimization ML, ΔE forecasting | Auto-certification, auto-route to Skein Dye Master | Throwster/Twister, Silk Degumming Master | Recipe cert, shade approval |
| Supply Chain | Dye lot matching, color archive | Color similarity ML | Auto-lot matching, auto-recipe retrieval | Sales forecast, design requirements | Dye lot creation |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Throwster/Degumming:** Yarn details, previous dye history
- **Tab 2 — Shade Matching:** Spectrophotometer input, Pantone matching, Delta-E calc
- **Tab 3 — Recipe Formulation:** Chemical recipe, temp profile, fastness prediction
- **Tab 4 — Certificates:** Recipe certificates, shade approval, Dye Lot linkage

---

### 7.10 Role 10: ROLE-MASTER-WEAVER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Loom operation, quality troubleshooting, material allocation, yield calc | `AI-WEAVING-DEFECT` real-time, yield prediction ML, skill-to-loom matching | Auto-yield calc, auto-wage split, auto-trigger QA-3 | Warp Joiner, Petni Master, Loom Harness Setter | Weaving cert, auto-route to QA |
| HR | Shift management, assistant weaver assignment | Performance ML | Auto-shift handover, auto-wage calc | SUP Loom Floor Supervisor batch release | Shift audit |
| Quality | First-pick verification, defect troubleshooting | Defect classification assist | Auto-escalation to SUP | Quality Inspector pre-approval | QA-3 trigger |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Warp Joiner/Petni/Harness:** Warp sheet details, harness setup, border splice
- **Tab 2 — Loom Operations:** Real-time pick counter, speed, tension, AI defect alerts
- **Tab 3 — Yield & Quality:** Yield prediction, first-pick verification, defect log
- **Tab 4 — Certificates:** Weaving certificates, yield proof, wage distribution

---

### 7.11 Role 11: ROLE-PETNI-MASTER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Petni warp transition, reed denting, dropper pinning, lease verification | `AI-WARP-DEFECT` reed quality, contrast inventory depletion prediction | Auto-certification, auto-route to Master Weaver | Warp Joiner, Store Inventory Manager | Petni cert, auto-deplete contrast yarn |
| Supply Chain | Contrast yarn inventory management | Demand prediction ML | Auto-requisition, auto-bin update | Store Inventory Manager issue | Stock reconciliation |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Warp Joiner:** Warp sheet alignment, knot quality
- **Tab 2 — Petni Setup:** Reed denting, dropper pinning, lease verification
- **Tab 3 — Border Splicing:** Contrast yarn splice, alignment check
- **Tab 4 — Certificates:** Petni certificates, contrast inventory depletion proof

---

### 7.12 Role 12: ROLE-PIRN-WINDERS

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Weft pirn winding, taper control, density monitoring, quality audit | `AI-WARP-DEFECT` pirn quality, density anomaly detection | Auto-certification, auto-route to Master Weaver | Throwster/Twister, Store Inventory Manager | Pirn batch cert |
| Supply Chain | Inventory issue/return | Weft density optimization ML | Auto-bin assignment | Store Inventory Manager issue | Stock update |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Throwster/Twister:** Yarn details, TPI, ply count
- **Tab 2 — Pirn Winding:** Machine setup, taper control, density settings
- **Tab 3 — Quality Audit:** Density monitoring, hook validation, defect images
- **Tab 4 — Certificates:** Pirn batch certificates, density proof

---

### 7.13 Role 13: ROLE-QA-DYEING-INSPECTOR

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Post-dye color consistency, Delta-E measurement, fastness testing, defect classification | `AI-DYE-DEFECT` automated ΔE, fastness prediction, B2B order matcher | Auto-certification, auto-piece-rate penalty, auto-trigger Silk Mark | Quality Inspector, Skein Dye Master, Master Colorist | Dye cert, auto-route to Silk Mark |
| Quality | Color fastness grading, shade variation detection | Color consistency ML | Auto-grade, auto-penalty | QA-2 certification | QA-3 trigger |
| Sales | B2B order matching for GRADE_A | Collaborative filtering ML | Auto-match to B2B orders | Sales forecast, B2B orders | Order allocation |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Quality Inspector/Skein Dye Master:** Approved fabric logs, dye lot details
- **Tab 2 — Color QA:** Delta-E measurement, fastness testing, defect classification
- **Tab 3 — Certificates:** Dye certificates, color consistency proof, B2B order match
- **Tab 4 — Sales Forecast:** Material requirements, upcoming lots

---

### 7.14 Role 14: ROLE-QUALITY-INSPECTOR

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Finished saree inspection, dimensional audit, defect classification, commercial grading | `AI-FABRIC-DEFECT` automated inspection, grade prediction ML, B2B auto-matcher | Auto-grade (A/B/C/REJECTED), auto-piece-rate penalty, auto-trigger QA Dyeing/Silk Mark | Master Weaver, Assistant Weaver | QA cert, auto-route to next role |
| Quality | Defect categorization, dimensional compliance | Multi-class defect classification | Auto-block on failure, auto-escalation | Loom completion | QA-3 trigger |
| Sales | B2B order matching for GRADE_A_EXPORT_PREMIUM | Collaborative filtering ML | Auto-match to premium B2B orders | Sales forecast, B2B orders | Order allocation |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Master Weaver/Assistant Weaver:** Weaving logs, yield data, defect history
- **Tab 2 — Dimensional Audit:** Length, width, border symmetry, EPI/PPI measurement
- **Tab 3 — Defect Classification:** AI-assisted defect detection, grading, piece-rate calc
- **Tab 4 — Certificates:** QA certificates, grade proof, B2B order match

---

### 7.15 Role 15: ROLE-SILK-DEGUMMING-MASTER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Thermal-chemical process control, sericin loss, certification | Process optimization ML, quality prediction from raw grade | Auto-certification, auto-route to Throwster/Twister | Filature Supplier, Store Inventory Manager | Degumming cert, auto-route downstream |
| Supply Chain | Inventory issuance/return | Yield prediction ML | Auto-bin update, auto-requisition | Store Inventory Manager issue | Stock reconciliation |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Filature Supplier/Store Inventory:** Raw silk lot details, storage conditions
- **Tab 2 — Degumming Process:** Alkali pH, temperature, sericin loss, process parameters
- **Tab 3 — Quality Audit:** Post-degumming quality, tenacity, luster
- **Tab 4 — Certificates:** Degumming certificates, downstream routing

---

### 7.16 Role 16: ROLE-SILK-GRADER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Supply Chain | Raw silk grading, A/B/C classification, tenacity testing | ML-based grade prediction from images, auto-sorting | Auto-certification, auto-route to Degumming/Inventory | Filature Supplier, Gate Clerk | Grade cert, auto-route downstream |
| Manufacturing | Material quality propagation | Quality prediction ML | Auto-bin assignment, auto-route | QA-1 results | Material release |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Filature Supplier:** Raw material details, GRN
- **Tab 2 — Grading:** Moisture, tenacity, size deviation, neatness assessment
- **Tab 3 — AI-Assisted Classification:** Image-based grade prediction, confidence score
- **Tab 4 — Certificates:** Grade certificates, downstream routing

---

### 7.17 Role 17: ROLE-SILK-MARK-OFFICER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Quality | Burn test, chemical solubility, XRF Zari audit, hologram tag serialization | Spectroscopy ML (purity verification), counterfeit detection, serialization uniqueness check | Auto-certification, auto-block on failure, auto-trigger Finishing | Quality Inspector, QA Dyeing Inspector, SUP Loom Floor Supervisor | Silk Mark cert, auto-route to Finishing |
| Compliance | Anti-adulteration lock, legal hold | Fraud detection ML | Global lockdown on FAILED_ADULTERATION_ALERT | QA-3 approval | Compliance release |
| Supply Chain | Tag inventory management | Serialization uniqueness ML | Auto-decrement tag inventory on issue | LOG Finishing scan | Tag binding |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Quality Inspector:** Approved QA logs, saree serial barcodes
- **Tab 2 — Compliance Audit:** Burn test, NaOH solubility, XRF results, Zari audit
- **Tab 3 — Certificates:** Silk Mark certificates, hologram tag serialization, QR binding
- **Tab 4 — Sales Forecast:** Material requirements, upcoming audit lots

---

### 7.18 Role 18: ROLE-SKEIN-DYE-MASTER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Dye bath execution, temperature profiling, chemical additives, post-dye QA | `AI-DYE-DEFECT` temp/chemical optimization, mass balance ML | Auto-certification, auto-route to QA Dyeing Inspector | Silk Degumming Master, Throwster/Twister, Master Colorist | Skein dye cert, auto-route to QA |
| Supply Chain | Inventory issuance/return, mass balance | Chemical usage optimization ML | Auto-bin update, auto-requisition | Store Inventory Manager issue | Stock reconciliation |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Master Colorist/Silk Degumming:** Recipe, yarn details, color requirements
- **Tab 2 — Dye Execution:** Bath setup, temp profile, chemical additives, time cycles
- **Tab 3 — Post-Dye QA:** Color fastness, rubbing test, tenacity, mass balance
- **Tab 4 — Certificates:** Skein dye certificates, QA Dyeing linkage

---

### 7.19 Role 19: ROLE-STORE-INVENTORY-MANAGER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Supply Chain | Inward stock placement, outward requisition, lot segregation, climate control, vault management | Smart reorder ML, shrinkage anomaly detection, FEFO automation, demand-aware bin optimization | Auto-reorder PR, auto-quarantine, auto-state transition, back-trace <15ms | All upstream roles (Filature, Zari, Finishing, Silk Mark) | Inventory cert, auto-issue to Warp/Master Weaver |
| Finance | Stock valuation, inventory audit | Valuation ML, anomaly detection | Auto-reconciliation, auto-alert on variance | Finance batch | Audit report |
| Procurement | Material requisition, supplier coordination | Demand forecasting ML | Auto-PR on low stock, auto-supplier suggestion | Production schedule | PO creation |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Finishing/Silk Mark:** Approved dispatch logs, certified sarees
- **Tab 2 — Inward Stock:** Bin allocation, lot matching, climate monitoring, vault status
- **Tab 3 — Outward Requisition:** Production requisition, material issue, recipient tracking
- **Tab 4 — Certificates:** Inventory certificates, lot traceability, movement proof
- **Tab 5 — Sales Forecast:** Material requirements, upcoming stock needs

---

### 7.20 Role 20: ROLE-SUP-LOOM-FLOOR-SUPERVISOR

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Floor monitoring, environmental control, quality escapes, root cause audits, batch release | `AI-SCHEDULER` production optimization, `AI-WEAVING-DEFECT` floor-wide, predictive downtime | Auto-batch release, auto-shift handover, auto-root-cause escalation | All upstream roles | Batch cert, auto-trigger QA-3 |
| HR | Shift scheduling, weaver coordination | Optimization ML, skill matching | Auto-schedule, auto-notify | Guild Manager, Master Weaver | Shift audit |
| Quality | First saree approval, quality escapes | Defect pattern ML | Auto-escalation, auto-hold | Master Weaver first-pick | QA-3 trigger |

**Key Dashboards:**
- **Tab 1 — Floor Overview:** OEE, active looms, environmental stats, AI alerts
- **Tab 2 — Batch Management:** Batch release, shift handover, root cause audits
- **Tab 3 — Quality Oversight:** First saree approval, defect trends, escape analysis
- **Tab 4 — Certificates:** Supervisor certificates, batch release proof

---

### 7.21 Role 21: ROLE-THROWSTER-TWISTER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Ply doubling, TPI control, twist setting/steaming, certification | Process optimization ML, TPI/ply quality prediction | Auto-certification, auto-route to Bobbin/Pirn/Master Colorist | Silk Degumming Master | Thrown silk cert |
| Supply Chain | Inventory issuance/return | Quality prediction ML | Auto-bin update | Store Inventory Manager issue | Stock reconciliation |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Silk Degumming Master:** Degummed silk details, quality metrics
- **Tab 2 — Throwing Process:** TPI settings, S/Z direction, ply count, steaming
- **Tab 3 — Quality Audit:** Twist uniformity, tensile strength, luster
- **Tab 4 — Certificates:** Thrown silk certificates, downstream routing

---

### 7.22 Role 22: ROLE-WARP-BEAM-PREPARATION

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|Auto-80-saree instantiation, auto-certification, auto-route to Warp Joiner | Bobbin Winder, Pirn Winders, Graph Drafter, Card Puncher | Warp beam cert, 80-saree IDs |
| Supply Chain | Material issuance, mass balance accounting | Yield prediction ML, optimal creel ML | Auto-bin update, auto-requisition | Store Inventory Manager issue | Stock reconciliation |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Bobbin/Pirn/Design:** Bobbin/pirn batches, design file, hook mapping
- **Tab 2 — Warping Setup:** Creel tension, end count (14,400), beaming-off
- **Tab 3 — Mass Balance & QA:** Weight check, tension sync, quality audit
- **Tab 4 — Certificates:** Warp beam certificates, 80-saree instantiation proof

---

### 7.23 Role 23: ROLE-WARP-JOINER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Manufacturing | Warp sheet alignment, precision knotting, drawing-through, lease correction | `AI-WARP-DEFECT` knot quality, tension prediction ML | Auto-certification, auto-route to Harness Setter/Petni Master | Warp Beam Preparation, Store Inventory Manager | Warp joining cert |
| Supply Chain | Material issuance/return | Tension optimization ML | Auto-bin update | Store Inventory Manager issue | Stock reconciliation |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Warp Beam Prep:** Beam details, design requirements
- **Tab 2 — Warp Joining:** Alignment, knotting, drawing-through, lease verification
- **Tab 3 — Quality Audit:** Knot strength, tension uniformity, defect detection
- **Tab 4 — Certificates:** Warp joining certificates, downstream routing

---

### 7.24 Role 24: ROLE-ZARI-INSPECTOR

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|---|
| Supply Chain | Zari assay, XRF audit, purity verification, refinery certification | `AI-ZARI-DEFECT` purity scoring, XRF ML model, counterfeit detection | Auto-certification, auto-route to Store Inventory/Throwster | Filature Supplier, Store Inventory Manager | Zari cert, purity class |
| Quality | Metallurgical audit, silk core verification | Spectroscopy ML | Auto-grade, auto-hold on failure | Raw material receipt | Refinery trigger |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Filature Supplier/Store Inventory:** Raw Zari details, supplier info
- **Tab 2 — Assay & XRF:** Silver content, gold content, silk core verification
- **Tab 3 — Refinery Process:** Refining steps, purity improvement, chemical treatment
- **Tab 4 — Certificates:** Zari certificates, purity proof, downstream routing

---

### 7.25 Role 25: ROLE-DESIGN-GENERATOR

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|Auto-certification, auto-route to Graph Drafter/SKU Manager | SKU Manager, Sales forecast | Design cert, SKU linkage |
| Design | GAN generation, style transfer, trend prediction | GAN models, style transfer ML, sales-feedback RL | Auto-design generation, auto-trend analysis | Sales forecast, festival calendar | Design deployment |

**Key Dashboards:**
- **Tab 1 — Pre-Process from SKU Manager/Sales:** SKU brief, market trends, festival calendar
- **Tab 2 — AI Design Generation:** GAN interface, style transfer, pattern preview
- **Tab 3 — Design QA:** Defect prediction, hook validation, simulation
- **Tab 4 — Certificates:** Design certificates, SKU linkage, deployment status

---

### 7.26 Role 26: ROLE-BUY-BACK-MANAGER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|Auto-risk assessment, auto-claim approval (low risk), auto-escalation (high risk) | LOG Finishing, Store Inventory Manager | Buy-back cert, refund trigger |
| Finance | Risk provisioning, warranty claims | `AI-BUYBACK-RISK` risk scoring, claim fraud detection | Auto-reserve calculation, auto-payout on approval | Finance batch | Financial statement update |

**Key Dashboards:**
- **Tab 1 — Pre-Process from Finishing/Inventory:** Dispatched sarees, certificates, grade details
- **Tab 2 — Risk Assessment:** ML risk score, warranty terms, claim history
- **Tab 3 — Claim Processing:** Claim verification, fraud detection, refund calculation
- **Tab 4 — Certificates:** Buy-back certificates, refund proof

---

### 7.27 Role 27: ROLE-GUILD-MANAGER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|Auto-skill certification, auto-payment split, auto-incentive calc | Master Weaver, Assistant Weaver | Guild cert, payment distribution |
| HR | Skill verification, performance analytics, incentive distribution | Skill progression ML, performance clustering, demand-aware scheduling | Auto-skill cert, auto-payment split, auto-incentive calc | SUP Loom Floor Supervisor, Master Weaver | Payroll integration |

**Key Dashboards:**
- **Tab 1 — Guild Overview:** Member count, skill distribution, performance metrics
- **Tab 2 — Skill Management:** Skill matrix, certification tracking, progression paths
- **Tab 3 — Payment & Incentives:** Piece-rate splits, incentive calc, distribution proof
- **Tab 4 — Certificates:** Guild membership certificates, skill certifications

---

### 7.28 Role 28: ROLE-IOT-DEVICE-MANAGER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|Auto-alert on device failure, auto-model update, auto-telemetry routing | All roles (via edge) | Device health, model deployment |
| Manufacturing | Edge controller management, loom telemetry, edge AI deployment | Anomaly detection on telemetry, predictive maintenance ML, model drift detection | Auto-alert, auto-work order, auto-model update | IoT device registration | Telemetry archive |

**Key Dashboards:**
- **Tab 1 — Device Fleet:** Edge controller status, loom connectivity, firmware versions
- **Tab 2 — Telemetry:** Real-time sensor data, vibration, temperature, motor current
- **Tab 3 — AI Models:** Model versions, drift metrics, deployment status
- **Tab 4 — Alerts & Maintenance:** Device failures, predictive maintenance, work orders

---

### 7.29 Role 29: ROLE-LOCALIZATION-MANAGER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|Auto-translation on content update, auto-TTS generation, auto-UI language switch | All dashboards | Translated content, audio files |
| All | Multi-language translation (Telugu/Tamil/Kannada/Hindi/Bengali), TTS audio, UI localization | `AI-TTS` neural TTS, MT quality scoring, context-aware translation | Auto-translation, auto-TTS, auto-language detection | Content update trigger | Localized UI |

**Key Dashboards:**
- **Tab 1 — Translation Management:** i18n keys, translation status, quality scores
- **Tab 2 — TTS Audio:** Audio generation, voice selection, language mapping
- **Tab 3 — UI Localization:** Dashboard preview, language switching, font support
- **Tab 4 — Certificates:** Translation certificates, audio proof

---

### 7.30 Role 30: ROLE-SKU-MANAGER

| Module | Features | AI/ML | Automation | Pre-Process | Post-Process |
|---|---|---|---|---|Auto-SKU creation from design, auto-B2B linkage, auto-forecast update | Design Generator, Sales forecast | SKU cert, B2B order linkage |
| Sales | SKU catalog, B2B order management | `AI-DEMAND-FCAST` SKU-level forecast, design recommendation ML | Auto-SKU creation, auto-forecast update, auto-order allocation | Sales forecast, design approval | Production trigger |

**Key Dashboards:**
- **Tab 1 — SKU Catalog:** Active SKUs, design linkage, B2B order history
- **Tab 2 — Design Management:** Design approval, SKU assignment, design-to-SKU mapping
- **Tab 3 — Sales Integration:** B2B orders, forecast integration, demand signals
- **Tab 4 — Certificates:** SKU certificates, B2B order proof

---

## 8. End-to-End Automated Flow: Complete Lineage Example

### Scenario: Premium Kanchipuram Bridal Saree (2400 Hook, Pure Zari)

```
SALES FORECAST (AI-DEMAND-FCAST)
  │
  ▼
SKU MANAGER creates SKU-KNC-2400-BR-09
  │
  ▼
DESIGN GENERATOR (GAN) creates DES-2400-KNC-09-HASH
  │
  ▼
GRAPH DRAFTER configures 2400-hook mapping, color separation
  │
  ▼
CARD PUNCHER programs loom controller
  │
  ▼
FILATURE SUPPLIER delivers RAW-LOT-FIL-001-20240815
  │  → AI-ZARI-DEFECT scans spools
  │  → QC Inspector validates moisture/tenacity
  │  → SILK GRADER grades A
  │  → Auto-payout triggered
  ▼
STORE INVENTORY MANAGER receives into SILK-RACK-B04
  │
  ▼
SILK DEGUMMING MASTER processes (alkali pH 11.5, 95°C, 22% sericin loss)
  │  → Auto-certification
  ▼
THROWSTER/TWISTER twists (TPI 1200, ply 2, S-twist)
  │  → Auto-certification
  ▼
BOBIN WINDER / PIRN WINDERS wind yarns
  │  → AI-WARP-DEFECT validates hook compatibility
  │  → Auto-certification
  ▼
STORE INVENTORY MANAGER issues to WARP-BCH-KNC-001
  │
  ▼
WARP BEAM PREPARATION creels 14,400 ends, winds 504m
  │  → Auto-80-saree instantiation (KNC-2400-BR-09-S01 to S80)
  │  → Auto-certification
  ▼
WARP JOINER knots, draws through, lease corrects
  │  → Auto-certification
  ▼
LOOM HARNESS SETTER configures harness, shed alignment
  │  → Auto-certification
  ▼
PETNI MASTER splices body/border warps
  │  → Auto-deplete contrast yarn inventory
  │  → Auto-certification
  ▼
MASTER WEAVER + ASSISTANT WEAVER operate loom
  │  → AI-WEAVING-DEFECT real-time monitoring
  │  → Auto-yield calc, auto-wage split
  ▼
SUP LOOM FLOOR SUPERVISOR releases batch
  │  → Auto-trigger QA-3
  ▼
QUALITY INSPECTOR inspects (length 6.32m, 0 defects)
  │  → AI-FABRIC-DEFECT automated inspection
  │  → GRADE_A_PREMIUM
  │  → Auto-piece-rate (100% release)
  │  → Auto B2B order match (B2B-PO-2024-001)
  ▼
QA DYEING INSPECTOR verifies color (ΔE 0.3, fastness Grade 5)
  │  → Auto-certification
  ▼
SILK MARK OFFICER certifies (burn test PASSED, XRF Ag 48.2%, Au 5.2g/kg)
  │  → SMK-8293746192 generated
  │  → Auto-certification
  ▼
LOG FINISHING TRANSIT SPECIALIST packs (anti-tarnish vacuum, desiccant)
  │  → Auto-manifest generation
  │  → Auto-AWB (DHL-123456789)
  │  → Auto-inventory state → IN_TRANSIT_REVENUE
  ▼
STORE INVENTORY MANAGER receives into DISPATCH-WAREHOUSE
  │
  ▼
BUY-BACK MANAGER registers warranty (risk score: LOW)
  │
  ▼
FINANCE auto-releases supplier payout (smart contract)
  │
  ▼
BACK-TRACE: saree_serial_id = KNC-2400-BR-09-S42
  → Redis cache hit: <15ms
  → Full lineage: RawLot → ThrownLot → DyeLot → BobbinBatch → WarpBeam → Saree
  → 11-step upstream trace with all certifications, grades, and operator IDs
```

---

## 9. Data Model: Core Entities & Relationships

### 9.1 Entity Relationship Overview

```
User (USR-{ROLE}-{SNOWFLAKE})
  │
  ├─→ has_role → Role (ROLE-{NAME})
  │
  ├─→ operates_at → FactoryNode (FACT-{REGION}-{SEQ})
  │
  └─→ participates_in → WorkflowTransition (state machine)

RawMaterialLot (RAW-LOT-{VENDOR_ID}-{TIMESTAMP})
  │
  ├─→ graded_by → SilkGrader
  │     └─→ grade → GradeCert (A/B/C)
  │
  ├─→ inspected_by → QCInspector
  │     └─→ cert → QACert (QA-1)
  │
  ├─→ degummed_by → SilkDegummingMaster
  │     └─→ produces → ThrownLot (THROWN-SILK-{SNOWFLAKE})
  │
  └─→ stored_in → InventoryBin

ThrownLot
  │
  ├─→ twisted_by → ThrowsterTwister
  │     └─→ produces → BobbinBatch / PirnBatch
  │
  └─→ supplied_to → MasterColorist
        └─→ formulates → DyeRecipe
              └─→ executed_by → SkeinDyeMaster
                    └─→ produces → DyeLot (DYE-LOT-{COLOR}-{TIMESTAMP})
                          └─→ certified_by → QADyeingInspector

ZariSpool
  │
  ├─→ inspected_by → ZariInspector
  │     └─→ cert → ZariCert (purity class)
  │
  └─→ stored_in → VaultBin

WarpBeam (WARP-BCH-{LOOM_ID}-{DATE}-{SEQ})
  │
  ├─→ created_by → WarpBeamPreparation
  │     ├─→ uses → BobbinBatch
  │     ├─→ uses → PirnBatch
  │     └─→ uses → JacquardFile (DES-{HOOKS}-{HASH})
  │
  ├─→ joined_by → WarpJoiner
  │     └─→ produces → WarpSheet
  │
  ├─→ harnessed_by → LoomHarnessSetter
  │     └─→ produces → LoomHarness
  │
  ├─→ splices → PetniMaster
  │     └─→ produces → PetniBatch
  │
  └─→ spawns → Saree (×80)
        │   {WARP_BATCH_ID}-S01 ... S80
        │
        ├─→ woven_by → MasterWeaver
        │     └─→ assisted_by → AssistantWeaver
        │
        ├─→ supervised_by → SUP_LoomFloorSupervisor
        │
        ├─→ inspected_by → QualityInspector
        │     └─→ grades → Grade (A/B/C/REJECTED)
        │
        ├─→ certified_by → QADyeingInspector
        │
        ├─→ certified_by → SilkMarkOfficer
        │     └─→ issues → SilkMarkTag (SMK-{SNOWFLAKE})
        │           └─→ affixed_to → Saree
        │
        ├─→ finished_by → LogFinishingSpecialist
        │     └─→ produces → DispatchManifest
        │
        ├─→ managed_by → StoreInventoryManager
        │     └─→ movement → InventoryMovement
        │
        └─→ covered_by → BuyBackGuarantee
              └─→ managed_by → BuyBackManager

JacquardFile (DES-{HOOKS}-{HASH})
  │
  ├─→ generated_by → DesignGenerator
  │     └─→ triggered_by → SKUManager
  │
  ├─→ drafted_by → GraphDrafter
  │
  └─→ programmed_by → CardPuncher

SKU
  │
  ├─→ defined_by → SKUManager
  ├─→ design → JacquardFile
  └─→ linked_to → B2BOrder

B2BOrder
  │
  ├─→ triggers → ProductionPlan
  ├─→ fulfilled_by → DispatchManifest
  └─→ matched_to → Saree (via GRADE_A_EXPORT_PREMIUM)

Guild
  │
  ├─→ manages → GuildManager
  ├─→ member → MasterWeaver
  ├─→ member → AssistantWeaver
  └─→ certified_by → GuildCert

EdgeController
  │
  ├─→ manages → IoTDeviceManager
  ├─→ connected_to → Loom
  └─→ streams → LoomTelemetry
```

### 9.2 Kafka Event Schema

```json
{
  "event_id": "EVT-{SNOWFLAKE}",
  "event_type": "CERTIFIED | BLOCKED | REJECTED | RELEASED",
  "entity_type": "RAW-LOT | THROWN-SILK | DYE-LOT | BOBBIN-BCH | WARP-BCH | SAREE | SMK | CERT",
  "entity_id": "{ENTITY_ID}",
  "factory_node_id": "FACT-{REGION}-{SEQ}",
  "role_id": "ROLE-{NAME}",
  "user_id": "USR-{ROLE}-{SNOWFLAKE}",
  "timestamp": "{ISO-8601}",
  "payload": {
    "previous_state": "{STATE}",
    "new_state": "{STATE}",
    "guardrails_triggered": [],
    "certificate_hash": "{HASH}",
    "qr_tag_id": "{QR}",
    "lineage_ref": "{SNOWFLAKE}"
  },
  "trace_id": "{TRACE_ID}",
  "metadata": {}
}
```

---

## 10. Neo4j Graph Schema: Full Relationship Map

```cypher
// Core User/Role Graph
(u:User {id: "USR-{ROLE}-{SNOWFLAKE}"})-[:HAS_ROLE]->(r:Role {role_id: "ROLE-{ROLE}"})
(u)-[:OPERATES_AT]->(f:FactoryNode {id: "FACT-{REGION}-{SEQ}"})

// Raw Material Flow
(:FilatureSupplier)-[:SUPPLIES]->(:RawMaterialLot {id: "RAW-LOT-{VENDOR_ID}-{TIMESTAMP}"})
(:RawMaterialLot)-[:GRADED_BY]->(:SilkGrader)
(:RawMaterialLot)-[:INSPECTED_BY]->(:QCInspector)
(:RawMaterialLot)-[:STORED_IN]->(:InventoryBin)
(:RawMaterialLot)-[:DEGUMMED_BY]->(:SilkDegummingMaster)
(:RawMaterialLot)-[:THROWN_BY]->(:ThrownLot {id: "THROWN-SILK-{SNOWFLAKE}"})
(:ThrownLot)-[:TWISTED_BY]->(:ThrowsterTwister)
(:ThrownLot)-[:SUPPLIED_TO]->(:MasterColorist)

// Zari Flow
(:FilatureSupplier)-[:SUPPLIES_ZARI]->(:ZariSpool)
(:ZariSpool)-[:INSPECTED_BY]->(:ZariInspector)
(:ZariSpool)-[:STORED_IN_VAULT]->(:VaultBin)
(:ZariSpool)-[:REFINED_BY]->(:ZariBatch)

// Yarn Preparation
(:ThrowsterTwister)-[:SUPPLIES_TO]->(:BobbinWinder)
(:ThrowsterTwister)-[:SUPPLIES_TO]->(:PirnWinders)
(:BobbinWinder)-[:WINDS]->(:BobbinBatch {id: "BOBBIN-BCH-{SNOWFLAKE}"})
(:PirnWinders)-[:WINDS]->(:PirnBatch {id: "PIRN-BCH-{SNOWFLAKE}"})

// Dyeing Flow
(:MasterColorist)-[:FORMULATES]->(:DyeRecipe)
(:MasterColorist)-[:SUPPLIES_TO]->(:SkeinDyeMaster)
(:SkeinDyeMaster)-[:DYES]->(:DyeLot {id: "DYE-LOT-{COLOR_CODE}-{TIMESTAMP}"})
(:DyeLot)-[:CERTIFIED_BY]->(:QADyeingInspector)

// Design Flow
(:DesignGenerator)-[:GENERATES]->(:JacquardFile {id: "DES-{HOOKS}-{PATTERN_HASH}"})
(:SKUManager)-[:DEFINES]->(:SKU)
(:JacquardFile)-[:DRAFTED_BY]->(:GraphDrafter)
(:JacquardFile)-[:PROGRAMMED_BY]->(:CardPuncher)

// Warp Preparation
(:BobbinBatch)-[:SUPPLIED_TO]->(:WarpBeamPreparation)
(:PirnBatch)-[:SUPPLIED_TO]->(:WarpBeamPreparation)
(:JacquardFile)-[:USED_BY]->(:WarpBeamPreparation)
(:WarpBeamPreparation)-[:CREATES]->(:WarpBeam {id: "WARP-BCH-{LOOM_ID}-{DATE}-{SEQ}"})
(:WarpBeam)-[:SPAWNS]->(:Saree {id: "{WARP_BATCH_ID}-S01"})
(:WarpBeam)-[:SPAWNS]->(:Saree {id: "{WARP_BATCH_ID}-S80"})

// Warp Joining & Harness
(:WarpBeam)-[:JOINED_BY]->(:WarpJoiner)
(:WarpJoiner)-[:SUPPLIES_TO]->(:LoomHarnessSetter)
(:WarpJoiner)-[:SUPPLIES_TO]->(:PetniMaster)
(:LoomHarnessSetter)-[:SETS_UP]->(:LoomHarness)
(:PetniMaster)-[:SPLICES]->(:WarpSheet)

// Weaving Execution
(:LoomHarness)-[:USED_BY]->(:MasterWeaver)
(:WarpSheet)-[:WOVEN_BY]->(:MasterWeaver)
(:MasterWeaver)-[:ASSIGNS_TO]->(:AssistantWeaver)
(:MasterWeaver)-[:REPORTS_TO]->(:SUPLoomFloorSupervisor)
(:EdgeController)-[:MONITORS]->(:Loom)
(:IoTDeviceManager)-[:MANAGES]->(:EdgeController)

// QA & Certification
(:Saree)-[:INSPECTED_BY]->(:QualityInspector)
(:QualityInspector)-[:GRADES]->(:Saree)
(:Saree)-[:INSPECTED_BY]->(:QADyeingInspector)
(:Saree)-[:CERTIFIED_BY]->(:SilkMarkOfficer)
(:SilkMarkOfficer)-[:ISSUES]->(:SilkMarkTag {id: "SMK-{SNOWFLAKE}"})
(:SilkMarkTag)-[:AFFIXED_TO]->(:Saree)

// Finishing & Dispatch
(:Saree)-[:FINISHED_BY]->(:LogFinishingSpecialist)
(:LogFinishingSpecialist)-[:PACKS]->(:Saree)
(:LogFinishingSpecialist)-[:DISPATCHES]->(:Saree)
(:Saree)-[:STORED_IN]->(:DispatchWarehouse)
(:LogFinishingSpecialist)-[:GENERATES]->(:DispatchManifest)

// Inventory Management
(:InventoryBin)-[:ISSUED_TO]->(:WarpBeamPreparation)
(:InventoryBin)-[:ISSUED_TO]->(:MasterWeaver)
(:StoreInventoryManager)-[:MANAGES]->(:InventoryBin)
(:StoreInventoryManager)-[:RECONCILES]->(:VaultBin)

// Buy-Back & Guild
(:Saree)-[:COVERED_BY]->(:BuyBackGuarantee)
(:BuyBackManager)-[:MANAGES]->(:BuyBackGuarantee)
(:MasterWeaver)-[:MEMBER_OF]->(:Guild)
(:AssistantWeaver)-[:MEMBER_OF]->(:Guild)
(:GuildManager)-[:MANAGES]->(:Guild)

// Localization & SKU
(:Dashboard)-[:LOCALIZED_BY]->(:LocalizationManager)
(:SKUManager)-[:DEFINES]->(:SKU)
(:DesignGenerator)-[:GENERATES_FOR]->(:SKU)
```

---

## 11. Integration Architecture: Event-Driven CQRS

### 11.1 Command Side (Write)

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Frontend   │────▶│  API Gateway │────▶│  Write DB    │
│   (React)    │     │  (Fastify)   │     │(CockroachDB) │
└──────────────┘     └──────┬───────┘     └──────┬───────┘
                               │                   │
                               ▼                   ▼
                        ┌──────────────┐     ┌──────────────┐
                        │ Guardrail    │     │   Kafka      │
                        │ Engine       │     │  Producer    │
                        └──────┬───────┘     └──────┬───────┘
                               │                   │
                               ▼                   ▼
                        ┌──────────────┐     ┌──────────────┐
                        │  Validation  │     │   Neo4j      │
                        │  & Routing   │     │  (Async)     │
                        └──────────────┘     └──────────────┘
```

### 11.2 Query Side (Read)

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Frontend   │────▶│  API Gateway │────▶│  Read DB     │
│   (React)    │     │  (Fastify)   │     │(CockroachDB) │
└──────────────┘     └──────┬───────┘     └──────┬───────┘
                               │                   │
                               ▼                   ▼
                        ┌──────────────┐     ┌──────────────┐
                        │  Redis       │     │   Neo4j      │
                        │  Cache       │     │  (Graph)     │
                        │  (<15ms)     │     │  (<50ms)     │
                        └──────────────┘     └──────────────┘
```

### 11.3 Event Flow: Saree Lifecycle

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SAREE LIFECYCLE EVENT FLOW                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  WarpBeamCertified ──▶ lot.{f}.warp-beam-preparation.certified     │
│       │                                                             │
│       ▼                                                             │
│  WarpJoinerCertified ──▶ lot.{f}.warp-joiner.certified             │
│       │                                                             │
│       ▼                                                             │
│  LoomHarnessSetCertified ──▶ lot.{f}.loom-harness-setter.certified │
│       │                                                             │
│       ▼                                                             │
│  PetniCertified ──▶ lot.{f}.petni-master.certified                 │
│       │                                                             │
│       ▼                                                             │
│  WeavingStarted ──▶ iot.{f}.loom.{id}.telemetry                    │
│       │                                                             │
│       ▼                                                             │
│  FirstPickVerified ──▶ lot.{f}.master-weaver.certified             │
│       │                                                             │
│       ▼                                                             │
│  BatchReleased ──▶ lot.{f}.sup-supervisor.certified                │
│       │                                                             │
│       ▼                                                             │
│  QA3Inspected ──▶ lot.{f}.quality-inspector.certified              │
│       │                                                             │
│       ├──▶ GRADE_A ──▶ lot.{f}.qa-dyeing-inspector.certified       │
│       │       │                                                     │
│       │       ▼                                                     │
│       │   SilkMarkCertified ──▶ lot.{f}.silk-mark-officer.certified│
│       │       │                                                     │
│       │       ▼                                                     │
│       │   FinishingCertified ──▶ lot.{f}.log-finishing.certified   │
│       │       │                                                     │
│       │       ▼                                                     │
│       │   DispatchReleased ──▶ lot.{f}.store-inventory.certified   │
│       │       │                                                     │
│       │       ▼                                                     │
│       │   BuyBackRegistered ──▶ lot.{f}.buy-back-manager.certified │
│       │                                                             │
│       ├──▶ GRADE_B/REJECTED ──▶ QUARANTINE                         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 12. Technology Stack Summary

| Layer | Technology | Purpose |
|---|---|---|
| **Frontend** | React 18 + MUI v5 + PWA | Role-specific dashboards, offline-first |
| **API Gateway** | Fastify + JWT + OAuth2 | Auth, rate limiting, routing |
| **Write DB** | CockroachDB (active-active) | OLTP, high-frequency writes |
| **Read DB** | CockroachDB Read Replicas | CQRS query side |
| **Graph DB** | Neo4j Cluster | Lineage, back-trace, relationships |
| **Cache** | Redis Cluster | Session, trace cache, hot data |
| **Message Broker** | Apache Kafka | Event streaming, audit log |
| **IoT** | MQTT over TLS 1.3 | Edge controller telemetry |
| **Edge AI** | ONNX/TensorRT | Real-time defect detection |
| **Cloud AI** | PyTorch + FastAPI | Demand forecast, GAN, PPO |
| **Object Storage** | S3-compatible | Design files, certificates, images |
| **Search** | Elasticsearch | Full-text search across lots, designs |
| **Monitoring** | Prometheus + Grafana | Metrics, dashboards, alerts |
| **Logging** | Loki + Tempo | Distributed tracing, logs |
| **CI/CD** | GitHub Actions + ArgoCD | GitOps deployment |
| **Infrastructure** | Kubernetes + Helm | Container orchestration |
| **Secrets** | HashiCorp Vault | API keys, certificates, JWT signing |

---

## 13. Scalability & Performance Targets

| Metric | Target | Architecture |
|---|---|---|
| **Concurrent Users** | 1M+ | JWT stateless + Redis session |
| **Writes/sec** | 100K+ | CockroachDB sharding + Kafka batching |
| **Back-trace Latency** | <15ms | Neo4j + Redis cache |
| **Dashboard Load** | <2s | CDN + edge cache + code splitting |
| **AI Inference** | <100ms | Edge ONNX (weaving) / Cloud batch (forecast) |
| **Event Processing** | <500ms | Kafka + async workers |
| **DB Replication** | <1s | CockroachDB active-active |
| **Uptime** | 99.99% | Multi-region + auto-failover |
| **Data Retention** | 7 years | S3 Glacier + CockroachDB time-series |

---

*This document serves as the complete feature discovery and enterprise architecture blueprint for the Silk Saree Manufacturing ERP, covering all 30 roles, 9 ERP modules, embedded AI/ML services, automated workflows, and end-to-end integration from filature to saree inventory at million-user scale.*
