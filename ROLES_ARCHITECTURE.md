# Silk Saree ERP: End-to-End Architecture & Role Integration Blueprint

## 1. Distributed Identity Protocol & System Syntax

### 64-Bit Snowflake ID Structure

```
+-------------------------------------------------------------------------+
|                        64-Bit Snowflake ID Structure                    |
+-------------------------------------------------------------------------+
| 1 Bit |    41 Bits     |   5 Bits    |   5 Bits    |      12 Bits       |
| Unused| Timestamp (ms) | Data Center | Worker Node | Sequence (0-4095)  |
+-------------------------------------------------------------------------+
```

### Entity Syntax Mapping Table

| Entity Type | ID Syntax Pattern | Description |
|---|---|---|
| Vendor / Supplier | `VND-{ROLE}-{SNOWFLAKE}` | Suppliers, Twisters, Dyers, Weavers, Finishers |
| User / Operator | `USR-{ROLE}-{SNOWFLAKE}` | Internal staff, QC officers, Store managers |
| Raw Material Lot | `RAW-LOT-{VENDOR_ID}-{TIMESTAMP}` | Unprocessed silk skeins or zari spools |
| Thrown Silk Lot | `THROWN-SILK-{SNOWFLAKE}` | Degummed and twisted yarn batch |
| Dye Lot | `DYE-LOT-{COLOR_CODE}-{TIMESTAMP}` | Skein dyed yarn certified under QA-2 |
| Bobbin / Pirn Batch | `BOBBIN-BCH-{SNOWFLAKE}` / `PIRN-BCH-{SNOWFLAKE}` | Wound yarn packages ready for warping/weaving |
| Jacquard File | `DES-{HOOKS}-{PATTERN_HASH}` | Compiled electronic design file for card-cutter/looms |
| Master Warp Beam | `WARP-BCH-{LOOM_ID}-{DATE}-{SEQ}` | 504m continuous beam (spawns 80 child saree IDs) |
| Master Saree Code | `{WARP_BATCH_ID}-S01 ... S80` | Individual saree item tracking unit |
| Silk Mark Tag | `SMK-{SNOWFLAKE}` | Holographic silk mark security tag |

---

## 2. Complete Phase-by-Phase Logic & Automated QA Gates

### Phase 1: Raw Material Intake & Quality Testing (QA-1)
**Actors & Inputs:** Gate Clerk (`USR-GATE`) logs scale weight, bale count, and supplier ID. QC Inspector (`USR-QC`) inputs moisture %, tenacity, size deviation %, and neatness.

**System Formula:**
```
Conditioned Weight (kg) = Actual Weight * (1 - (Moisture% / 100)) * 1.11
```

**Automated Guardrail Gate (QA-1):**
```
IF Live Moisture % > 11.0% OR Size Deviation % > 4.0% OR Tenacity < 3.8 g/d:
  → System sets state to QC_REJECT_HOLD and locks inventory.
ELSE:
  → System transitions state to QA1_CERTIFIED_SILK and authorizes supplier payout.
```

### Phase 2: Degumming & Throwing
**Actors & Inputs:** Degumming Specialist (`VND-DEG`) inputs alkali pH, temp, and target sericin loss % (Target: 20-25%). Throwster (`VND-THR`) inputs TPI (turns per inch), S/Z direction, and ply count.

**System Output:** Generates `THROWN_SILK_LOT_ID` linked upstream to `RAW_LOT_ID`.

### Phase 3: Skein Dyeing & Color Certification (QA-2)
**Actors & Inputs:** Colorist (`VND-CLR`) inputs Pantone code, spectrophotometer Delta E (ΔE), rubbing fastness, and post-dye tenacity.

**Automated Guardrail Gate (QA-2):**
```
IF target_machine == "2400_HOOK_JACQUARD" AND (Delta_E > 0.5 OR post_dye_tenacity < 3.8 g/d):
  → BLOCK EXECUTION: Reject for high-density warp use.
ELSE:
  → Issue DYE_LOT_ID with status QA2_CERTIFIED.
```

### Phase 4: Yarn Winding & Preparation
**Actors & Inputs:** Winding Operator (`USR-WND`) inputs winding speed (m/min), jointing method, and tension settings.

**Automated Guardrail Gate:**
```
IF target_machine == "2400_HOOK_JACQUARD" AND joint_method == "Standard Weaver's Knot":
  → BLOCK EXECUTION: Air splicing mandatory to pass 2400-hook reed clearance.
IF winding_speed > 180 m/min:
  → BLOCK EXECUTION: Speed exceeds threshold for fine silk.
```

### Phase 5 & 6: Jacquard Compilation, Beam Creation & Saree Instantiation
**Actors & Inputs:** Designer (`USR-DSG`) compiles card files. Warping Master (`USR-WRP`) creels 14,400 ends and winds a 504-meter beam.

**Automated Saree Generation:** Upon beam completion, the system generates 1 Master `WARP_BATCH_ID` and automatically instantiates 80 child Saree IDs (`{WARP_BATCH_ID}-S01` through `S80`).

### Phase 7: Petni Contrast Splicing & Weaving Execution
**Actors & Inputs:** Petni Master (`USR-PTN`) splices body and border warps. Weaver (`VND-WVR`) operates loom with automated pick counters.

**Automated Guardrail Gate:**
```
IF crossed_ends_count > 0: → BLOCK LOOM START.
IF EPI >= 140 AND dropper_wire_weight > 0.3g: → BLOCK LOOM START (heavy droppers snap fine warp).
IF warp_tension NOT IN (110 cN TO 150 cN): → AUTO-PAUSE LOOM.
IF section == "PALLU_ZARI" AND loom_speed > 165 PPM: → AUTO-THROTTLE LOOM SPEED.
```

### Phase 8 & 9: QA-3 Final Inspection, Silk Mark & Crate Storage
**Actors & Inputs:** QA Inspector (`USR-QA3`) verifies dimensions and defect counts. Store Manager (`USR-STR`) scans items into transport crates.

**Automated Guardrail Gate (QA-3):**
```
IF length >= 6.30m AND defects == 0: → Assign GRADE_A_PREMIUM and bind SMK-{SNOWFLAKE}.
IF zari_type == "PURE_GOLD_SILVER_TESTED_ZARI" AND packaging != "ANTI_TARNISH_VACUUM":
  → BLOCK DISPATCH.
```

---

## 3. Data Integration & Universal Back-Tracing Engine

The ERP uses an event-driven Kafka pipeline to push transactional state changes into Neo4j graph nodes and pre-computed Redis hash maps.

```
[ Operational Event ] ──> [ Kafka Topic ] ──> [ Write Engine ] ──> [ CockroachDB Shards ]
                                                  │
                                                  ├──> [ Neo4j Graph DB ]
                                                  └──> [ Redis Trace Cache ]
```

**High-Speed Universal Back-Tracing Microservice (Python):**
```python
def back_trace_master_saree(saree_serial_id: str) -> dict:
    """
    Retrieves complete 11-step upstream manufacturing lineage
    from a single Saree Serial Code in <15ms.
    """
    # 1. Check Redis In-Memory Trace Cache
    cached_trace = redis_client.hgetall(f"trace:{saree_serial_id}")
    if cached_trace:
        return {"source": "REDIS_CACHE", "lineage": cached_trace}
        
    # 2. Extract Warp Beam Batch ID from Saree Code
    warp_batch_id = saree_serial_id.rsplit("-", 1)[0]
    
    # 3. Query Neo4j Directed Acyclic Graph
    cypher_query = """
    MATCH (saree:Saree {id: $saree_id})
    MATCH (saree)<-[:PRODUCED_SAREE]-(warp:WarpBatch {id: $warp_id})
    MATCH (warp)<-[:USED_WARP_YARN]-(dye_warp:DyeLot)
    MATCH (warp)<-[:SPLICED_BORDER]-(petni:PetniBatch)<-[:USED_BORDER_YARN]-(dye_border:DyeLot)
    MATCH (dye_warp)<-[:DYED_FROM]-(thrown:ThrownLot)<-[:THROWN_FROM]-(raw_silk:RawLot)
    MATCH (dye_warp)<-[:USED_ZARI]-(raw_zari:RawLot)
    RETURN saree, warp, petni, dye_warp, dye_border, thrown, raw_silk, raw_zari
    """
    graph_result = graph_db.execute(cypher_query, {"saree_id": saree_serial_id, "warp_id": warp_batch_id})
    
    return {"source": "NEO4J_GRAPH_DB", "lineage": graph_result}
```

---

## 4. Role Profiles: End-to-End Architecture & Relationships

### Role 1: ROLE-SYSTEM-ADMIN
- **Display Name:** System/Admin Superuser
- **Email:** system.admin@factory.com
- **Employee ID:** EMP-001
- **Entity ID Pattern:** `USR-SYS-{SNOWFLAKE}`
- **Phase Coverage:** All phases (orchestrator)
- **Pre-Process:** None (superuser)
- **Post-Process:** None (superuser)
- **Downstream Relationships:** All roles
- **Snowflake ID:** `USR-SYS-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `admin.*`, `system.*`
- **Neo4j Labels:** `SystemAdmin`, `User`
- **Relationships:**
  - `[:MANAGES]→` All roles
  - `[:AUDITS]→` All workflow states
  - `[:CONFIGURES]→` Factory nodes, Loom assignments

### Role 2: ROLE-ASSISTANT-WEAVER
- **Display Name:** Assistant Weaver
- **Email:** assistant.weaver@factory.com
- **Employee ID:** EMP-002
- **Entity ID Pattern:** `USR-AW-{SNOWFLAKE}`
- **Phase Coverage:** Phase 7 (Weaving Execution)
- **Pre-Process:** Master Weaver job assignment
- **Post-Process:** Shift logs, wage splits, breakage alarms
- **Downstream Relationships:** Master Weaver, Petni Master, Warp Joiner, Loom Harness Setter
- **Snowflake ID:** `USR-AW-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.assistant-weaver.certified`
- **Neo4j Labels:** `AssistantWeaver`, `User`, `LoomOperator`
- **Relationships:**
  - `[:ASSIGNED_TO]→` MasterWeaverJob
  - `[:OPERATES]→` Loom
  - `[:REPORTS]→` MasterWeaver
  - `[:HANDLES]→` WarpJoiningJob
  - `[:SUPPORTS]→` PetniMasterJob

### Role 3: ROLE-BOBBIN-WINDER
- **Display Name:** Bobbin Winder
- **Email:** bobbin.winder@factory.com
- **Employee ID:** EMP-003
- **Entity ID Pattern:** `USR-BW-{SNOWFLAKE}`
- **Phase Coverage:** Phase 4 (Yarn Winding & Preparation)
- **Pre-Process:** Filature Supplier raw silk intake
- **Post-Process:** Bobbin batch certification, 1536/2400 hook validation
- **Downstream Relationships:** Filature Supplier, Warp Beam Preparation, Master Weaver
- **Snowflake ID:** `USR-BW-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.bobbin-winder.certified`
- **Neo4j Labels:** `BobbinWinder`, `User`, `YarnPreparation`
- **Relationships:**
  - `[:WINDS]→` BobbinBatch (`BOBBIN-BCH-{SNOWFLAKE}`)
  - `[:RECEIVES]→` ThrownSilkLot
  - `[:SUPPLIES_TO]→` WarpBeamPreparation
  - `[:VALIDATES]→` HookCount (1536/2400)

### Role 4: ROLE-CARD-PUNCHER
- **Display Name:** Card Puncher (Digital E-Jacquard Programmer)
- **Email:** card.puncher@factory.com
- **Employee ID:** EMP-004
- **Entity ID Pattern:** `USR-CP-{SNOWFLAKE}`
- **Phase Coverage:** Phase 5 (Jacquard Compilation)
- **Pre-Process:** Graph Drafter design files
- **Post-Process:** Card punch certification, design verification
- **Downstream Relationships:** Graph Drafter, Loom Harness Setter, Master Weaver
- **Snowflake ID:** `USR-CP-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.card-puncher.certified`
- **Neo4j Labels:** `CardPuncher`, `User`, `DesignEngineer`
- **Relationships:**
  - `[:PROGRAMS]→` JacquardFile (`DES-{HOOKS}-{PATTERN_HASH}`)
  - `[:RECEIVES]→` GraphDraftDesign
  - `[:DEPLOYS_TO]→` LoomController
  - `[:VERIFIES]→` HarnessSetup

### Role 5: ROLE-FILATURE-SUPPLIER
- **Display Name:** Filature Supplier
- **Email:** filature.supplier@factory.com
- **Employee ID:** EMP-005
- **Entity ID Pattern:** `VND-FIL-{SNOWFLAKE}`
- **Phase Coverage:** Phase 1 (Raw Material Intake)
- **Pre-Process:** External supplier
- **Post-Process:** Raw lot certification, supplier payout authorization
- **Downstream Relationships:** Gate Clerk, QC Inspector, Zari Inspector, Throwster/Twister
- **Snowflake ID:** `VND-FIL-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.filature-supplier.certified`
- **Neo4j Labels:** `FilatureSupplier`, `Vendor`, `RawMaterialSource`
- **Relationships:**
  - `[:SUPPLIES]→` RawMaterialLot (`RAW-LOT-{VENDOR_ID}-{TIMESTAMP}`)
  - `[:TRIGGERS]→` GateClerkInspection
  - `[:RECEIVES_PAYOUT_FROM]→` SystemAdmin
  - `[:FEEDS]→` ZariInspector
  - `[:FEEDS]→` ThrowsterTwister

### Role 6: ROLE-GRAPH-DRAFTER
- **Display Name:** Graph Drafter (2400 Hook)
- **Email:** graph.drafter@factory.com
- **Employee ID:** EMP-006
- **Entity ID Pattern:** `USR-GD-{SNOWFLAKE}`
- **Phase Coverage:** Phase 5 (Jacquard Compilation)
- **Pre-Process:** Design Generator, SKU Manager
- **Post-Process:** Design certification, hook mapping validation
- **Downstream Relationships:** Design Generator, Card Puncher, Warp Beam Preparation
- **Snowflake ID:** `USR-GD-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.graph-drafter.certified`
- **Neo4j Labels:** `GraphDrafter`, `User`, `DesignEngineer`
- **Relationships:**
  - `[:CREATES]→` JacquardFile (`DES-{HOOKS}-{PATTERN_HASH}`)
  - `[:RECEIVES_FROM]→` DesignGenerator
  - `[:SUPPLIES_TO]→` CardPuncher
  - `[:CONFIGURES]→` HookMapping (1536/2400)

### Role 7: ROLE-LOG-FINISHING
- **Display Name:** LOG Finishing Transit Specialist
- **Email:** log.finishing@factory.com
- **Employee ID:** EMP-007
- **Entity ID Pattern:** `USR-LF-{SNOWFLAKE}`
- **Phase Coverage:** Phase 8 (Finishing & Transit)
- **Pre-Process:** Quality Inspector, QA Dyeing Inspector, Silk Mark Officer
- **Post-Process:** Dispatch certification, B2B shipment consolidation
- **Downstream Relationships:** Quality Inspector, QA Dyeing Inspector, Silk Mark Officer, Store Inventory Manager
- **Snowflake ID:** `USR-LF-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.log-finishing.certified`
- **Neo4j Labels:** `LogFinishingSpecialist`, `User`, `FinishingOperator`
- **Relationships:**
  - `[:RECEIVES_FROM]→` QualityInspector
  - `[:RECEIVES_FROM]→` QADyeingInspector
  - `[:RECEIVES_FROM]→` SilkMarkOfficer
  - `[:FINISHES]→` Saree
  - `[:PACKS]→` Saree (anti-tarnish packaging)
  - `[:DISPATCHES_TO]→` StoreInventoryManager

### Role 8: ROLE-LOOM-HARNESS-SETTER
- **Display Name:** Loom Harness Setter
- **Email:** loom.harness@factory.com
- **Employee ID:** EMP-008
- **Entity ID Pattern:** `USR-LHS-{SNOWFLAKE}`
- **Phase Coverage:** Phase 6 (Beam Creation & Setup)
- **Pre-Process:** Warp Joiner, Warp Beam Preparation
- **Post-Process:** Harness certification, shed alignment validation
- **Downstream Relationships:** Warp Joiner, Warp Beam Preparation, Master Weaver, Petni Master
- **Snowflake ID:** `USR-LHS-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.loom-harness-setter.certified`
- **Neo4j Labels:** `LoomHarnessSetter`, `User`, `LoomTechnician`
- **Relationships:**
  - `[:SETS_UP]→` LoomHarness
  - `[:RECEIVES_FROM]→` WarpJoiner
  - `[:RECEIVES_FROM]→` WarpBeamPreparation
  - `[:ENABLES]→` MasterWeaver
  - `[:SUPPORTS]→` PetniMaster

### Role 9: ROLE-MASTER-COLORIST
- **Display Name:** Master Colorist
- **Email:** master.colorist@factory.com
- **Employee ID:** EMP-009
- **Entity ID Pattern:** `USR-MC-{SNOWFLAKE}`
- **Phase Coverage:** Phase 3 (Skein Dyeing & Color Certification)
- **Pre-Process:** Throwster/Twister, Silk Degumming Master
- **Post-Process:** Color certification, shade approval, recipe formulation
- **Downstream Relationships:** Throwster/Twister, Silk Degumming Master, Skein Dye Master, QA Dyeing Inspector
- **Snowflake ID:** `USR-MC-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.master-colorist.certified`
- **Neo4j Labels:** `MasterColorist`, `User`, `DyeSpecialist`
- **Relationships:**
  - `[:FORMULATES]→` DyeRecipe
  - `[:RECEIVES_FROM]→` ThrowsterTwister
  - `[:RECEIVES_FROM]→` SilkDegummingMaster
  - `[:SUPPLIES_TO]→` SkeinDyeMaster
  - `[:CERTIFIES]→` DyeLot

### Role 10: ROLE-MASTER-WEAVER
- **Display Name:** Master Weaver
- **Email:** master.weaver@factory.com
- **Employee ID:** EMP-010
- **Entity ID Pattern:** `USR-MW-{SNOWFLAKE}`
- **Phase Coverage:** Phase 7 (Weaving Execution)
- **Pre-Process:** Warp Joiner, Petni Master, Loom Harness Setter
- **Post-Process:** Weaving certification, yield calculation, first-pick verification
- **Downstream Relationships:** Warp Joiner, Petni Master, Loom Harness Setter, Assistant Weaver, Quality Inspector, SUP Loom Floor Supervisor
- **Snowflake ID:** `USR-MW-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.master-weaver.certified`
- **Neo4j Labels:** `MasterWeaver`, `User`, `LoomOperator`
- **Relationships:**
  - `[:OPERATES]→` Loom
  - `[:RECEIVES_FROM]→` WarpJoiner
  - `[:RECEIVES_FROM]→` PetniMaster
  - `[:ASSIGNS_TO]→` AssistantWeaver
  - `[:TRIGGERS]→` QualityInspector
  - `[:REPORTS_TO]→` SUP_LoomFloorSupervisor

### Role 11: ROLE-PETNI-MASTER
- **Display Name:** Petni Master
- **Email:** petni.master@factory.com
- **Employee ID:** EMP-011
- **Entity ID Pattern:** `USR-PM-{SNOWFLAKE}`
- **Phase Coverage:** Phase 7 (Petni Contrast Splicing)
- **Pre-Process:** Warp Joiner, Dye Lot certification
- **Post-Process:** Petni certification, border alignment, contrast inventory depletion
- **Downstream Relationships:** Warp Joiner, Master Weaver, Loom Harness Setter, Store Inventory Manager
- **Snowflake ID:** `USR-PM-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.petni-master.certified`
- **Neo4j Labels:** `PetniMaster`, `User`, `WarpSpecialist`
- **Relationships:**
  - `[:SPLICES]→` WarpSheet (body + border)
  - `[:RECEIVES_FROM]→` WarpJoiner
  - `[:SUPPLIES_TO]→` MasterWeaver
  - `[:ENABLES]→` LoomHarnessSetter
  - `[:DEPLETES]→` StoreInventoryManager (contrast yarn)

### Role 12: ROLE-PIRN-WINDERS
- **Display Name:** Pirn Winders
- **Email:** pirn.winders@factory.com
- **Employee ID:** EMP-012
- **Entity ID Pattern:** `USR-PW-{SNOWFLAKE}`
- **Phase Coverage:** Phase 4 (Yarn Winding & Preparation)
- **Pre-Process:** Filature Supplier, Throwster/Twister
- **Post-Process:** Pirn batch certification, weft density validation
- **Downstream Relationships:** Filature Supplier, Throwster/Twister, Master Weaver
- **Snowflake ID:** `USR-PW-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.pirn-winders.certified`
- **Neo4j Labels:** `PirnWinders`, `User`, `YarnPreparation`
- **Relationships:**
  - `[:WINDS]→` PirnBatch (`PIRN-BCH-{SNOWFLAKE}`)
  - `[:RECEIVES_FROM]→` ThrowsterTwister
  - `[:SUPPLIES_TO]→` MasterWeaver
  - `[:VALIDATES]→` WeftDensity

### Role 13: ROLE-QA-DYEING-INSPECTOR
- **Display Name:** QA Dyeing Inspector
- **Email:** qa.dyeing@factory.com
- **Employee ID:** EMP-013
- **Entity ID Pattern:** `USR-QDI-{SNOWFLAKE}`
- **Phase Coverage:** Phase 3 → Phase 8 transition (Dye QA → Finishing)
- **Pre-Process:** Quality Inspector, Skein Dye Master, Master Colorist
- **Post-Process:** Dye certification, color consistency verification, B2B order matcher
- **Downstream Relationships:** Quality Inspector, Skein Dye Master, Master Colorist, Silk Mark Officer, LOG Finishing Transit Specialist
- **Snowflake ID:** `USR-QDI-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.qa-dyeing-inspector.certified`
- **Neo4j Labels:** `QADyeingInspector`, `User`, `QAOfficer`
- **Relationships:**
  - `[:INSPECTS]→` DyeLot
  - `[:RECEIVES_FROM]→` SkeinDyeMaster
  - `[:RECEIVES_FROM]→` MasterColorist
  - `[:TRIGGERS]→` SilkMarkOfficer
  - `[:SUPPLIES_TO]→` LogFinishingSpecialist

### Role 14: ROLE-QUALITY-INSPECTOR
- **Display Name:** Quality Inspector
- **Email:** quality.inspector@factory.com
- **Employee ID:** EMP-014
- **Entity ID Pattern:** `USR-QI-{SNOWFLAKE}`
- **Phase Coverage:** Phase 8 (QA-3 Final Inspection)
- **Pre-Process:** Master Weaver, Assistant Weaver
- **Post-Process:** Quality certification, grading, B2B order auto-matcher
- **Downstream Relationships:** Master Weaver, Assistant Weaver, SUP Loom Floor Supervisor, QA Dyeing Inspector, Silk Mark Officer, LOG Finishing Transit Specialist
- **Snowflake ID:** `USR-QI-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.quality-inspector.certified`
- **Neo4j Labels:** `QualityInspector`, `User`, `QAOfficer`
- **Relationships:**
  - `[:INSPECTS]→` Saree
  - `[:RECEIVES_FROM]→` MasterWeaver
  - `[:RECEIVES_FROM]→` AssistantWeaver
  - `[:GRADES]→` Saree (GRADE_A/B/C/REJECTED)
  - `[:TRIGGERS]→` QADyeingInspector
  - `[:TRIGGERS]→` SilkMarkOfficer
  - `[:SUPPLIES_TO]→` LogFinishingSpecialist

### Role 15: ROLE-SILK-DEGUMMING-MASTER
- **Display Name:** Silk Degumming Master
- **Email:** silk.degumming@factory.com
- **Employee ID:** EMP-015
- **Entity ID Pattern:** `USR-SDM-{SNOWFLAKE}`
- **Phase Coverage:** Phase 2 (Degumming & Throwing)
- **Pre-Process:** Filature Supplier, Store Inventory Manager
- **Post-Process:** Degumming certification, sericin loss validation
- **Downstream Relationships:** Filature Supplier, Store Inventory Manager, Throwster/Twister, Master Colorist
- **Snowflake ID:** `USR-SDM-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.silk-degumming-master.certified`
- **Neo4j Labels:** `SilkDegummingMaster`, `User`, `ProcessingSpecialist`
- **Relationships:**
  - `[:PROCESSES]→` RawSilkLot
  - `[:RECEIVES_FROM]→` FilatureSupplier
  - `[:RECEIVES_FROM]→` StoreInventoryManager
  - `[:SUPPLIES_TO]→` ThrowsterTwister
  - `[:SUPPLIES_TO]→` MasterColorist

### Role 16: ROLE-SILK-GRADER
- **Display Name:** Silk Grader
- **Email:** silk.grader@factory.com
- **Employee ID:** EMP-016
- **Entity ID Pattern:** `USR-SG-{SNOWFLAKE}`
- **Phase Coverage:** Phase 1 (Raw Material Quality Testing)
- **Pre-Process:** Filature Supplier, Gate Clerk
- **Post-Process:** Silk grading certification, A/B/C classification
- **Downstream Relationships:** Filature Supplier, Store Inventory Manager, Silk Degumming Master
- **Snowflake ID:** `USR-SG-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.silk-grader.certified`
- **Neo4j Labels:** `SilkGrader`, `User`, `QAOfficer`
- **Relationships:**
  - `[:GRADES]→` RawSilkLot
  - `[:RECEIVES_FROM]→` FilatureSupplier
  - `[:RECEIVES_FROM]→` GateClerk
  - `[:SUPPLIES_TO]→` StoreInventoryManager
  - `[:TRIGGERS]→` SilkDegummingMaster

### Role 17: ROLE-SILK-MARK-OFFICER
- **Display Name:** Silk Mark Officer
- **Email:** silk.mark@factory.com
- **Employee ID:** EMP-017
- **Entity ID Pattern:** `USR-SMO-{SNOWFLAKE}`
- **Phase Coverage:** Phase 9 (Silk Mark Certification)
- **Pre-Process:** Quality Inspector, QA Dyeing Inspector, SUP Loom Floor Supervisor
- **Post-Process:** Silk Mark certification, hologram tag serialization, anti-adulteration lock
- **Downstream Relationships:** Quality Inspector, QA Dyeing Inspector, SUP Loom Floor Supervisor, LOG Finishing Transit Specialist, Store Inventory Manager
- **Snowflake ID:** `USR-SMO-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.silk-mark-officer.certified`
- **Neo4j Labels:** `SilkMarkOfficer`, `User`, `ComplianceOfficer`
- **Relationships:**
  - `[:CERTIFIES]→` Saree (Silk Mark)
  - `[:RECEIVES_FROM]→` QualityInspector
  - `[:RECEIVES_FROM]→` QADyeingInspector
  - `[:RECEIVES_FROM]→` SUP_LoomFloorSupervisor
  - `[:ISSUES]→` SilkMarkTag (`SMK-{SNOWFLAKE}`)
  - `[:SUPPLIES_TO]→` LogFinishingSpecialist
  - `[:SUPPLIES_TO]→` StoreInventoryManager

### Role 18: ROLE-SKEIN-DYE-MASTER
- **Display Name:** Skein Dye Master
- **Email:** skein.dye@factory.com
- **Employee ID:** EMP-018
- **Entity ID Pattern:** `USR-SDM-{SNOWFLAKE}`
- **Phase Coverage:** Phase 3 (Skein Dyeing & Color Certification)
- **Pre-Process:** Silk Degumming Master, Throwster/Twister, Master Colorist
- **Post-Process:** Skein dye certification, temperature profiling, mass balance
- **Downstream Relationships:** Silk Degumming Master, Throwster/Twister, Master Colorist, QA Dyeing Inspector, Store Inventory Manager
- **Snowflake ID:** `USR-SDM-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.skein-dye-master.certified`
- **Neo4j Labels:** `SkeinDyeMaster`, `User`, `DyeSpecialist`
- **Relationships:**
  - `[:DYES]→` SkeinLot
  - `[:RECEIVES_FROM]→` SilkDegummingMaster
  - `[:RECEIVES_FROM]→` ThrowsterTwister
  - `[:RECEIVES_RECIPE_FROM]→` MasterColorist
  - `[:SUPPLIES_TO]→` QADyeingInspector
  - `[:SUPPLIES_TO]→` StoreInventoryManager

### Role 19: ROLE-STORE-INVENTORY-MANAGER
- **Display Name:** Store Inventory Manager
- **Email:** store.inventory@factory.com
- **Employee ID:** EMP-019
- **Entity ID Pattern:** `USR-SIM-{SNOWFLAKE}`
- **Phase Coverage:** Phase 1 (Inward Stock) → Phase 9 (Outward Dispatch)
- **Pre-Process:** Filature Supplier, Zari Inspector, LOG Finishing Transit Specialist, Silk Mark Officer
- **Post-Process:** Inventory certification, stock release, vault reconciliation
- **Downstream Relationships:** Filature Supplier, Zari Inspector, LOG Finishing Transit Specialist, Silk Mark Officer, SUP Loom Floor Supervisor, Master Weaver, Warp Beam Preparation
- **Snowflake ID:** `USR-SIM-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.store-inventory-manager.certified`
- **Neo4j Labels:** `StoreInventoryManager`, `User`, `WarehouseManager`
- **Relationships:**
  - `[:MANAGES]→` InventoryBin
  - `[:RECEIVES_FROM]→` FilatureSupplier
  - `[:RECEIVES_FROM]→` ZariInspector
  - `[:RECEIVES_FROM]→` LogFinishingSpecialist
  - `[:RECEIVES_FROM]→` SilkMarkOfficer
  - `[:ISSUES_TO]→` WarpBeamPreparation
  - `[:ISSUES_TO]→` MasterWeaver
  - `[:RECONCILES]→` VaultZari

### Role 20: ROLE-SUP-LOOM-FLOOR-SUPERVISOR
- **Display Name:** SUP Loom Floor Supervisor
- **Email:** sup.supervisor@factory.com
- **Employee ID:** EMP-020
- **Entity ID Pattern:** `USR-SUP-{SNOWFLAKE}`
- **Phase Coverage:** Phase 6 → Phase 9 (Full Floor Oversight)
- **Pre-Process:** All upstream roles
- **Post-Process:** Shift handover clearance, batch release authorization, root cause audits
- **Downstream Relationships:** All production floor roles (Master Weaver, Petni Master, Warp Joiner, Loom Harness Setter, Assistant Weaver, Quality Inspector, QA Dyeing Inspector, Silk Mark Officer)
- **Snowflake ID:** `USR-SUP-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.sup-supervisor.certified`
- **Neo4j Labels:** `SUPLoomFloorSupervisor`, `User`, `ProductionSupervisor`
- **Relationships:**
  - `[:SUPERVISES]→` MasterWeaver
  - `[:SUPERVISES]→` PetniMaster
  - `[:SUPERVISES]→` WarpJoiner
  - `[:SUPERVISES]→` LoomHarnessSetter
  - `[:SUPERVISES]→` AssistantWeaver
  - `[:AUDITS]→` QualityInspector
  - `[:AUDITS]→` QADyeingInspector
  - `[:AUDITS]→` SilkMarkOfficer
  - `[:AUTHORIZES]→` BatchRelease

### Role 21: ROLE-THROWSTER-TWISTER
- **Display Name:** Throwster/Twister
- **Email:** throwster.twister@factory.com
- **Employee ID:** EMP-021
- **Entity ID Pattern:** `VND-THR-{SNOWFLAKE}`
- **Phase Coverage:** Phase 2 (Degumming & Throwing)
- **Pre-Process:** Silk Degumming Master
- **Post-Process:** Thrown silk lot certification, TPI control
- **Downstream Relationships:** Silk Degumming Master, Bobbin Winder, Pirn Winders, Master Colorist, Skein Dye Master
- **Snowflake ID:** `VND-THR-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.throwster-twister.certified`
- **Neo4j Labels:** `ThrowsterTwister`, `Vendor`, `YarnProcessor`
- **Relationships:**
  - `[:TWISTS]→` ThrownSilkLot (`THROWN-SILK-{SNOWFLAKE}`)
  - `[:RECEIVES_FROM]→` SilkDegummingMaster
  - `[:SUPPLIES_TO]→` BobbinWinder
  - `[:SUPPLIES_TO]→` PirnWinders
  - `[:SUPPLIES_TO]→` MasterColorist
  - `[:SUPPLIES_TO]→` SkeinDyeMaster

### Role 22: ROLE-WARP-BEAM-PREPARATION
- **Display Name:** Warp Beam Preparation Specialist (80 Saree Length)
- **Email:** warp.beam@factory.com
- **Employee ID:** EMP-022
- **Entity ID Pattern:** `USR-WBP-{SNOWFLAKE}`
- **Phase Coverage:** Phase 5 & 6 (Beam Creation & Saree Instantiation)
- **Pre-Process:** Bobbin Winder, Pirn Winders, Graph Drafter, Card Puncher
- **Post-Process:** Warp beam certification, 80-saree instantiation, mass balance
- **Downstream Relationships:** Bobbin Winder, Pirn Winders, Graph Drafter, Card Puncher, Petni Master, Master Weaver, Store Inventory Manager
- **Snowflake ID:** `USR-WBP-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.warp-beam-preparation.certified`
- **Neo4j Labels:** `WarpBeamPreparation`, `User`, `WarpSpecialist`
- **Relationships:**
  - `[:CREATES]→` WarpBeam (`WARP-BCH-{LOOM_ID}-{DATE}-{SEQ}`)
  - `[:RECEIVES_FROM]→` BobbinWinder
  - `[:RECEIVES_FROM]→` PirnWinders
  - `[:USES_DESIGN_FROM]→` CardPuncher
  - `[:SPAWNS]→` Saree (80 children: S01-S80)
  - `[:SUPPLIES_TO]→` PetniMaster
  - `[:SUPPLIES_TO]→` MasterWeaver
  - `[:REPORTS_TO]→` StoreInventoryManager

### Role 23: ROLE-WARP-JOINER
- **Display Name:** Warp Joiner
- **Email:** warp.joiner@factory.com
- **Employee ID:** EMP-023
- **Entity ID Pattern:** `USR-WJ-{SNOWFLAKE}`
- **Phase Coverage:** Phase 6 (Warp Joining & Tie-in)
- **Pre-Process:** Warp Beam Preparation, Store Inventory Manager
- **Post-Process:** Warp joining certification, knotting quality audit
- **Downstream Relationships:** Warp Beam Preparation, Store Inventory Manager, Loom Harness Setter, Petni Master, Master Weaver
- **Snowflake ID:** `USR-WJ-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.warp-joiner.certified`
- **Neo4j Labels:** `WarpJoiner`, `User`, `WarpSpecialist`
- **Relationships:**
  - `[:JOINS]→` WarpSheet
  - `[:RECEIVES_FROM]→` WarpBeamPreparation
  - `[:RECEIVES_FROM]→` StoreInventoryManager
  - `[:SUPPLIES_TO]→` LoomHarnessSetter
  - `[:SUPPLIES_TO]→` PetniMaster
  - `[:SUPPLIES_TO]→` MasterWeaver

### Role 24: ROLE-ZARI-INSPECTOR
- **Display Name:** Zari Inspector / Zari Refinery Specialist
- **Email:** zari.inspector@factory.com
- **Employee ID:** EMP-024
- **Entity ID Pattern:** `USR-ZI-{SNOWFLAKE}`
- **Phase Coverage:** Phase 1 (Raw Material Intake) → Phase 2 (Zari Refinery)
- **Pre-Process:** Filature Supplier, Store Inventory Manager
- **Post-Process:** Zari assay certification, purity verification, XRF audit
- **Downstream Relationships:** Filature Supplier, Store Inventory Manager, Throwster/Twister, Master Weaver, Silk Mark Officer
- **Snowflake ID:** `USR-ZI-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.zari-inspector.certified`
- **Neo4j Labels:** `ZariInspector`, `User`, `QAOfficer`
- **Relationships:**
  - `[:INSPECTS]→` ZariSpool
  - `[:RECEIVES_FROM]→` FilatureSupplier
  - `[:RECEIVES_FROM]→` StoreInventoryManager
  - `[:REFINES]→` ZariBatch
  - `[:SUPPLIES_TO]→` ThrowsterTwister
  - `[:SUPPLIES_TO]→` MasterWeaver
  - `[:TRIGGERS]→` SilkMarkOfficer

### Role 25: ROLE-DESIGN-GENERATOR
- **Display Name:** Design Generator
- **Email:** design.generator@factory.com
- **Employee ID:** EMP-025
- **Entity ID Pattern:** `USR-DG-{SNOWFLAKE}`
- **Phase Coverage:** Phase 5 (Jacquard Compilation)
- **Pre-Process:** SKU Manager, Sales forecast
- **Post-Process:** Design certification, GAN generation, sales feedback loop
- **Downstream Relationships:** SKU Manager, Graph Drafter, Card Puncher
- **Snowflake ID:** `USR-DG-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.design-generator.certified`
- **Neo4j Labels:** `DesignGenerator`, `User`, `AIDesigner`
- **Relationships:**
  - `[:GENERATES]→` JacquardFile (`DES-{HOOKS}-{PATTERN_HASH}`)
  - `[:RECEIVES_FROM]→` SKUManager
  - `[:SUPPLIES_TO]→` GraphDrafter
  - `[:SUPPLIES_TO]→` CardPuncher
  - `[:LEARNS_FROM]→` SalesFeedback

### Role 26: ROLE-BUY-BACK-MANAGER
- **Display Name:** Buy-Back Manager
- **Email:** buyback.manager@factory.com
- **Employee ID:** EMP-026
- **Entity ID Pattern:** `USR-BBM-{SNOWFLAKE}`
- **Phase Coverage:** Post-Phase 9 (After Dispatch)
- **Pre-Process:** LOG Finishing Transit Specialist, Store Inventory Manager
- **Post-Process:** Buy-back risk assessment, warranty claims
- **Downstream Relationships:** LOG Finishing Transit Specialist, Store Inventory Manager, Quality Inspector, Silk Mark Officer
- **Snowflake ID:** `USR-BBM-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.buy-back-manager.certified`
- **Neo4j Labels:** `BuyBackManager`, `User`, `CustomerService`
- **Relationships:**
  - `[:MANAGES]→` BuyBackGuarantee
  - `[:RECEIVES_FROM]→` LogFinishingSpecialist
  - `[:RECEIVES_FROM]→` StoreInventoryManager
  - `[:REFERENCES]→` QualityInspector
  - `[:REFERENCES]→` SilkMarkOfficer
  - `[:TRIGGERS]→` RefundProcess

### Role 27: ROLE-GUILD-MANAGER
- **Display Name:** Guild Manager
- **Email:** guild.manager@factory.com
- **Employee ID:** EMP-027
- **Entity ID Pattern:** `USR-GM-{SNOWFLAKE}`
- **Phase Coverage:** Cross-cutting (Weaver Guild Management)
- **Pre-Process:** Master Weaver, Assistant Weaver
- **Post-Process:** Guild certification, weaver skill verification
- **Downstream Relationships:** Master Weaver, Assistant Weaver, SUP Loom Floor Supervisor
- **Snowflake ID:** `USR-GM-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.guild-manager.certified`
- **Neo4j Labels:** `GuildManager`, `User`, `HRManager`
- **Relationships:**
  - `[:MANAGES]→` Guild
  - `[:CERTIFIES]→` WeaverSkill
  - `[:RECEIVES_FROM]→` MasterWeaver
  - `[:RECEIVES_FROM]→` AssistantWeaver
  - `[:COORDINATES_WITH]→` SUP_LoomFloorSupervisor

### Role 28: ROLE-IOT-DEVICE-MANAGER
- **Display Name:** IoT Device Manager
- **Email:** iot.device@factory.com
- **Employee ID:** EMP-028
- **Entity ID Pattern:** `USR-IOT-{SNOWFLAKE}`
- **Phase Coverage:** All phases (device monitoring)
- **Pre-Process:** None (infrastructure)
- **Post-Process:** Device telemetry, edge AI model deployment
- **Downstream Relationships:** All roles (via edge controllers)
- **Snowflake ID:** `USR-IOT-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `iot.{factory}.telemetry`, `iot.{factory}.alerts`
- **Neo4j Labels:** `IoTDeviceManager`, `User`, `DevOpsEngineer`
- **Relationships:**
  - `[:MANAGES]→` EdgeController
  - `[:MONITORS]→` LoomTelemetry
  - `[:DEPLOYS_TO]→` LoomController
  - `[:SUPPORTS]→` AllRoles (via MQTT)

### Role 29: ROLE-LOCALIZATION-MANAGER
- **Display Name:** Localization Manager
- **Email:** localization.manager@factory.com
- **Employee ID:** EMP-029
- **Entity ID Pattern:** `USR-LM-{SNOWFLAKE}`
- **Phase Coverage:** Cross-cutting (UI/UX Localization)
- **Pre-Process:** None (supporting role)
- **Post-Process:** Translation certification, TTS audio generation
- **Downstream Relationships:** All dashboard users
- **Snowflake ID:** `USR-LM-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `localization.{factory}.updates`
- **Neo4j Labels:** `LocalizationManager`, `User`, `ContentManager`
- **Relationships:**
  - `[:TRANSLATES]→` I18nKeys
  - `[:GENERATES]→` VoiceAudioCache
  - `[:SUPPORTS]→` AllDashboards

### Role 30: ROLE-SKU-MANAGER
- **Display Name:** SKU Manager
- **Email:** sku.manager@factory.com
- **Employee ID:** EMP-030
- **Entity ID Pattern:** `USR-SKU-{SNOWFLAKE}`
- **Phase Coverage:** Pre-Phase 1 (Product Definition)
- **Pre-Process:** Design Generator, Sales forecast
- **Post-Process:** SKU certification, B2B order linkage
- **Downstream Relationships:** Design Generator, Graph Drafter, Store Inventory Manager, LOG Finishing Transit Specialist
- **Snowflake ID:** `USR-SKU-{timestamp}-{datacenter}-{worker}-{sequence}`
- **Kafka Topics:** `lot.{factory}.sku-manager.certified`
- **Neo4j Labels:** `SKUManager`, `User`, `ProductManager`
- **Relationships:**
  - `[:DEFINES]→` SKU
  - `[:RECEIVES_FROM]→` DesignGenerator
  - `[:SUPPLIES_TO]→` GraphDrafter
  - `[:TRIGGERS]→` StoreInventoryManager
  - `[:TRIGGERS]→` LogFinishingSpecialist

---

## 5. End-to-End Role Relationship Matrix

### Upstream → Downstream Flow

```
Filature Supplier (VND-FIL)
  ├──→ Gate Clerk / QC Inspector (QA-1)
  │     ├──→ Silk Grader (GRADE)
  │     │     └──→ Silk Degumming Master (VND-DEG)
  │     │           ├──→ Throwster/Twister (VND-THR)
  │     │           │     ├──→ Bobbin Winder (BOBBIN-BCH)
  │     │           │     │     └──→ Warp Beam Preparation (WARP-BCH)
  │     │           │     └──→ Pirn Winders (PIRN-BCH)
  │     │           │           └──→ Master Weaver (LOOM)
  │     │           └──→ Master Colorist (DYE RECIPE)
  │     │                 └──→ Skein Dye Master (DYE-LOT)
  │     │                       └──→ QA Dyeing Inspector (QA-2)
  │     └──→ Zari Inspector (ZARI ASSAY)
  │           └──→ Store Inventory Manager (VAULT)
  ├──→ Store Inventory Manager (RAW-LOT)
  └──→ Warp Joiner (WARP SHEET)
        ├──→ Loom Harness Setter (HARNESS)
        │     └──→ Master Weaver (LOOM SETUP)
        │           ├──→ Petni Master (PALLU SPLICE)
        │           │     └──→ Master Weaver (WEAVING)
        │           │           ├──→ Assistant Weaver (SHIFT)
        │           │           └──→ SUP Loom Floor Supervisor (BATCH RELEASE)
        │           │                 ├──→ Quality Inspector (QA-3)
        │           │                 │     ├──→ QA Dyeing Inspector (COLOR QA)
        │           │                 │     │     └──→ Silk Mark Officer (COMPLIANCE)
        │           │                 │     │           ├──→ LOG Finishing Transit Specialist (PACK/DISPATCH)
        │           │                 │     │           │     └──→ Store Inventory Manager (DISPATCH WAREHOUSE)
        │           │                 │     │           └──→ Store Inventory Manager (INVENTORY)
        │           │                 │     └──→ LOG Finishing Transit Specialist (DIRECT)
        │           │                 │           └──→ Store Inventory Manager (DISPATCH WAREHOUSE)
        │           │                 └──→ Store Inventory Manager (GREY/OFF-CUT)
        │           └──→ Store Inventory Manager (WARP YARN ISSUE)
        └──→ Master Weaver (DIRECT WARP JOIN)
              └──→ Store Inventory Manager (WARP YARN ISSUE)

Design Generator (GAN)
  └──→ SKU Manager (PRODUCT DEFINITION)
        └──→ Graph Drafter (DES-{HOOKS})
              └──→ Card Puncher (PROGRAMMING)
                    └──→ Loom Harness Setter / Master Weaver (EXECUTION)

Buy-Back Manager (POST-DISPATCH)
  └──→ References: Quality Inspector, Silk Mark Officer, LOG Finishing Transit Specialist

Guild Manager (CROSS-CUTTING)
  └──→ References: Master Weaver, Assistant Weaver, SUP Loom Floor Supervisor

IoT Device Manager (INFRASTRUCTURE)
  └──→ Monitors: All Loom Controllers, Edge AI, Sensors

Localization Manager (SUPPORT)
  └──→ Supports: All Dashboards, UI Text, TTS
```

### Cross-Role Kafka Topic Map

| Role | Publishes To | Subscribes To |
|---|---|---|
| System Admin | `admin.*` | All topics |
| Filature Supplier | `lot.{factory}.filature-supplier.certified` | — |
| Gate Clerk | `lot.{factory}.gate-clerk.certified` | — |
| Silk Grader | `lot.{factory}.silk-grader.certified` | `lot.{factory}.filature-supplier.certified` |
| QC Inspector | `lot.{factory}.qc-inspector.certified` | `lot.{factory}.filature-supplier.certified` |
| Zari Inspector | `lot.{factory}.zari-inspector.certified` | `lot.{factory}.filature-supplier.certified` |
| Silk Degumming Master | `lot.{factory}.silk-degumming-master.certified` | `lot.{factory}.silk-grader.certified` |
| Throwster/Twister | `lot.{factory}.throwster-twister.certified` | `lot.{factory}.silk-degumming-master.certified` |
| Master Colorist | `lot.{factory}.master-colorist.certified` | `lot.{factory}.throwster-twister.certified` |
| Skein Dye Master | `lot.{factory}.skein-dye-master.certified` | `lot.{factory}.master-colorist.certified` |
| Bobbin Winder | `lot.{factory}.bobbin-winder.certified` | `lot.{factory}.throwster-twister.certified` |
| Pirn Winders | `lot.{factory}.pirn-winders.certified` | `lot.{factory}.throwster-twister.certified` |
| Graph Drafter | `lot.{factory}.graph-drafter.certified` | `lot.{factory}.design-generator.certified` |
| Design Generator | `lot.{factory}.design-generator.certified` | `lot.{factory}.sku-manager.certified` |
| SKU Manager | `lot.{factory}.sku-manager.certified` | Sales forecast |
| Card Puncher | `lot.{factory}.card-puncher.certified` | `lot.{factory}.graph-drafter.certified` |
| Warp Beam Preparation | `lot.{factory}.warp-beam-preparation.certified` | `lot.{factory}.bobbin-winder.certified`, `lot.{factory}.pirn-winders.certified`, `lot.{factory}.card-puncher.certified` |
| Store Inventory Manager | `lot.{factory}.store-inventory-manager.certified` | `lot.{factory}.filature-supplier.certified`, `lot.{factory}.zari-inspector.certified`, `lot.{factory}.log-finishing.certified`, `lot.{factory}.silk-mark-officer.certified` |
| Warp Joiner | `lot.{factory}.warp-joiner.certified` | `lot.{factory}.warp-beam-preparation.certified`, `lot.{factory}.store-inventory-manager.certified` |
| Loom Harness Setter | `lot.{factory}.loom-harness-setter.certified` | `lot.{factory}.warp-joiner.certified` |
| Petni Master | `lot.{factory}.petni-master.certified` | `lot.{factory}.warp-joiner.certified` |
| Master Weaver | `lot.{factory}.master-weaver.certified` | `lot.{factory}.loom-harness-setter.certified`, `lot.{factory}.petni-master.certified`, `lot.{factory}.store-inventory-manager.certified` |
| Assistant Weaver | `lot.{factory}.assistant-weaver.certified` | `lot.{factory}.master-weaver.certified` |
| SUP Loom Floor Supervisor | `lot.{factory}.sup-supervisor.certified` | All production roles |
| Quality Inspector | `lot.{factory}.quality-inspector.certified` | `lot.{factory}.master-weaver.certified`, `lot.{factory}.assistant-weaver.certified` |
| QA Dyeing Inspector | `lot.{factory}.qa-dyeing-inspector.certified` | `lot.{factory}.quality-inspector.certified` |
| Silk Mark Officer | `lot.{factory}.silk-mark-officer.certified` | `lot.{factory}.quality-inspector.certified`, `lot.{factory}.qa-dyeing-inspector.certified`, `lot.{factory}.sup-supervisor.certified` |
| LOG Finishing Transit Specialist | `lot.{factory}.log-finishing.certified` | `lot.{factory}.quality-inspector.certified`, `lot.{factory}.silk-mark-officer.certified` |
| Buy-Back Manager | `lot.{factory}.buy-back-manager.certified` | `lot.{factory}.log-finishing.certified`, `lot.{factory}.store-inventory-manager.certified` |
| Guild Manager | `lot.{factory}.guild-manager.certified` | `lot.{factory}.master-weaver.certified`, `lot.{factory}.assistant-weaver.certified` |
| IoT Device Manager | `iot.{factory}.telemetry`, `iot.{factory}.alerts` | All edge devices |
| Localization Manager | `localization.{factory}.updates` | — |

---

## 6. Neo4j Graph Schema (Full Relationship Map)

```cypher
// Users and Roles
(u:User {id: "USR-{ROLE}-{SNOWFLAKE}"})-[:HAS_ROLE]->(r:Role {role_id: "ROLE-{ROLE}"})

// Raw Material Flow
(:FilatureSupplier)-[:SUPPLIES]->(:RawMaterialLot {id: "RAW-LOT-{VENDOR_ID}-{TIMESTAMP}"})
(:RawMaterialLot)-[:GRADED_BY]->(:SilkGrader)
(:RawMaterialLot)-[:INSPECTED_BY]->(:QCInspector)
(:RawMaterialLot)-[:STORED_IN]->(:InventoryBin)
(:RawMaterialLot)-[:DEGUMMED_BY]->(:SilkDegummingMaster)
(:RawMaterialLot)-[:THROWN_BY]->(:ThrownLot {id: "THROWN-SILK-{SNOWFLAKE}"})

// Zari Flow
(:FilatureSupplier)-[:SUPPLIES_ZARI]->(:ZariSpool)
(:ZariSpool)-[:INSPECTED_BY]->(:ZariInspector)
(:ZariSpool)-[:STORED_IN_VAULT]->(:VaultBin)
(:ZariSpool)-[:REFINED_BY]->(:ZariBatch)

// Yarn Preparation
(:ThrownLot)-[:TWISTED_BY]->(:ThrowsterTwister)
(:ThrowsterTwister)-[:SUPPLIES_TO]->(:BobbinWinder)
(:ThrowsterTwister)-[:SUPPLIES_TO]->(:PirnWinders)
(:BobbinWinder)-[:WINDS]->(:BobbinBatch {id: "BOBBIN-BCH-{SNOWFLAKE}"})
(:PirnWinders)-[:WINDS]->(:PirnBatch {id: "PIRN-BCH-{SNOWFLAKE}"})

// Dyeing Flow
(:ThrownLot)-[:SUPPLIED_TO]->(:MasterColorist)
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

// Inventory Issuance
(:InventoryBin)-[:ISSUED_TO]->(:WarpBeamPreparation)
(:InventoryBin)-[:ISSUED_TO]->(:MasterWeaver)
(:InventoryBin)-[:ISSUED_TO]->(:StoreInventoryManager)

// Warp Joining & Harness
(:WarpBeam)-[:JOINED_BY]->(:WarpJoiner)
(:WarpJoiner)-[:SUPPLIES_TO]->(:LoomHarnessSetter)
(:WarpJoiner)-[:SUPPLIES_TO]->(:PetniMaster)
(:LoomHarnessSetter)-[:SETS_UP]->(:LoomHarness)

// Weaving Execution
(:LoomHarness)-[:USED_BY]->(:MasterWeaver)
(:PetniMaster)-[:SPLICES]->(:WarpSheet)
(:WarpSheet)-[:WOVEN_BY]->(:MasterWeaver)
(:MasterWeaver)-[:ASSIGNS_TO]->(:AssistantWeaver)
(:MasterWeaver)-[:REPORTS_TO]->(:SUPLoomFloorSupervisor)

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

// Buy-Back & Guild
(:Saree)-[:COVERED_BY]->(:BuyBackGuarantee)
(:BuyBackManager)-[:MANAGES]->(:BuyBackGuarantee)
(:MasterWeaver)-[:MEMBER_OF]->(:Guild)
(:AssistantWeaver)-[:MEMBER_OF]->(:Guild)
(:GuildManager)-[:MANAGES]->(:Guild)

// IoT & Localization
(:EdgeController)-[:MONITORED_BY]->(:IoTDeviceManager)
(:Loom)-[:CONNECTED_TO]->(:EdgeController)
(:Dashboard)-[:LOCALIZED_BY]->(:LocalizationManager)
```

---

## 7. Back-Tracing Example: Complete Lineage

**Query:** `back_trace_master_saree("KNC-2400-BR-09-S42")`

**Expected Response (from Neo4j):**
```json
{
  "source": "NEO4J_GRAPH_DB",
  "lineage": {
    "saree": {
      "id": "KNC-2400-BR-09-S42",
      "grade": "GRADE_A_PREMIUM",
      "length_meters": 6.32,
      "silk_mark_tag": "SMK-8293746192",
      "status": "DISPATCHED"
    },
    "warp_batch": {
      "id": "KNC-2400-BR-09",
      "loom_id": "LOOM-2400-001",
      "beam_date": "2024-08-15",
      "total_sarees": 80
    },
    "petni_batch": {
      "id": "PETNI-KNC-2400-001",
      "border_yarn_lot": "DYE-LOT-BRD-001"
    },
    "dye_warp": {
      "id": "DYE-LOT-WRP-001",
      "color_code": "C-2400-BR-09",
      "qa2_status": "QA2_CERTIFIED"
    },
    "dye_border": {
      "id": "DYE-LOT-BRD-001",
      "color_code": "C-2400-BRD-09",
      "qa2_status": "QA2_CERTIFIED"
    },
    "thrown_lot": {
      "id": "THROWN-SILK-8823471",
      "tpi": 1200,
      "ply_count": 2
    },
    "raw_silk": {
      "id": "RAW-LOT-FIL-001-20240815",
      "supplier": "VND-FIL-001",
      "grade": "A"
    },
    "raw_zari": {
      "id": "RAW-LOT-ZARI-001",
      "supplier": "VND-ZARI-001",
      "purity": "PURE_GOLD_SILVER_TESTED_ZARI"
    }
  }
}
```

---

## 8. Storage & Caching Strategy

| Data Type | Primary Store | Cache | Reason |
|---|---|---|---|
| User sessions, JWT blacklist | CockroachDB | Redis | Fast auth lookups |
| Production lot states | CockroachDB | Redis | High-frequency writes |
| Workflow transitions | CockroachDB | — | Audit trail |
| Saree lineage graph | Neo4j | Redis Trace Cache | <15ms back-trace |
| Scanner logs | CockroachDB (sharded) | Kafka | High write throughput |
| AI inference results | CockroachDB | Redis | Fast read for dashboards |
| Design files | CockroachDB + S3 | Redis | Binary + metadata |
| IoT telemetry | CockroachDB (time-series) | Kafka + Redis | Real-time + historical |
| i18n translations | CockroachDB | Redis | Fast UI rendering |
| Silk Mark tags | CockroachDB | Redis | Fraud prevention lookups |
| Inventory bins | CockroachDB | Redis | Real-time stock checks |

---

## 9. Event-Driven CQRS Flow

**Command Side (Write):**
```
POST /api/v1/{role}/logs
  → Validate guardrails
  → Write to CockroachDB (Write DB)
  → Publish event to Kafka topic
  → Neo4j graph update (async)
  → Redis cache invalidation (async)
```

**Query Side (Read):**
```
GET /api/v1/{role}/logs
  → Read from CockroachDB (Read Replica)
  → Check Redis cache first
  → Fallback to Neo4j if graph query needed
  → Return aggregated result
```

---

*This document is the single source of truth for all 30 role profiles, their Snowflake ID patterns, phase coverage, guardrails, and end-to-end relationships in the Silk Saree ERP system.*
