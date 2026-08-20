-- ============================================================
-- Migration: 002_workflow_transitions.sql
-- Sequential role transitions for the 24-step state machine
-- ============================================================

-- Phase 1: Raw Material Processing
INSERT INTO workflow_transitions (from_role_id, to_role_id, required_certificate_step, allowed_verdicts) VALUES
((SELECT id FROM roles WHERE role_id = 'ROLE-FILATURE-SUPPLIER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-ZARI-INSPECTOR'), 'ZariDefectDetection', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-ZARI-INSPECTOR'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-SILK-GRADER'), 'ZariDefectDetection', '["PASS"]'),

-- Phase 2: Dyeing & Color Processing
((SELECT id FROM roles WHERE role_id = 'ROLE-SILK-GRADER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-SKEIN-DYE-MASTER'), 'GradingCertification', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-SKEIN-DYE-MASTER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-QA-DYEING-INSPECTOR'), 'DyeColoringDefectDetection', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-QA-DYEING-INSPECTOR'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-SILK-DEGUMMING-MASTER'), 'DyeColoringDefectDetection', '["PASS"]'),

-- Phase 3: Marking & Inventory
((SELECT id FROM roles WHERE role_id = 'ROLE-SILK-DEGUMMING-MASTER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-SILK-MARK-OFFICER'), 'DegummingCertification', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-SILK-MARK-OFFICER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-STORE-INVENTORY-MANAGER'), 'TraceabilityCertification', '["PASS"]'),

-- Phase 4: Warp Preparation
((SELECT id FROM roles WHERE role_id = 'ROLE-STORE-INVENTORY-MANAGER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-WARP-BEAM-PREPARATION'), 'InventoryCertification', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-WARP-BEAM-PREPARATION'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-WARP-JOINER'), 'WarpDefectDetection', '["PASS"]'),

-- Phase 5: Design & Programming
((SELECT id FROM roles WHERE role_id = 'ROLE-WARP-JOINER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-GRAPH-DRAFTER'), 'WarpAlignmentCertification', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-GRAPH-DRAFTER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-CARD-PUNCHER'), 'DesignValidation', '["PASS"]'),

-- Phase 6: Loom Setup & Thread Preparation
((SELECT id FROM roles WHERE role_id = 'ROLE-CARD-PUNCHER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-LOOM-HARNESS-SETTER'), 'PatternCertification', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-LOOM-HARNESS-SETTER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-BOBBIN-WINDER'), 'HarnessAlignmentCertification', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-BOBBIN-WINDER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-PIRN-WINDERS'), 'ZariDefectDetection', '["PASS"]'),

-- Phase 7: Weaving
((SELECT id FROM roles WHERE role_id = 'ROLE-PIRN-WINDERS'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-PETNI-MASTER'), 'ZariDefectDetection', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-PETNI-MASTER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-THROWSTER-TWISTER'), 'ZariDefectDetection', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-THROWSTER-TWISTER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-MASTER-WEAVER'), 'ZariDefectDetection', '["PASS"]'),

-- Phase 8: Finishing & Final Quality
((SELECT id FROM roles WHERE role_id = 'ROLE-MASTER-WEAVER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-LOG-FINISHING'), 'FabricDefectDetection', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-LOG-FINISHING'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-QUALITY-INSPECTOR'), 'FabricDefectDetection', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-QUALITY-INSPECTOR'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-QA-DYEING-INSPECTOR'), 'ComprehensiveInspection', '["PASS"]'),

-- Phase 9: Final Review & Completion
((SELECT id FROM roles WHERE role_id = 'ROLE-QA-DYEING-INSPECTOR'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-MASTER-COLORIST'), 'FinalQAApproval', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-MASTER-COLORIST'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-ZARI-INSPECTOR'), 'DyeColoringDefectDetection', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-ZARI-INSPECTOR'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-SUP-LOOM-FLOOR-SUPERVISOR'), 'ZariDefectDetection', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-SUP-LOOM-FLOOR-SUPERVISOR'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-ASSISTANT-WEAVER'), 'ProductionSummaryCertification', '["PASS"]'),
((SELECT id FROM roles WHERE role_id = 'ROLE-ASSISTANT-WEAVER'), 
 (SELECT id FROM roles WHERE role_id = 'ROLE-SYSTEM-ADMIN'), 'WeavingDefectDetection', '["PASS"]');
