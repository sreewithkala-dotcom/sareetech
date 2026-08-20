#!/usr/bin/env python3
"""
Database Seeder
Populates initial data for testing and development.
"""
import psycopg2
import os
from datetime import datetime, timedelta
import uuid

def get_db():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'silk_erp'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres')
    )

def seed():
    conn = get_db()
    cur = conn.cursor()

    print("Seeding database...")

    # Create admin user
    admin_password_hash = "pbkdf2:sha256:260000$admin123$5e884898da28047151d0e56f8dc6292773603d0d6aabbddc7f5b8a9e3f3c3b3a"  # password: admin123
    cur.execute("""
        INSERT INTO users (email, password_hash, role_id, factory_node_id, full_name, employee_id)
        SELECT 'admin@factory.com', %s, id, 'FACT-BLR-01', 'System Admin', 'EMP-001'
        FROM roles WHERE role_id = 'ROLE-SYSTEM-ADMIN'
        ON CONFLICT (email) DO NOTHING
    """, (admin_password_hash,))

    # Create sample users for each role
    roles = [
        ('ROLE-ASSISTANT-WEAVER', 'assistant.weaver@factory.com', 'Assistant Weaver', 'EMP-002'),
        ('ROLE-BOBBIN-WINDER', 'bobbin.winder@factory.com', 'Bobbin Winder', 'EMP-003'),
        ('ROLE-CARD-PUNCHER', 'card.puncher@factory.com', 'Card Puncher', 'EMP-004'),
        ('ROLE-FILATURE-SUPPLIER', 'filature.supplier@factory.com', 'Filature Supplier', 'EMP-005'),
        ('ROLE-GRAPH-DRAFTER', 'graph.drafter@factory.com', 'Graph Drafter', 'EMP-006'),
        ('ROLE-LOG-FINISHING', 'log.finishing@factory.com', 'LOG Finishing Specialist', 'EMP-007'),
        ('ROLE-LOG-FINISHING-TRANSIT-SPECIALIST', 'log.finishing.transit@factory.com', 'LOG Finishing & Transit Specialist', 'EMP-007A'),
        ('ROLE-LOOM-HARNESS-SETTER', 'loom.harness@factory.com', 'Loom Harness Setter', 'EMP-008'),
        ('ROLE-MASTER-COLORIST', 'master.colorist@factory.com', 'Master Colorist', 'EMP-009'),
        ('ROLE-MASTER-WEAVER', 'master.weaver@factory.com', 'Master Weaver', 'EMP-010'),
        ('ROLE-PETNI-MASTER', 'petni.master@factory.com', 'Petni Master', 'EMP-011'),
        ('ROLE-PIRN-WINDERS', 'pirn.winders@factory.com', 'Pirn Winders', 'EMP-012'),
        ('ROLE-QA-DYEING-INSPECTOR', 'qa.dyeing@factory.com', 'QA Dyeing Inspector', 'EMP-013'),
        ('ROLE-QUALITY-INSPECTOR', 'quality.inspector@factory.com', 'Quality Inspector', 'EMP-014'),
        ('ROLE-SILK-DEGUMMING-MASTER', 'silk.degumming@factory.com', 'Silk Degumming Master', 'EMP-015'),
        ('ROLE-SILK-GRADER', 'silk.grader@factory.com', 'Silk Grader', 'EMP-016'),
        ('ROLE-SILK-MARK-OFFICER', 'silk.mark@factory.com', 'Silk Mark Officer', 'EMP-017'),
        ('ROLE-SKEIN-DYE-MASTER', 'skein.dye@factory.com', 'Skein Dye Master', 'EMP-018'),
        ('ROLE-STORE-INVENTORY-MANAGER', 'store.inventory@factory.com', 'Store Inventory Manager', 'EMP-019'),
        ('ROLE-SUP-LOOM-FLOOR-SUPERVISOR', 'sup.supervisor@factory.com', 'SUP Loom Floor Supervisor', 'EMP-020'),
        ('ROLE-THROWSTER-TWISTER', 'throwster.twister@factory.com', 'Throwster/Twister', 'EMP-021'),
        ('ROLE-WARP-BEAM-PREPARATION', 'warp.beam@factory.com', 'Warp Beam Preparation Specialist', 'EMP-022'),
        ('ROLE-WARP-JOINER', 'warp.joiner@factory.com', 'Warp Joiner', 'EMP-023'),
        ('ROLE-ZARI-INSPECTOR', 'zari.inspector@factory.com', 'Zari Inspector', 'EMP-024'),
        ('ROLE-DESIGN-GENERATOR', 'design.generator@factory.com', 'Design Generator', 'EMP-025'),
        ('ROLE-BUY-BACK-MANAGER', 'buyback.manager@factory.com', 'Buy-Back Manager', 'EMP-026'),
        ('ROLE-GUILD-MANAGER', 'guild.manager@factory.com', 'Guild Manager', 'EMP-027'),
        ('ROLE-IOT-DEVICE-MANAGER', 'iot.device@factory.com', 'IoT Device Manager', 'EMP-028'),
        ('ROLE-LOCALIZATION-MANAGER', 'localization.manager@factory.com', 'Localization Manager', 'EMP-029'),
    ]

    for role_id, email, name, emp_id in roles:
        cur.execute("""
            INSERT INTO users (email, password_hash, role_id, factory_node_id, full_name, employee_id)
            SELECT %s, %s, id, 'FACT-BLR-01', %s, %s
            FROM roles WHERE role_id = %s
            ON CONFLICT (email) DO NOTHING
        """, (email, admin_password_hash, name, emp_id, role_id))

    # Create sample production lots
    for i in range(1, 11):
        lot_number = f"LOT-2024-{i:04d}"
        asset_id = f"ASSET-{uuid.uuid4().hex[:12].upper()}"
        cur.execute("""
            INSERT INTO production_lots (lot_number, factory_node_id, asset_id, status, priority)
            VALUES (%s, 'FACT-BLR-01', %s, 'Queued', %s)
            ON CONFLICT (lot_number) DO NOTHING
        """, (lot_number, asset_id, i % 5))

    conn.commit()
    cur.close()
    conn.close()

    print("Database seeded successfully!")
    print("\nDefault users created:")
    print("  admin@factory.com / admin123 (System Admin)")
    print("  [role].user@factory.com / admin123 (24 role-specific users)")

if __name__ == '__main__':
    seed()
