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
        ('ROLE-PRODUCTION-PLANNING', 'production.planning@factory.com', 'Production Planning & Control Manager', 'EMP-030'),
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

    # Seed production lines
    line_data = [
        ('LINE-JAC-01', 'FACT-BLR-01', 'Jacquard Line Alpha', 'JACQUARD', 12, 24, 1, 'ACTIVE', 85.5, 82.0, 120.0, 90.0, 'ZONE-A'),
        ('LINE-JAC-02', 'FACT-BLR-01', 'Jacquard Line Beta', 'JACQUARD', 8, 16, 2, 'ACTIVE', 78.2, 75.5, 80.0, 75.0, 'ZONE-A'),
        ('LINE-HAND-01', 'FACT-BLR-01', 'Handloom Line Gamma', 'HANDLOOM', 20, 40, 3, 'ACTIVE', 65.0, 60.0, 50.0, 60.0, 'ZONE-B'),
        ('LINE-PWR-01', 'FACT-BLR-01', 'Powerloom Line Delta', 'POWERLOOM', 16, 20, 4, 'ACTIVE', 88.0, 85.0, 200.0, 85.0, 'ZONE-C'),
        ('LINE-FIN-01', 'FACT-BLR-01', 'Finishing Line Epsilon', 'AUTO', 4, 8, 5, 'ACTIVE', 92.0, 90.0, 300.0, 80.0, 'ZONE-D'),
    ]
    for line in line_data:
        cur.execute("""
            INSERT INTO production_lines (line_id, factory_node_id, line_name, line_type,
                total_looms, total_workers, supervisor_id, status, efficiency_pct,
                oee_score, capacity_meters_per_day, current_utilization_pct, location_zone)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (line_id) DO NOTHING
        """, line)

    # Seed shifts for next 3 days
    for day_offset in range(3):
        shift_date = (datetime.now() + timedelta(days=day_offset)).strftime('%Y-%m-%d')
        for i, line in enumerate(line_data):
            cur.execute("""
                INSERT INTO shifts (shift_id, factory_node_id, line_id, shift_name, shift_type,
                    start_time, end_time, total_workers_scheduled, total_looms_scheduled,
                    supervisor_id, status, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (shift_id) DO NOTHING
            """, (
                f"SHIFT-{shift_date}-LINE-{i+1:02d}",
                'FACT-BLR-01',
                line[0],
                f"Shift {i+1} - {line[3]}",
                'DAY',
                '08:00:00',
                '16:00:00',
                line[4] // 2,
                line[5] // 2,
                line[6],
                'SCHEDULED',
                shift_date
            ))

    conn.commit()
    cur.close()
    conn.close()

    print("Database seeded successfully!")
    print("\nDefault users created:")
    print("  admin@factory.com / admin123 (System Admin)")
    print("  [role].user@factory.com / admin123 (24 role-specific users)")

if __name__ == '__main__':
    seed()
