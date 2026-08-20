#!/usr/bin/env python3
"""
Seed script for SKU Product Catalog
Reads from sku_catalog.csv and inserts all rows into PostgreSQL
"""

import psycopg2
import csv
import os

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'silk_erp'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(SCRIPT_DIR, 'sku_catalog.csv')

def seed_sku_catalog():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        cursor.execute("DELETE FROM sku_catalog")
        conn.commit()
        
        insert_query = """
        INSERT INTO sku_catalog (
            sku_ref_id, geographic_hub, weave_category, jacquard_capacity, zari_configuration,
            warp_denier, weft_denier, zari_wire_denier, warp_net_weight_g, weft_net_weight_g,
            zari_net_weight_g, bobbin_waste_weight_g, total_saree_weight_g, yarn_raw_cost_inr,
            zari_raw_cost_inr, labor_surcharge_inr, total_mfg_cost_inr, mrp_inr, selling_price_inr,
            min_floor_price_inr, weight_category_profile
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        with open(CSV_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                cursor.execute(insert_query, (
                    row['sku_ref_id'],
                    row['geographic_hub'],
                    row['weave_category'],
                    row['jacquard_capacity'],
                    row['zari_configuration'],
                    row['warp_denier'],
                    row['weft_denier'],
                    row['zari_wire_denier'],
                    float(row['warp_net_weight_g']),
                    float(row['weft_net_weight_g']),
                    float(row['zari_net_weight_g']),
                    float(row['bobbin_waste_weight_g']),
                    float(row['total_saree_weight_g']),
                    float(row['yarn_raw_cost_inr']),
                    float(row['zari_raw_cost_inr']),
                    float(row['labor_surcharge_inr']),
                    float(row['total_mfg_cost_inr']),
                    float(row['mrp_inr']),
                    float(row['selling_price_inr']),
                    float(row['min_floor_price_inr']),
                    row['weight_category_profile']
                ))
                count += 1
        
        conn.commit()
        print(f"Successfully seeded {count} SKUs from {CSV_PATH}")
        
    except Exception as e:
        conn.rollback()
        print(f"Error seeding SKU data: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    seed_sku_catalog()
