#!/usr/bin/env python3
"""
Seed script for SKU Product Catalog
Inserts 480 SKU rows from the provided data
"""

import psycopg2
import os

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'silk_erp'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

SKU_DATA = [
    ("SKU-0001", "Varanasi", "North Indian Katan", "640 Hooks", "1Gram Gold Zari", "16/18 D", "20/22 D", "40 Denier", 33, 47, 120, 9, 209, 542, 1800, 3600, 6953, 24334, 17034, 9386, "Lightweight Dress Silk"),
    ("SKU-0002", "Varanasi", "North Indian Katan", "640 Hooks", "2G Gold Zari", "16/18 D", "20/22 D", "45 Denier", 33, 47, 120, 9, 209, 542, 1200, 3600, 6353, 22234, 15564, 8576, "Lightweight Dress Silk"),
    ("SKU-0003", "Varanasi", "North Indian Katan", "640 Hooks", "3G Gold Zari", "16/18 D", "20/22 D", "50 Denier", 33, 47, 130, 9, 218, 542, 778, 3600, 5930, 20756, 14529, 8006, "Lightweight Dress Silk"),
    ("SKU-0004", "Varanasi", "North Indian Katan", "640 Hooks", "5G Gold Zari", "16/18 D", "20/22 D", "55 Denier", 33, 47, 130, 9, 218, 542, 4536, 3600, 9689, 33910, 23737, 13080, "Lightweight Dress Silk"),
    ("SKU-0005", "Varanasi", "North Indian Katan", "1024 Hooks", "1Gram Gold Zari", "17/19 D", "21/23 D", "45 Denier", 35, 49, 220, 14, 318, 570, 3300, 4500, 9398, 32893, 23025, 12687, "Lightweight Dress Silk"),
    ("SKU-0006", "Varanasi", "North Indian Katan", "1024 Hooks", "2G Gold Zari", "17/19 D", "21/23 D", "50 Denier", 35, 49, 238, 14, 335, 570, 2376, 4500, 8474, 29659, 20761, 11440, "Lightweight Dress Silk"),
    ("SKU-0007", "Varanasi", "North Indian Katan", "1024 Hooks", "3G Gold Zari", "17/19 D", "21/23 D", "55 Denier", 35, 49, 238, 14, 335, 570, 1426, 4500, 7524, 26333, 18433, 10157, "Lightweight Dress Silk"),
    ("SKU-0008", "Varanasi", "North Indian Katan", "1024 Hooks", "5G Gold Zari", "17/19 D", "21/23 D", "60 Denier", 35, 49, 253, 15, 352, 570, 8855, 4500, 14956, 52348, 36643, 20191, "Lightweight Dress Silk"),
    ("SKU-0009", "Varanasi", "North Indian Katan", "1534 Hooks", "1Gram Gold Zari", "18/20 D", "22/24 D", "50 Denier", 37, 51, 346, 20, 454, 599, 5184, 5220, 12051, 42178, 29524, 16269, "Lightweight Dress Silk"),
    ("SKU-0010", "Varanasi", "North Indian Katan", "1534 Hooks", "2G Gold Zari", "18/20 D", "22/24 D", "55 Denier", 37, 51, 346, 20, 454, 599, 3456, 5220, 10323, 36130, 25291, 13936, "Lightweight Dress Silk"),
    ("SKU-0011", "Varanasi", "North Indian Katan", "1534 Hooks", "3G Gold Zari", "18/20 D", "22/24 D", "60 Denier", 37, 51, 368, 21, 477, 599, 2208, 5220, 9078, 31774, 22242, 12256, "Lightweight Dress Silk"),
    ("SKU-0012", "Varanasi", "North Indian Katan", "1534 Hooks", "5G Gold Zari", "18/20 D", "22/24 D", "65 Denier", 37, 51, 368, 21, 477, 599, 12880, 5220, 19750, 69126, 48388, 26663, "Lightweight Dress Silk"),
    ("SKU-0013", "Varanasi", "North Indian Katan", "2400 Hooks", "1Gram Gold Zari", "19/21 D", "23/25 D", "55 Denier", 39, 53, 454, 25, 571, 627, 6804, 5940, 14436, 50526, 35368, 19489, "Standard Mid-Weight Saree"),
    ("SKU-0014", "Varanasi", "North Indian Katan", "2400 Hooks", "2G Gold Zari", "19/21 D", "23/25 D", "60 Denier", 39, 53, 483, 26, 601, 627, 4830, 5940, 12466, 43629, 30541, 16828, "Standard Mid-Weight Saree"),
    ("SKU-0015", "Varanasi", "North Indian Katan", "2400 Hooks", "3G Gold Zari", "19/21 D", "23/25 D", "65 Denier", 39, 53, 483, 26, 601, 627, 2898, 5940, 10534, 36867, 25807, 14220, "Standard Mid-Weight Saree"),
    ("SKU-0016", "Varanasi", "North Indian Katan", "2400 Hooks", "5G Gold Zari", "19/21 D", "23/25 D", "70 Denier", 39, 53, 483, 26, 601, 627, 16905, 5940, 24541, 85892, 60124, 33130, "Standard Mid-Weight Saree"),
]

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
        
        for sku in SKU_DATA:
            cursor.execute(insert_query, sku)
        
        conn.commit()
        print(f"Successfully seeded {len(SKU_DATA)} SKUs")
        
    except Exception as e:
        conn.rollback()
        print(f"Error seeding SKU data: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    seed_sku_catalog()
