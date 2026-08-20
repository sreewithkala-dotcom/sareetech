from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
import os

app = FastAPI(title="SKU Catalog Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'silk_erp'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

class SKUModel(BaseModel):
    sku_ref_id: str
    geographic_hub: str
    weave_category: str
    jacquard_capacity: str
    zari_configuration: str
    warp_denier: str
    weft_denier: str
    zari_wire_denier: str
    warp_net_weight_g: float
    weft_net_weight_g: float
    zari_net_weight_g: float
    bobbin_waste_weight_g: float
    total_saree_weight_g: float
    yarn_raw_cost_inr: float
    zari_raw_cost_inr: float
    labor_surcharge_inr: float
    total_mfg_cost_inr: float
    mrp_inr: float
    selling_price_inr: float
    min_floor_price_inr: float
    weight_category_profile: str

class SKUResponse(SKUModel):
    id: int
    is_active: bool
    created_at: str
    updated_at: str

    class Config:
        from_attributes = True

def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

@app.get('/api/v1/sku/health')
def health():
    return {'status': 'ok', 'service': 'sku-catalog', 'version': '1.0.0'}

@app.get('/api/v1/sku', response_model=List[SKUResponse])
def list_skus(
    skip: int = 0,
    limit: int = 100,
    geographic_hub: Optional[str] = None,
    weave_category: Optional[str] = None,
    jacquard_capacity: Optional[str] = None,
    weight_category: Optional[str] = None,
    db=Depends(get_db)
):
    cursor = db.cursor()
    query = "SELECT * FROM sku_catalog WHERE is_active = TRUE"
    params = []
    
    if geographic_hub:
        query += " AND geographic_hub = %s"
        params.append(geographic_hub)
    if weave_category:
        query += " AND weave_category = %s"
        params.append(weave_category)
    if jacquard_capacity:
        query += " AND jacquard_capacity = %s"
        params.append(jacquard_capacity)
    if weight_category:
        query += " AND weight_category_profile = %s"
        params.append(weight_category)
    
    query += " ORDER BY id LIMIT %s OFFSET %s"
    params.extend([limit, skip])
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    
    columns = ['id', 'sku_ref_id', 'geographic_hub', 'weave_category', 'jacquard_capacity',
               'zari_configuration', 'warp_denier', 'weft_denier', 'zari_wire_denier',
               'warp_net_weight_g', 'weft_net_weight_g', 'zari_net_weight_g', 'bobbin_waste_weight_g',
               'total_saree_weight_g', 'yarn_raw_cost_inr', 'zari_raw_cost_inr', 'labor_surcharge_inr',
               'total_mfg_cost_inr', 'mrp_inr', 'selling_price_inr', 'min_floor_price_inr',
               'weight_category_profile', 'is_active', 'created_at', 'updated_at']
    
    return [dict(zip(columns, row)) for row in rows]

@app.get('/api/v1/sku/{sku_id}', response_model=SKUResponse)
def get_sku(sku_id: int, db=Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("SELECT * FROM sku_catalog WHERE id = %s AND is_active = TRUE", (sku_id,))
    row = cursor.fetchone()
    cursor.close()
    
    if not row:
        raise HTTPException(status_code=404, detail="SKU not found")
    
    columns = ['id', 'sku_ref_id', 'geographic_hub', 'weave_category', 'jacquard_capacity',
               'zari_configuration', 'warp_denier', 'weft_denier', 'zari_wire_denier',
               'warp_net_weight_g', 'weft_net_weight_g', 'zari_net_weight_g', 'bobbin_waste_weight_g',
               'total_saree_weight_g', 'yarn_raw_cost_inr', 'zari_raw_cost_inr', 'labor_surcharge_inr',
               'total_mfg_cost_inr', 'mrp_inr', 'selling_price_inr', 'min_floor_price_inr',
               'weight_category_profile', 'is_active', 'created_at', 'updated_at']
    
    return dict(zip(columns, row))

@app.get('/api/v1/sku/filters/options')
def get_filter_options(db=Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("SELECT DISTINCT geographic_hub FROM sku_catalog WHERE is_active = TRUE ORDER BY geographic_hub")
    hubs = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT weave_category FROM sku_catalog WHERE is_active = TRUE ORDER BY weave_category")
    categories = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT jacquard_capacity FROM sku_catalog WHERE is_active = TRUE ORDER BY jacquard_capacity")
    capacities = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT weight_category_profile FROM sku_catalog WHERE is_active = TRUE ORDER BY weight_category_profile")
    weight_categories = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    return {
        'geographic_hubs': hubs,
        'weave_categories': categories,
        'jacquard_capacities': capacities,
        'weight_categories': weight_categories
    }

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=5009)
