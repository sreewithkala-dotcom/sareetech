-- Add SKU linkage to finished sarees for buy-back valuation
-- This enables SKU-based cost calculation in the buy-back engine

-- Add sku_id to finished_sarees
ALTER TABLE finished_sarees
ADD COLUMN IF NOT EXISTS sku_id INTEGER REFERENCES sku_catalog(id);

-- Create index for faster buy-back lookups by SKU
CREATE INDEX IF NOT EXISTS idx_finished_sarees_sku_id ON finished_sarees(sku_id);

-- Backfill: Link existing finished sarees to SKU via design_files
-- This assumes design_files.design_json contains sku_ref_id from design generation
UPDATE finished_sarees fs
SET sku_id = (
    SELECT sc.id
    FROM production_lots pl
    JOIN design_files df ON df.pattern_id = (pl.initial_data->>'pattern_id')
    JOIN sku_catalog sc ON sc.sku_ref_id = (df.design_json->>'sku_ref_id')
    WHERE pl.id = fs.lot_id
    LIMIT 1
)
WHERE fs.sku_id IS NULL
  AND EXISTS (
      SELECT 1 FROM production_lots pl2
      WHERE pl2.id = fs.lot_id
        AND pl2.initial_data ? 'pattern_id'
  );
