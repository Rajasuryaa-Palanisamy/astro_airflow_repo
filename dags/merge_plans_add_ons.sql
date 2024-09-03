MERGE `searce-dna-ps1-delivery.recurly_curated.add_ons` AS target
USING `searce-dna-ps1-delivery.recurly_raw.raw_add_ons` AS source
ON target.id = source.id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET
target.id = source.id, 
target.accounting_code = source.accounting_code, 
target.code = source.code, 
target.created_at = source.created_at, 
target.default_quantity = source.default_quantity, 
target.deleted_at = source.deleted_at, 
target.display_quantity = source.display_quantity, 
target.name = source.name, 
target.object = source.object, 
target.plan_id = source.plan_id, 
target.state = source.state, 
target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT ROW
