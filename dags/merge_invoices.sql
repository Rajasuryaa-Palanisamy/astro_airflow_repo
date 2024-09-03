MERGE `searce-dna-ps1-delivery.recurly_curated.invoices` AS target
USING `searce-dna-ps1-delivery.recurly_raw.raw_invoices` AS source
ON target.id = source.id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET
target.id	= source.id,	
target.updated_at	= source.updated_at,	
target.Line_items_has_more	= source.Line_items_has_more,	
target.line_items_object	= source.line_items_object,	
target.subtotal	= source.subtotal,	
target.terms_and_conditions	= source.terms_and_conditions,	
target.account	= source.account,	
target.customer_notes	= source.customer_notes,	
target.total	= source.total,	
target.paid	= source.paid,	
target.type	= source.type
WHEN NOT MATCHED THEN
  INSERT ROW
