 MERGE `searce-dna-ps1-delivery.salesforce_curated.product2` AS target
USING `searce-dna-ps1-delivery.salesforce_raw.raw_product2` AS source
ON target.id = source.id
WHEN MATCHED AND source.SystemModstamp > target.SystemModstamp THEN
  UPDATE SET
target.Id	= source.Id,
target.CreatedById	= source.CreatedById,
target.CreatedDate	= source.CreatedDate,
target.Description	= source.Description,
target.Family	= source.Family,
target.IsActive	= source.IsActive,
target.IsArchived	= source.IsArchived,
target.IsDeleted	= source.IsDeleted,
target.LastModifiedById	= source.LastModifiedById,
target.LastModifiedDate	= source.LastModifiedDate,
target.Name	= source.Name,
target.ProductCode	= source.ProductCode,
target.SystemModstamp	= source.SystemModstamp
WHEN NOT MATCHED THEN
  INSERT ROW