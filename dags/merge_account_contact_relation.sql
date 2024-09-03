 MERGE `searce-dna-ps1-delivery.salesforce_curated.account_contact_relation` AS target
USING `searce-dna-ps1-delivery.salesforce_raw.raw_account_contact_relation` AS source
ON target.id = source.id
WHEN MATCHED AND source.SystemModstamp > target.SystemModstamp THEN
  UPDATE SET
target.Id	= source.Id,
target.AccountId	= source.AccountId,
target.ContactId	= source.ContactId,
target.CreatedById	= source.CreatedById,
target.CreatedDate	= source.CreatedDate,
target.EndDate	= source.EndDate,
target.IsActive	= source.IsActive,
target.IsDeleted	= source.IsDeleted,
target.IsDirect	= source.IsDirect,
target.LastModifiedById	= source.LastModifiedById,
target.LastModifiedDate	= source.LastModifiedDate,
target.Relationship_Strength__c	= source.Relationship_Strength__c,
target.Roles	= source.Roles,
target.StartDate	= source.StartDate,
target.SystemModstamp	= source.SystemModstamp
WHEN NOT MATCHED THEN
  INSERT ROW