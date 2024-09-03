MERGE `searce-dna-ps1-delivery.recurly_curated.plans` AS target
USING `searce-dna-ps1-delivery.recurly_raw.raw_plans` AS source
ON target.id = source.id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET
  target.id	= source.id,
target.updated_at	= source.updated_at,
target.interval_unit	= source.interval_unit,
target.description	= source.description,
target.state	= source.state,
target.accounting_code	= source.accounting_code,
target.hosted_pages	= source.hosted_pages,
target.tax_exempt	= source.tax_exempt,
target.code	= source.code,
target.auto_renew	= source.auto_renew,
target.trial_unit	= source.trial_unit,
target.interval_length	= source.interval_length,
target.setup_fee_accounting_code	= source.setup_fee_accounting_code,
target.created_at	= source.created_at,
target.trial_length	= source.trial_length,
target.name	= source.name,
target.object	= source.object,
target.total_billing_cycles	= source.total_billing_cycles,
target.deleted_at	= source.deleted_at
WHEN NOT MATCHED THEN
  INSERT ROW
