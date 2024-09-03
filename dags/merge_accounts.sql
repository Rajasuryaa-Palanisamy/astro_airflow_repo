MERGE `searce-dna-ps1-delivery.recurly_curated.accounts` AS target
USING `searce-dna-ps1-delivery.recurly_raw.raw_accounts` AS source
ON target.id = source.id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET
target.id = source.id,
target.code = source.code,
target.email = source.email,
target.first_name = source.first_name,
target.last_name = source.last_name,
target.updated_at = source.updated_at,
target.object = source.object,
target.state = source.state,
target.hosted_login_token = source.hosted_login_token,
target.created_at = source.created_at,
target.deleted_at = source.deleted_at,
target.username = source.username,
target.preferred_locale = source.preferred_locale,
target.cc_emails = source.cc_emails,
target.company = source.company,
target.vat_number = source.vat_number,
target.tax_exempt = source.tax_exempt,
target.bill_to = source.bill_to,
target.address = source.address,
target.billing_info = source.billing_info
WHEN NOT MATCHED THEN
  INSERT ROW
    """
