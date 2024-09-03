MERGE `searce-dna-ps1-delivery.recurly_curated.coupons` AS target
USING `searce-dna-ps1-delivery.recurly_raw.raw_coupons` AS source
ON target.id = source.id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET
target.id = source.id,
target.object = source.object,
target.code = source.code,
target.name = source.name,
target.max_redemptions = source.max_redemptions,
target.max_redemptions_per_account = source.max_redemptions_per_account,
target.unique_coupon_codes_count = source.unique_coupon_codes_count,
target.duration = source.duration,
target.temporal_amount = source.temporal_amount,
target.temporal_unit = source.temporal_unit,
target.applies_to_all_plans = source.applies_to_all_plans,
target.applies_to_non_plan_charges = source.applies_to_non_plan_charges,
target.plans = source.plans,
target.items = source.items,
target.redemption_resource = source.redemption_resource,
target.discount = source.discount,
target.coupon_type = source.coupon_type,
target.hosted_page_description = source.hosted_page_description,
target.invoice_description = source.invoice_description,
target.redeem_by = source.redeem_by,
target.created_at = source.created_at,
target.updated_at = source.updated_at,
target.expired_at = source.expired_at
WHEN NOT MATCHED THEN
  INSERT ROW
