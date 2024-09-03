MERGE `searce-dna-ps1-delivery.recurly_curated.subscriptions` AS target
USING `searce-dna-ps1-delivery.recurly_raw.raw_subscriptions` AS source
ON target.id = source.id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET
target.id	= source.id,
target.updated_at	= source.updated_at,
target.subtotal	= source.subtotal,
target.current_period_started_at	= source.current_period_started_at,
target.plan	= source.plan,
target.net_terms	= source.net_terms,
target.state	= source.state,
target.terms_and_conditions	= source.terms_and_conditions,
target.unit_amount	= source.unit_amount,
target.current_period_ends_at	= source.current_period_ends_at,
target.account	= source.account,
target.auto_renew	= source.auto_renew,
target.currency	= source.currency,
target.customer_notes	= source.customer_notes,
target.add_ons_total	= source.add_ons_total,
target.uuid	= source.uuid,
target.remaining_pause_cycles	= source.remaining_pause_cycles,
target.paused_at	= source.paused_at,
target.quantity	= source.quantity,
target.created_at	= source.created_at,
target.renewal_billing_cycles	= source.renewal_billing_cycles,
target.current_term_started_at	= source.current_term_started_at,
target.current_term_ends_at	= source.current_term_ends_at,
target.activated_at	= source.activated_at,
target.po_number	= source.po_number,
target.collection_method	= source.collection_method,
target.remaining_billing_cycles	= source.remaining_billing_cycles,
target.object	= source.object,
target.total_billing_cycles	= source.total_billing_cycles,
target.pending_change	= source.pending_change,
target.expiration_reason	= source.expiration_reason,
target.trial_ends_at	= source.trial_ends_at,
target.expires_at	= source.expires_at,
target.canceled_at	= source.canceled_at,
target.trial_started_at	= source.trial_started_at,
target.bank_account_authorized_at	= source.bank_account_authorized_at
WHEN NOT MATCHED THEN
  INSERT ROW
