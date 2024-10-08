 MERGE `searce-dna-ps1-delivery.salesforce_curated.contact` AS target
USING `searce-dna-ps1-delivery.salesforce_raw.raw_contact` AS source
ON target.id = source.id
WHEN MATCHED AND source.SystemModstamp > target.SystemModstamp THEN
  UPDATE SET
target.Id	= source.Id,
target.AccountId	= source.AccountId,
target.AiraAccountCode__c	= source.AiraAccountCode__c,
target.Brand_Name__c	= source.Brand_Name__c,
target.Campaign__c	= source.Campaign__c,
target.Comments__c	= source.Comments__c,
target.CreatedById	= source.CreatedById,
target.CreatedDate	= source.CreatedDate,
target.Department	= source.Department,
target.DoNotCall	= source.DoNotCall,
target.Email	= source.Email,
target.EmailBouncedDate	= source.EmailBouncedDate,
target.EmailBouncedReason	= source.EmailBouncedReason,
target.ExtAccountCode__c	= source.ExtAccountCode__c,
target.Fax	= source.Fax,
target.FirstName	= source.FirstName,
target.FlexTrialCampaign__c	= source.FlexTrialCampaign__c,
target.IsAgent__c	= source.IsAgent__c,
target.IsDeleted	= source.IsDeleted,
target.IsEmailBounced	= source.IsEmailBounced,
target.IsGuest__c	= source.IsGuest__c,
target.IsPersonAccount	= source.IsPersonAccount,
target.LastActivityDate	= source.LastActivityDate,
target.LastModifiedById	= source.LastModifiedById,
target.LastModifiedDate	= source.LastModifiedDate,
target.LastName	= source.LastName,
target.List_Source__c	= source.List_Source__c,
target.MailingAddress	= source.MailingAddress,
target.MailingCity	= source.MailingCity,
target.MailingCountry	= source.MailingCountry,
target.MailingPostalCode	= source.MailingPostalCode,
target.MailingState	= source.MailingState,
target.MailingStreet	= source.MailingStreet,
target.MiddleName	= source.MiddleName,
target.MobilePhone	= source.MobilePhone,
target.Name	= source.Name,
target.New_Id__c	= source.New_Id__c,
target.OwnerId	= source.OwnerId,
target.Phone	= source.Phone,
target.PhotoUrl	= source.PhotoUrl,
target.Salutation	= source.Salutation,
target.Standardized_Title__c	= source.Standardized_Title__c,
target.SystemModstamp	= source.SystemModstamp,
target.TestUser__c	= source.TestUser__c,
target.Title	= source.Title,
target.TrailSecondsUsed__c	= source.TrailSecondsUsed__c,
target.TrialMinutesUsed__c	= source.TrialMinutesUsed__c,
target.TrialSecondsRemaining__c	= source.TrialSecondsRemaining__c,
target.et4ae5__HasOptedOutOfMobile__c	= source.et4ae5__HasOptedOutOfMobile__c,
target.et4ae5__Mobile_Country_Code__c	= source.et4ae5__Mobile_Country_Code__c,
target.Suffix	= source.Suffix,
target.CountryCode__c	= source.CountryCode__c,
target.PlatformStatus__c	= source.PlatformStatus__c,
target.TrialStartDate__c	= source.TrialStartDate__c,
target.GuestTrialDate__c	= source.GuestTrialDate__c,
target.Plan__c	= source.Plan__c,
target.Account_Name_Formula__c	= source.Account_Name_Formula__c
WHEN NOT MATCHED THEN
  INSERT ROW