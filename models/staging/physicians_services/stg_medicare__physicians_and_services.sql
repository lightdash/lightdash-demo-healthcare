
with all_physicians_services as (

  select
    cast(npi as int64) as physician_id,
    upper(nppes_provider_last_org_name) as physician_last_name,
    upper(nppes_provider_first_name) as physician_first_name,
    upper(nppes_credentials) as physician_credentials,
    upper(nppes_provider_gender) as physician_gender,
    upper(nppes_provider_city) as physician_city,
    upper(nppes_provider_state) as physician_state,
    upper(provider_type) as physician_type,
    if(upper(medicare_participation_indicator) = 'Y', true, false) as is_medicare_participant,
    hcpcs_code as service_code,
    hcpcs_description as service_description,
    hcpcs_drug_indicator as drug_indicator,
    line_srvc_cnt,
    bene_unique_cnt,
    bene_day_srvc_cnt,
    average_medicare_allowed_amt,
    average_submitted_chrg_amt,
    average_medicare_payment_amt

  from {{ source('medicare','physicians_and_other_supplier_2012') }}

  union all 

  select
    cast(npi as int64) as physician_id,
    upper(nppes_provider_last_org_name) as physician_last_name,
    upper(nppes_provider_first_name) as physician_first_name,
    upper(nppes_credentials) as physician_credentials,
    upper(nppes_provider_gender) as physician_gender,
    upper(nppes_provider_city) as physician_city,
    upper(nppes_provider_state) as physician_state,
    upper(provider_type) as physician_type,
    if(upper(medicare_participation_indicator) = 'Y', true, false) as is_medicare_participant,
    hcpcs_code as service_code,
    hcpcs_description as service_description,
    hcpcs_drug_indicator as drug_indicator,
    line_srvc_cnt,
    bene_unique_cnt,
    bene_day_srvc_cnt,
    average_medicare_allowed_amt,
    average_submitted_chrg_amt,
    average_medicare_payment_amt,
    

  from {{ source('medicare','physicians_and_other_supplier_2013') }}

  union all 

  select
    cast(npi as int64) as physician_id,
    upper(nppes_provider_last_org_name) as physician_last_name,
    upper(nppes_provider_first_name) as physician_first_name,
    upper(nppes_credentials) as physician_credentials,
    upper(nppes_provider_gender) as physician_gender,
    upper(nppes_provider_city) as physician_city,
    upper(nppes_provider_state) as physician_state,
    upper(provider_type) as physician_type,
    if(upper(medicare_participation_indicator) = 'Y', true, false) as is_medicare_participant,
    hcpcs_code as service_code,
    hcpcs_description as service_description,
    hcpcs_drug_indicator as drug_indicator,
    line_srvc_cnt,
    bene_unique_cnt,
    bene_day_srvc_cnt,
    average_medicare_allowed_amt,
    average_submitted_chrg_amt,
    average_medicare_payment_amt,
    

  from {{ source('medicare','physicians_and_other_supplier_2014') }}

  union all 

  select
    cast(npi as int64) as physician_id,
    upper(nppes_provider_last_org_name) as physician_last_name,
    upper(nppes_provider_first_name) as physician_first_name,
    upper(nppes_credentials) as physician_credentials,
    upper(nppes_provider_gender) as physician_gender,
    upper(nppes_provider_city) as physician_city,
    upper(nppes_provider_state) as physician_state,
    upper(provider_type) as physician_type,
    if(upper(medicare_participation_indicator) = 'Y', true, false) as is_medicare_participant,
    hcpcs_code as service_code,
    hcpcs_description as service_description,
    hcpcs_drug_indicator as drug_indicator,
    line_srvc_cnt,
    bene_unique_cnt,
    bene_day_srvc_cnt,
    average_medicare_allowed_amt,
    average_submitted_chrg_amt,
    average_medicare_payment_amt,
    

  from {{ source('medicare','physicians_and_other_supplier_2015') }}

), divisions as (

  select 
    *,
    {{ assign_division('physician_state', 'physician_division') }}

  from all_physicians_services

), regions as (

  select 
    *,
    {{ assign_region('physician_division', 'physician_region') }}

  from divisions

)

select
  service_code||' ('||regions.physician_id||')' as uid,  -- unique identifier, physician + service provided
  physician_id,
  physician_last_name,
  physician_first_name,
  physician_credentials,
  COALESCE(physician_first_name, '') || ' ' ||
    COALESCE(physician_last_name, '') || ' ' ||
    COALESCE(physician_credentials, '') AS physician_full_name,
  physician_gender,
  physician_city,
  physician_state,
  physician_division,
  physician_region,
  physician_type,
  is_medicare_participant,
  service_code,
  service_description,
  drug_indicator,
  COALESCE(line_srvc_cnt, 0) as line_srvc_cnt,
  COALESCE(bene_unique_cnt, 0) as bene_unique_cnt,
  COALESCE(bene_day_srvc_cnt, 0) as bene_day_srvc_cnt,
  COALESCE(average_medicare_allowed_amt, 0) as avg_medicare_allowed_amt,
  COALESCE(average_submitted_chrg_amt, 0) as avg_submitted_charge_amt,
  COALESCE(average_medicare_payment_amt, 0) as avg_medicare_payment_amt
  

from regions
-- unioning duplicates across years; keep only one record per uid
qualify row_number() over (partition by uid) = 1