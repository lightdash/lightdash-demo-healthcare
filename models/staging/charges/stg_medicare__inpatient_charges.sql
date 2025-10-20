
--stg_medicare__inpatient_charges

with all_years as (

  select 
    date('2011-01-01') as service_year,
    cast(provider_id as int64) as provider_id,
    upper(provider_name) as provider_name,
    upper(provider_city) as provider_city,
    upper(provider_state) as provider_state,
    upper(hospital_referral_region_description) as hospital_referral_region_description,
    total_discharges,
    average_covered_charges,
    average_total_payments,
    average_medicare_payments

  from {{ source('medicare','inpatient_charges_2011') }}

  union all 

  select 
    date('2012-01-01') as service_year,
    cast(provider_id as int64) as provider_id,
    upper(provider_name) as provider_name,
    upper(provider_city) as provider_city,
    upper(provider_state) as provider_state,
    upper(hospital_referral_region_description) as hospital_referral_region_description,
    total_discharges,
    average_covered_charges,
    average_total_payments,
    average_medicare_payments

  from {{ source('medicare','inpatient_charges_2012') }}

  union all 

  select 
    date('2013-01-01') as service_year,
    cast(provider_id as int64) as provider_id,
    upper(provider_name) as provider_name,
    upper(provider_city) as provider_city,
    upper(provider_state) as provider_state,
    upper(hospital_referral_region_description) as hospital_referral_region_description,
    total_discharges,
    average_covered_charges,
    average_total_payments,
    average_medicare_payments

  from {{ source('medicare','inpatient_charges_2013') }}

  union all 

  select 
    date('2014-01-01') as service_year,
    cast(provider_id as int64) as provider_id,
    upper(provider_name) as provider_name,
    upper(provider_city) as provider_city,
    upper(provider_state) as provider_state,
    upper(hospital_referral_region_description) as hospital_referral_region_description,
    total_discharges,
    average_covered_charges,
    average_total_payments,
    average_medicare_payments

  from {{ source('medicare','inpatient_charges_2014') }}

  union all 

  select
    date('2015-01-01') as service_year,
    cast(provider_id as int64) as provider_id,
    upper(provider_name) as provider_name,
    upper(provider_city) as provider_city,
    upper(provider_state) as provider_state,
    upper(hospital_referral_region_description) as hospital_referral_region_description,
    total_discharges,
    average_covered_charges,
    average_total_payments,
    average_medicare_payments

  from {{ source('medicare','inpatient_charges_2015') }}

),

get_latest_name as (
  --we need to do this because some names change over time 
  --but we need them to be the same across all years for reporting
  select 
    provider_id, 
    max_by(provider_name, service_year) as latest_provider_name 
  from all_years 
  group by provider_id

), divisions as (

  select 
    *,
    {{ assign_division('provider_state', 'provider_division') }}

  from all_years

), regions as (

  select 
    *,
    {{ assign_region('provider_division', 'provider_region') }}

  from divisions

) select 

  service_year,
  regions.provider_id,
  get_latest_name.latest_provider_name as provider_name,
  provider_city,
  provider_state,
  provider_division,
  provider_region,
  hospital_referral_region_description,
  provider_name||' ('||regions.provider_id||')' as provider_name_and_id,
  total_discharges,
  average_covered_charges,
  average_total_payments,
  average_medicare_payments,
  average_covered_charges * total_discharges as total_charged

  from regions
  inner join get_latest_name
    on regions.provider_id = get_latest_name.provider_id