
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

),

divisions as (

  select *,
    case
      when provider_state in ('AK', 'CA', 'HI', 'OR', 'WA')
        then 'Pacific'
      when provider_state in ('AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY')
        then 'Mountain'
      when provider_state in ('AR', 'LA', 'OK', 'TX')
        then 'West South Central'
      when provider_state in ('AL', 'KY', 'MS', 'TN')
        then 'East South Central'
      when provider_state in ('DE', 'FL', 'GA', 'MD', 'NC','SC', 'VA', 'WA', 'WV', 'DC')
        then 'South Atlantic'
      when provider_state in ('IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD')
        then 'West North Central'
      when provider_state in ('IL', 'IN', 'MI', 'OH', 'WI')
        then 'East North Central'
      when provider_state in ('NJ', 'NY', 'PA')
        then 'Middle Atlantic'
      when provider_state in ('CT', 'ME', 'MA', 'NH', 'RI', 'VT')
        then 'New England'
      end as provider_division

  from all_years

),

regions as (

select *,
  case
    when provider_division in ('Pacific', 'Mountain')
      then 'West'
    when provider_division in ('West South Central', 'East South Central', 'South Atlantic')
      then 'South'
    when provider_division in ('West North Central', 'East North Central')
      then 'Midwest'
    when provider_division in ('Middle Atlantic', 'New England')
      then 'Northeast'
    end as provider_region

from divisions

)

select 
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
  average_covered_charges * total_discharges as total_charged,

from regions

  inner join get_latest_name
    on regions.provider_id = get_latest_name.provider_id