with source as (

  select * from {{ source('medicare', 'hospital_general_info') }}

), clean as (

    select
      cast(provider_id as int64) as provider_id,
      upper(hospital_name) as provider_name,
      upper(address) as provider_address,
      upper(city) as provider_city,
      upper(state) as provider_state,
      upper(zip_code) as provider_zip_code,
      upper(county_name) as provider_county_name,
      upper(phone_number) as provider_phone_number,
      upper(hospital_type) as hospital_type,
      upper(hospital_ownership) as hospital_ownership,
      emergency_services as provides_emergency_services,
      COALESCE(meets_criteria_for_promoting_interoperability_of_ehrs, false) as meets_criteria_for_meaningful_use_of_ehrs,
      IF(hospital_overall_rating = "Not Available", NULL, CAST(hospital_overall_rating AS FLOAT64)) as hospital_overall_rating,
      IF(mortality_group_measure_count = "Not Available", NULL, CAST(mortality_group_measure_count AS FLOAT64)) as mortality_group_measure_count,
      IF(facility_mortaility_measures_count = "Not Available", NULL, CAST(facility_mortaility_measures_count AS FLOAT64)) as facility_mortality_measures_count,
      IF(mortality_measures_better_count = "Not Available", NULL, CAST(mortality_measures_better_count AS FLOAT64)) as mortality_measures_better_count,
      IF(mortality_measures_no_different_count = "Not Available", NULL, CAST(mortality_measures_no_different_count AS FLOAT64)) as mortality_measures_no_different_count,
      IF(mortality_measures_worse_count = "Not Available", NULL, CAST(mortality_measures_worse_count AS FLOAT64)) as mortality_measures_worse_count,
      IF(mortaility_group_footnote = "Not Available", NULL, mortaility_group_footnote) as mortaility_group_footnote,
      IF(safety_measures_count = "Not Available", NULL, CAST(safety_measures_count AS FLOAT64)) as safety_measures_count,
      IF(facility_care_safety_measures_count = "Not Available", NULL, CAST(facility_care_safety_measures_count AS FLOAT64)) as facility_care_safety_measures_count,
      IF(safety_measures_better_count = "Not Available", NULL, CAST(safety_measures_better_count AS FLOAT64)) as safety_measures_better_count,
      IF(safety_measures_no_different_count = "Not Available", NULL, CAST(safety_measures_no_different_count AS FLOAT64)) as safety_measures_no_different_count,
      IF(safety_measures_worse_count = "Not Available", NULL, CAST(safety_measures_worse_count AS FLOAT64)) as safety_measures_worse_count,
      IF(safety_group_footnote = "Not Available", NULL, safety_group_footnote) as safety_group_footnote,
      IF(readmission_measures_count = "Not Available", NULL, CAST(readmission_measures_count AS FLOAT64)) as readmission_measures_count,
      IF(facility_readmission_measures_count = "Not Available", NULL, CAST(facility_readmission_measures_count AS FLOAT64)) as facility_readmission_measures_count,
      IF(readmission_measures_better_count = "Not Available", NULL, CAST(readmission_measures_better_count AS FLOAT64)) as readmission_measures_better_count,
      IF(readmission_measures_no_different_count = "Not Available", NULL, CAST(readmission_measures_no_different_count AS FLOAT64)) as readmission_measures_no_different_count,
      IF(readmission_measures_worse_count = "Not Available", NULL, CAST(readmission_measures_worse_count AS FLOAT64)) as readmission_measures_worse_count,
      IF(readmission_measures_footnote = "Not Available", NULL, readmission_measures_footnote) as readmission_measures_footnote,
      IF(patient_experience_measures_count = "Not Available", NULL, CAST(patient_experience_measures_count AS FLOAT64)) as patient_experience_measures_count,
      IF(facility_patient_experience_measures_count = "Not Available", NULL, CAST(facility_patient_experience_measures_count AS FLOAT64)) as facility_patient_experience_measures_count,
      IF(patient_experience_measures_footnote = "Not Available", NULL, patient_experience_measures_footnote) as patient_experience_measures_footnote,
      IF(timely_and_effective_care_measures_count = "Not Available", NULL, CAST(timely_and_effective_care_measures_count AS FLOAT64)) as timely_and_effective_care_measures_count,
      IF(facility_timely_and_effective_care_measures_count = "Not Available", NULL, CAST(facility_timely_and_effective_care_measures_count AS FLOAT64)) as facility_timely_and_effective_care_measures_count,
      IF(timely_and_effective_care_measures_footnote = "Not Available", NULL, timely_and_effective_care_measures_footnote) as timely_and_effective_care_measures_footnote
    from source


), divisions as (

  select 
    *,
    {{ assign_division('provider_state', 'provider_division') }}

  from clean

), regions as (

  select 
    *,
    {{ assign_region('provider_division', 'provider_region') }}

  from divisions

) select * from regions
