
select *,
  --case statement needs to be in this order to avoid incorrect grouping due to roman numerals
  case
    when lower(apc) like '%level 3%' or lower(apc) like '%level iii%' 
      then 'Level 3'
    when lower(apc) like '%level 4%' or lower(apc) like '%level iv%' 
      then 'Level 4'
    when lower(apc) like '%level 5%' or lower(apc) like '%level v%' 
      then 'Level 5'
    when lower(apc) like '%level 2%' or lower(apc) like '%level ii%' 
      then 'Level 2'
    when lower(apc) like '%level 1%' or lower(apc) like '%level i%' 
      then 'Level 1'
    else 'No Level' 
    end as parsed_apc_level

from {{ref("stg_medicare__outpatient_charges")}}
