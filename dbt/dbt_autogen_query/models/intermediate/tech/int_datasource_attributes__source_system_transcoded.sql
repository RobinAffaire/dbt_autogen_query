with source as (
  select * from {{ ref('stg_flat_file__datasource_attributes')}}
),

source_system_transcoded as (
  select distinct 
    sap_source_system                                 as sap_source_system,
    case sap_source_system when 'P93_050' then 'sap_p93'
                           else 'ERR_UNKNOWN' end     as database_name,
  from source
)

select * from source_system_transcoded