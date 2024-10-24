with source as (
   select * from {{ ref('stg_flat_file__datasource_attributes') }}
),

data_type_transcoded as (
    select distinct 
      sap_data_type as sap_data_type,
      case sap_data_type when 'CHAR' then 'varchar'
                         when 'CUKY' then 'varchar'
                         when 'UNIT' then 'varchar'
                         when 'CURR' then 'varchar'
                         when 'DATS' then 'date'
                         when 'NUMC' then 'integer'
                         when 'QUAN' then 'integer'
                         when 'DEC'  then 'decimal'
                         when 'TIMS' then 'timestamp'
                         else 'ERR_UNKNOWN' end as sf_data_type
      from source
)

select * from data_type_transcoded