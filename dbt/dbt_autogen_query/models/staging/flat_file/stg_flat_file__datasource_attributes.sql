with
  source as (
    select * exclude (empty1) from {{ source('flat_file', 'rsdssegfd')}}
  ),

  renamed as (
    select 
      --ids
      datasource as sap_datasource_name,
      logsys as sap_source_system,
      segid as sap_segment,
      posit as sap_field_position,
      
      --strings
      fieldnm as sap_field_name,
      datatype as sap_data_type,
      txtlg    as sap_field_desc,

      --numerics
      leng     as sap_field_length,
      decimals as sap_field_decimals,

    from source
  )

  select * from renamed