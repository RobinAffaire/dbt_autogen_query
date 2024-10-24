with source as (
    select * from {{ref('stg_flat_file__datasource_attributes')}}
),

data_type_transcoded as (
    select * from {{ref('int_datasource_attributes__data_type_transcoded')}}
),

functional_fields_determination as (
    select 
    -- Origin fields
    l0.sap_datasource_name as sap_datasource_name,
    l0.sap_source_system   as sap_source_system,
    l0.sap_segment         as sap_segment,
    l0.sap_field_position  as sap_field_position,
    l0.sap_field_name      as sap_field_name,
    
    -- Lookup: data_type_transco
    data_type_transcoded.sf_data_type as sf_data_type,

    -- Computed fields
    lower(l0.sap_field_name) as sap_field_name_lowercase,
  -- Origin
  from source l0
  -- Lookup: datasource_attributes__data_type_transcoded
  left outer join data_type_transcoded
    on l0.sap_data_type = data_type_transcoded.sap_data_type
),

technical_fields_determination as (
    select 
    -- Origin
    l0.sap_datasource_name as sap_datasource_name,
    l0.sap_source_system   as sap_source_system,
    l0.sap_segment         as sap_segment,

    -- Computed fields
    max( l0.sap_field_position ) + 1 as sap_field_position,
    'raw_updated_at'                 as sap_field_name,
    'timestamp_ltz'                  as sf_data_type,
    'raw_updated_at'                 as sap_field_name_lowercase,
  -- Origin
  from source l0
  group by l0.sap_datasource_name, l0.sap_source_system, l0.sap_segment
),

union_of_functional_fields_and_technical_fields as (
    select * from functional_fields_determination
    union
    select * from technical_fields_determination
)

select * from union_of_functional_fields_and_technical_fields