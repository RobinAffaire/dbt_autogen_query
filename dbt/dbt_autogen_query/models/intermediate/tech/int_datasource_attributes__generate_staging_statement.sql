with source as (
  select * from {{ ref('stg_flat_file__datasource_attributes')}}
),

source_system_transcoded as (
    select * from {{ref('int_datasource_attributes__source_system_transcoded')}}
),

generate_staging_statement as (
    select distinct 
    -- Origin fields
    src.sap_datasource_name                as sap_datasource_name,
    src.sap_source_system                  as sap_source_system,

    -- Lookup fields from source_system_transcoded
    source_system_transcoded.database_name as work_database_name, -- database name = Source system

    -- Computed fields
    'raw    '                             as work_schema_name,   -- Schema: raw
    '"' || src.sap_datasource_name || '"' as work_table_name,    -- "datasource_name"
    'create table if not exists ' || 
    work_database_name || 
    '.' || 
    work_schema_name || 
    '.' || 
    work_table_name || 
    ' ( '                                 as create_raw_stmt_prefix, -- create statement
  -- Origin
  from source src
 
  -- Lookup: int_datasource_attributes__source_system_transcoded
  left outer join source_system_transcoded
    on src.sap_source_system = source_system_transcoded.sap_source_system
)

select * from generate_staging_statement