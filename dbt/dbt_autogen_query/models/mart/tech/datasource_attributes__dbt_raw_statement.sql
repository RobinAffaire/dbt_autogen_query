with fieldnames as (
    select * from {{ref('int_datasource_attributes__raw_fieldnames_determination')}}
),

create_stmt as (
    select * from {{ref('int_datasource_attributes__generate_staging_statement')}}
),

final as (
    select distinct 
    -- Key fields
    p0.sap_datasource_name as sap_datasource_name, 
    p0.sap_source_system   as sap_source_system,

    -- Computed field
    p0.create_raw_stmt_prefix || 
      listagg( p1.sap_field_name_lowercase || 
      ' ' || 
      p1.sf_data_type, ',' ) over (partition by p0.sap_datasource_name, p0.sap_source_system ) || ' ); ' 
                           as create_stmt
    -- Origin
    from fieldnames p1 
    -- Lookup: datasource_attributes__create_stmt
    left outer join create_stmt p0 
      on  p0.sap_datasource_name = p1.sap_datasource_name
      and p0.sap_source_system   = p1.sap_source_system
)

select * from final