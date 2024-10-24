-- This procedure cannot be created using snowsql. 
-- Error: Detected recursive file loading, skipping command
-- Store proc. created directly in snowsight.
CREATE OR REPLACE PROCEDURE tech.mart.sp_datasource_attributes__create_raw_tables()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET DEFAULT (select * from tech.mart.datasource_attributes__dbt_raw_statement);
    table_cursor cursor for res;
    sql_stmt varchar;
BEGIN
    FOR line IN table_cursor DO
      EXECUTE IMMEDIATE line.create_stmt;
    END FOR;

    RETURN 'OK';
EXCEPTION
  WHEN statement_error THEN
    RETURN OBJECT_CONSTRUCT('Error type', 'STATEMENT_ERROR',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate); 
END;
$$
;