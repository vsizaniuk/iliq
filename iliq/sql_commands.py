from enum import Enum


class PostgreSQLCommands(Enum):

    schema_list_select = '''
    SELECT s.nspname, o.rolname
     FROM pg_namespace s
     join pg_authid o
       on s.nspowner = o.oid
    where s.nspname not in ('information_schema', 'pg_catalog')
          and s.nspname not like 'pg_toast%%'
          and s.nspname not like 'pg_temp_%%'
          and s.nspname = coalesce( %s, s.nspname )
    '''

    views_routines_triggers_select = '''
    with objects_as_created as (select s.nspname                           as schema_name,
                                       t.oid,
                                       case
                                           when t.relkind = 'v' then
                                               'view'
                                           else 'materialized_view' end    as object_type,
                                       t.relname                           as object_name,
                                       format(case
                                                  when t.relkind = 'v' then
                                                      E'create or replace view %%s.%%s as \n %%s'
                                                  else E'create materialized view %%s.%%s as \n %%s' end,
                                              s.nspname,
                                              t.relname,
                                              pg_get_viewdef(t.oid, true)) as object_text
                                from pg_catalog.pg_class t
                                         join pg_catalog.pg_namespace s
                                              on t.relnamespace = s.oid
                                where t.relkind in ('v', 'm')
    
                                union all
    
                                select s.nspname,
                                       t.oid,
                                       case
                                           when t.prokind = 'f' then 'function'
                                           when t.prokind = 'p' then 'procedure' end,
                                       case when count(*) over (partition by t.proname) > 1 then
                                            t.proname ||'_'||row_number() over (partition by t.proname order by t.oid asc)
                                            else
                                            t.proname
                                       end as object_name,
                                       pg_get_functiondef(t.oid)
                                from pg_catalog.pg_proc t
                                         join pg_catalog.pg_namespace s
                                              on t.pronamespace = s.oid
                                where t.prokind not in ('a', 'w')
    
                                union all
    
                                select s.nspname,
                                       tr.oid,
                                       'trigger',
                                       tr.tgname,
                                       pg_get_triggerdef(tr.oid)
    
                                    from pg_catalog.pg_trigger tr
                                    join pg_catalog.pg_class c
                                    on tr.tgrelid = c.oid
                                    join pg_catalog.pg_namespace s
                                    on c.relnamespace = s.oid
                                    where not tr.tgisinternal
                                )
    
    select *
    from objects_as_created t
    where t.schema_name not in ('information_schema', 'pg_catalog')
      and t.schema_name not like 'pg_toast%%'
      and t.schema_name not like 'pg_temp_%%'
      and t.schema_name = coalesce( %s, t.schema_name )
    order by t.oid;
    '''

    routines_text_select = '''
    SELECT s.nspname as schema_name, 
           case when count(*) over (partition by f.proname) > 1 then
               f.proname ||'_'||row_number() over (partition by f.proname order by f.oid asc)
               else
               f.proname
           end as object_name, 
           case when f.prokind = 'f' then 'function'
                when f.prokind = 'p' then 'procedure' end as object_type,
           pg_get_functiondef(f.oid) object_text
      FROM pg_catalog.pg_proc f
      JOIN pg_catalog.pg_namespace s 
        ON f.pronamespace = s.oid
      join pg_catalog.pg_user u 
        on u.usesysid = s.nspowner
     
     WHERE f.prokind not in ('a', 'w')
           and s.nspname not in ('information_schema', 'pg_catalog')
           and s.nspname not like 'pg_toast%%'
           and s.nspname not like 'pg_temp_%%'
           and s.nspname = coalesce( %s, s.nspname )
    order by f.oid asc 
    '''

    triggers_text_select = '''
    select s.nspname as schema_name,
           tr.tgname as object_name,
           'trigger' as object_type,
           pg_get_triggerdef(tr.oid) object_text
       
      from pg_catalog.pg_trigger tr
      join pg_catalog.pg_class c
        on tr.tgrelid = c.oid
      join pg_catalog.pg_namespace s
        on c.relnamespace = s.oid
     where not tr.tgisinternal
           and s.nspname = coalesce( %s, s.nspname )
     order by tr.oid asc
    '''

    materialized_views_select = '''
    select t.schemaname as schema_name,
           t.matviewname as object_name,
           'materialized_view' as object_type,
           format(E'create materialized view %%s as \n %%s',
                  t.matviewname,
                  pg_get_viewdef(c.oid, true)) as object_text
    
      from pg_catalog.pg_matviews t
      join pg_catalog.pg_class c
        on t.matviewname = c.relname
     where t.schemaname = coalesce( %s, t.schemaname )
     order by c.oid asc
    '''

    object_types_select = '''
    WITH types AS (SELECT t.oid,
                          n.nspname,
                          pg_catalog.format_type(t.oid, NULL)                        AS obj_name,
                          coalesce(pg_catalog.obj_description(t.oid, 'pg_type'), '') AS description
                   FROM pg_catalog.pg_type t
                            JOIN pg_catalog.pg_namespace n
                                 ON n.oid = t.typnamespace
                            join pg_catalog.pg_class c
                                 on c.oid = t.typrelid
                            left join pg_catalog.pg_type el
                                      on el.oid = t.typelem
                                          AND el.typarray = t.oid
                   WHERE (t.typrelid = 0 or c.relkind = 'c')
                     AND el.oid is null
                     AND n.nspname not in ('pg_catalog', 'information_schema')
                     AND n.nspname !~ '^pg_toast'
                     AND n.nspname = coalesce(%s, n.nspname)
    ),
         cols AS (SELECT types.oid,
                         n.nspname                             AS schema_name,
                         format_type(t.oid, NULL)              AS obj_name,
                         a.attname                             AS column_name,
                         format_type(a.atttypid, a.atttypmod)  AS data_type,
                         a.attnotnull                          AS is_required,
                         a.attnum                              AS ordinal_position,
                         col_description(a.attrelid, a.attnum) AS description
                  FROM pg_catalog.pg_attribute a
                           JOIN pg_catalog.pg_type t
                                ON a.attrelid = t.typrelid
                           JOIN pg_catalog.pg_namespace n
                                ON n.oid = t.typnamespace
                           JOIN types
                                ON types.nspname = n.nspname
                                    AND types.obj_name = format_type(t.oid, NULL)
                  WHERE a.attnum > 0
                    AND NOT a.attisdropped)
    SELECT cols.schema_name,
           cols.obj_name                                                                              as object_name,
           'composite_type'                                                                           as object_type,
           format(E'create type %%s as \n(%%s);',
                  cols.obj_name,
                  string_agg(
                          format('%%s %%s',
                                 cols.column_name,
                                 cols.data_type)::text, E',\n'::text order by cols.ordinal_position)) as object_text
    FROM cols
    group by cols.schema_name,
             cols.obj_name,
             cols.oid
    order by cols.oid asc
    '''

    databasechangelog_delete = '''
    delete from {schema_name}.databasechangelog where id = coalesce(%s, id)
    '''


class OracleSQLCommands(Enum):
    schema_list_select = '''
    select t.username as schema_name
      from all_users t
     where t.oracle_maintained = 'N'
           and t.username = coalesce(:schema_name, t.username)
    '''

    views_routines_triggers_select = '''
    WITH object_list AS (
        SELECT
            t.owner AS schema_name,
            t.object_name,
            t.object_type,
            CASE
                WHEN t.object_type = 'TYPE'              THEN
                    1
                WHEN t.object_type = 'PACKAGE'           THEN
                    2
                WHEN t.object_type = 'VIEW'              THEN
                    2
                WHEN t.object_type = 'MATERIALIZED VIEW' THEN
                    2
                WHEN t.object_type = 'PROCEDURE'         THEN
                    3
                WHEN t.object_type = 'FUNCTION'          THEN
                    3
                WHEN t.object_type = 'TRIGGER'           THEN
                    3
            END     AS ot_order,
            t.object_id,
            t.timestamp
        FROM
            all_objects t
        WHERE
            t.object_type IN ( 'PACKAGE', 'TYPE', 'TRIGGER',
                               'PROCEDURE', 'FUNCTION', 'VIEW', 'MATERIALIZED VIEW' )
            AND t.owner IN (
                SELECT
                    t.username AS schema_name
                FROM
                    all_users t
                WHERE
                    t.oracle_maintained = 'N'
            )
    )
    SELECT
        t.schema_name,
        t.object_name,
        t.object_type, 
        DBMS_METADATA.GET_DDL(t.object_type, t.object_name, t.schema_name) as object_text
    FROM
        object_list t
    where t.schema_name = coalesce(:schema_name, t.schema_name)
    ORDER BY
        t.object_id,
        t.ot_order
    '''

    routines_text_select = '''
    WITH object_list AS (
        SELECT
            t.owner AS schema_name,
            t.object_name,
            t.object_type,
            CASE
                WHEN t.object_type = 'PACKAGE'           THEN
                    2
                WHEN t.object_type = 'PROCEDURE'         THEN
                    3
                WHEN t.object_type = 'FUNCTION'          THEN
                    3
            END     AS ot_order,
            t.object_id,
            t.timestamp
        FROM
            all_objects t
        WHERE
            t.object_type IN ( 'PACKAGE', 'PROCEDURE', 'FUNCTION' )
            AND t.owner IN (
                SELECT
                    t.username AS schema_name
                FROM
                    all_users t
                WHERE
                    t.oracle_maintained = 'N'
            )
    )
    SELECT
        t.schema_name,
        t.object_name,
        t.object_type, 
        DBMS_METADATA.GET_DDL(t.object_type, t.object_name, t.schema_name) as object_text
    FROM
        object_list t
    where t.schema_name = coalesce(:schema_name, t.schema_name)
    ORDER BY
        t.object_id,
        t.ot_order
    '''

    triggers_text_select = '''
    WITH object_list AS (
        SELECT
        t.owner AS schema_name,
        t.object_name,
        t.object_type,
        t.object_id,
        t.timestamp
    FROM
        all_objects t
    WHERE
        t.object_type IN ('TRIGGER')
        AND t.owner IN (
            SELECT
                t.username AS schema_name
            FROM
                all_users t
            WHERE
                t.oracle_maintained = 'N'
        )
    )
    SELECT
        t.schema_name,
        t.object_name,
        t.object_type, 
        DBMS_METADATA.GET_DDL(t.object_type, t.object_name, t.schema_name) as object_text
    FROM
        object_list t
    where t.schema_name = coalesce(:schema_name, t.schema_name)
    ORDER BY
        t.object_id
    '''

    materialized_views_select = '''
    WITH object_list AS (
        SELECT
            t.owner AS schema_name,
            t.object_name,
            t.object_type,
            t.object_id,
            t.timestamp
        FROM
            all_objects t
        WHERE
            t.object_type IN ( 'MATERIALIZED VIEW' )
            AND t.owner IN (
                SELECT
                    t.username AS schema_name
                FROM
                    all_users t
                WHERE
                    t.oracle_maintained = 'N'
            )
    )
    SELECT
        t.schema_name,
        t.object_name,
        t.object_type, 
        DBMS_METADATA.GET_DDL(t.object_type, t.object_name, t.schema_name) as object_text
    FROM
        object_list t
    where t.schema_name = coalesce(:schema_name, t.schema_name)
    ORDER BY
        t.ot_order
    '''

    object_types_select = '''
    WITH object_list AS (
        SELECT
            t.owner AS schema_name,
            t.object_name,
            t.object_type,
            t.object_id,
            t.timestamp
        FROM
            all_objects t
        WHERE
            t.object_type IN ( 'TYPE')
            AND t.owner IN (
                SELECT
                    t.username AS schema_name
                FROM
                    all_users t
                WHERE
                    t.oracle_maintained = 'N'
            )
    )
    SELECT
        t.schema_name,
        t.object_name,
        t.object_type, 
        DBMS_METADATA.GET_DDL(t.object_type, t.object_name, t.schema_name) as object_text
    FROM
        object_list t
    where t.schema_name = coalesce(:schema_name, t.schema_name)
    ORDER BY
        t.object_id
    '''

    databasechangelog_delete = '''
    delete from {schema_name}.databasechangelog where id = coalesce(:id, id)
    '''
