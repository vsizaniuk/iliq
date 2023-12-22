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
