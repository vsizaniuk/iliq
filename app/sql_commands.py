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
           f.proname as routine_name, 
           case when f.prokind = 'f' then 'function'
                when f.prokind = 'p' then 'procedure' end as routine_type,
           pg_get_functiondef(f.oid) routine_text
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
    '''