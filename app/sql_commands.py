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

    triggers_text_select = '''
    select s.nspname as schema_name,
           tr.tgname as trigger_name,
           pg_get_triggerdef(tr.oid) trigger_text
       
      from pg_catalog.pg_trigger tr
      join pg_catalog.pg_class c
        on tr.tgrelid = c.oid
      join pg_catalog.pg_namespace s
        on c.relnamespace = s.oid
     where not tr.tgisinternal
           and s.nspname = coalesce( %s, s.nspname )
    '''
