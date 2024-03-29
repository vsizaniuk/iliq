import os
import psycopg
from psycopg.rows import dict_row

import oracledb

from enum import Enum
from abc import ABC, abstractmethod
from .sql_commands import PostgreSQLCommands, OracleSQLCommands


class RDBMSTypes(Enum):
    postgresql = (1, ';')
    oracle = (2, ';')
    mssql = (3, 'GO')

    @property
    def sql_sep(self):
        return self.value[1]


class DBAccess(ABC):

    @abstractmethod
    def __init__(self):
        self.rdbms_type = None
        self.user_name = None
        self.password = None
        self.db_name = None
        self.host = None
        self.port = None
        self.conn = None

    def __str__(self):
        return f'DB driver for database {self.db_name} (rdbms is {self.rdbms_type})'

    @property
    def connected(self):
        return bool(self.conn)

    @property
    def sql_sep(self):
        return RDBMSTypes[self.rdbms_type].sql_sep

    @abstractmethod
    def get_all_schemas(self):
        ...

    @abstractmethod
    def get_all_procedures(self):
        ...

    @abstractmethod
    def get_all_triggers(self):
        ...

    @abstractmethod
    def get_all_mat_views(self):
        ...

    @abstractmethod
    def get_all_composite_types(self):
        ...

    @abstractmethod
    def get_views_routines_triggers(self):
        ...

    @abstractmethod
    def delete_change_set(self):
        ...

    @abstractmethod
    def truncate_change_log(self):
        ...

    @abstractmethod
    def execute_any_sql(self, cmd: str):
        ...


class PostgreSQLAccess(DBAccess):

    SQL = PostgreSQLCommands

    def __init__(self,
                 user_name: str,
                 password: str,
                 db_name: str,
                 host: str,
                 port=5432):
        self.rdbms_type = RDBMSTypes.postgresql.name
        self.user_name = user_name
        self.password = password
        self.db_name = db_name
        self.host = host
        self.port = port
        self.conn: psycopg.Connection = None

    @property
    def conn_str(self):
        return 'host={} port={} dbname={} user={} password={}'.format(self.host,
                                                                      self.port,
                                                                      self.db_name,
                                                                      self.user_name,
                                                                      self.password)

    def connect(self):
        if not self.conn:
            self.conn = psycopg.connect(self.conn_str)

    def close_conn(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_all_schemas(self):
        if not self.connected:
            self.connect()

        with self.conn.cursor() as cur:
            cur.execute(self.SQL.schema_list_select.value, (None,))
            schemas = [r[0] for r in cur.fetchall()]

        return schemas

    def get_schema(self, schema_name):
        if not self.connected:
            self.connect()

        with self.conn.cursor() as cur:
            cur.execute(self.SQL.schema_list_select.value, (schema_name,))
            schema = cur.fetchone()[0]

        return schema

    def get_all_procedures(self):
        if not self.connected:
            self.connect()

        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(self.SQL.routines_text_select.value, (None,))
            for line in cur:
                yield line

    def get_all_triggers(self):
        if not self.connected:
            self.connect()

        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(self.SQL.triggers_text_select.value, (None,))
            for line in cur:
                yield line

    def get_all_mat_views(self):
        if not self.connected:
            self.connect()

        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(self.SQL.materialized_views_select.value, (None,))
            for line in cur:
                yield line

    def get_all_composite_types(self):
        if not self.connected:
            self.connect()

        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(self.SQL.object_types_select.value, (None,))
            for line in cur:
                yield line

    def get_views_routines_triggers(self):
        if not self.connected:
            self.connect()

        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(self.SQL.views_routines_triggers_select.value, (None,))
            for line in cur:
                yield line

    def delete_change_set(self,
                          change_set_id: str,
                          change_log_schema: str = 'public'):
        if not self.connected:
            self.connect()

        if not change_set_id:
            raise ValueError

        with self.conn.cursor() as cur:
            sql_cmd = self.SQL.databasechangelog_delete.value.format(schema_name=change_log_schema)
            cur.execute(sql_cmd,
                        (change_set_id,))

        self.conn.commit()

    def truncate_change_log(self, change_log_schema: str = 'public'):
        if not self.connected:
            self.connect()

        with self.conn.cursor() as cur:
            sql_cmd = self.SQL.databasechangelog_delete.value.format(schema_name=change_log_schema)
            cur.execute(sql_cmd,
                        (None,))

        self.conn.commit()

    def execute_any_sql(self, sql, *params, commit=True):
        if not self.connected:
            self.connect()

        with self.conn.cursor() as cur:
            cur.execute(sql, params)

        if commit:
            self.conn.commit()


class OracleSQLAccess(DBAccess):

    SQL = OracleSQLCommands

    def __init__(self,
                 user_name: str,
                 password: str,
                 db_name: str,  #  service_name
                 host: str,
                 port=1521):
        self.rdbms_type = RDBMSTypes.oracle.name
        self.user_name = user_name
        self.password = password
        self.db_name = db_name
        self.host = host
        self.port = port
        self.conn: oracledb.Connection = None

    @staticmethod
    def set_row_factory(cursor: oracledb.Cursor):
        columns = [col[0] for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(columns, args))
        return cursor

    @property
    def conn_str(self):
        return oracledb.ConnectParams(host=self.host, port=self.port, service_name=self.db_name)

    def connect(self):
        if not self.connected:
            self.conn = oracledb.connect(user=self.user_name,
                                         password=self.password,
                                         params=self.conn_str)

    def close_conn(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_all_schemas(self):
        if not self.connected:
            self.connect()

        with self.conn.cursor() as cur:
            cur.execute(self.SQL.schema_list_select.value, (None,))
            schemas = [r[0] for r in cur.fetchall()]

        return schemas

    def get_schema(self, schema_name):
        if not self.connected:
            self.connect()

        with self.conn.cursor() as cur:
            cur.execute(self.SQL.schema_list_select.value, (schema_name,))
            schema = cur.fetchone()[0]

        return schema

    def get_all_procedures(self):
        if not self.connected:
            self.connect()

        with self.conn.cursor() as cur:

            cur = self.set_row_factory(cur)
            cur.execute(self.SQL.routines_text_select.value, (None,))
            for line in cur:
                yield line

    def get_all_triggers(self):
        if not self.connected:
            self.connect()

        with self.conn.cursor() as cur:
            cur = self.set_row_factory(cur)
            cur.execute(self.SQL.triggers_text_select.value, (None,))
            for line in cur:
                yield line

    def get_all_mat_views(self):
        if not self.connected:
            self.connect()

        with self.conn.cursor() as cur:
            cur = self.set_row_factory(cur)
            cur.execute(self.SQL.materialized_views_select.value, (None,))
            for line in cur:
                yield line

    def get_all_composite_types(self):
        if not self.connected:
            self.connect()

        with self.conn.cursor() as cur:
            cur = self.set_row_factory(cur)
            cur.execute(self.SQL.object_types_select.value, (None,))
            for line in cur:
                yield line

    def get_views_routines_triggers(self):
        if not self.connected:
            self.connect()

        with self.conn.cursor() as cur:
            cur = self.set_row_factory(cur)
            cur.execute(self.SQL.views_routines_triggers_select.value, (None,))
            for line in cur:
                yield line

    def delete_change_set(self,
                          change_set_id: str,
                          change_log_schema: str):
        if not self.connected:
            self.connect()

        if not change_set_id:
            raise ValueError

        with self.conn.cursor() as cur:
            sql_cmd = self.SQL.databasechangelog_delete.value.format(schema_name=change_log_schema)
            cur.execute(sql_cmd, (change_set_id,))

        self.conn.commit()

    def truncate_change_log(self, change_log_schema: str):
        if not self.connected:
            self.connect()

        with self.conn.cursor() as cur:
            sql_cmd = self.SQL.databasechangelog_delete.value.format(schema_name=change_log_schema)
            cur.execute(sql_cmd,
                        (None,))

        self.conn.commit()

    def execute_any_sql(self, sql: str, *params, commit=True):
        if not self.connected:
            self.connect()

        with self.conn.cursor() as cur:
            cur.execute(sql, params)

        if commit:
            self.conn.commit()


def get_db_driver(rdbms_type: str) -> DBAccess:
    if rdbms_type == RDBMSTypes.postgresql.name:
        return PostgreSQLAccess(os.environ.get('ILIQ_P_USERNAME'),
                                os.environ.get('ILIQ_P_PASSWORD'),
                                os.environ.get('ILIQ_P_DB_NAME'),
                                os.environ.get('ILIQ_P_HOST'),
                                os.environ.get('ILIQ_P_PORT'))
    else:
        raise NotImplementedError(f'RDBMS {rdbms_type} is not supported!')
