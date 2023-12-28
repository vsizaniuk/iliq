import psycopg
from psycopg.rows import dict_row

from enum import Enum
from abc import ABC, abstractmethod
from .sql_commands import PostgreSQLCommands


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
    def execute_any_sql(self):
        ...


class PostgreSQLAccess(DBAccess):

    SQL = PostgreSQLCommands

    def __init__(self,
                 user_name: str,
                 password: str,
                 db_name: str,
                 host: str,
                 port=5432):
        self.rdbms_type = 'postgresql'
        self.user_name = user_name
        self.password = password
        self.db_name = db_name
        self.host = host
        self.port = port
        self.conn: psycopg.Connection = None

    def __str__(self):
        return f'DB driver for database {self.db_name} (rdbms is {self.rdbms_type})'

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
