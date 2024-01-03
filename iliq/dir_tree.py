import os
import re

from enum import Enum
from shutil import rmtree

from .db_connectors import DBAccess, RDBMSTypes

_OBJECTS_PATH_NAMES = ('tables', 'views', 'procedures', 'functions',
                       'packages', 'scripts', 'triggers', 'sequences',
                       'materialized_views', 'other_objects')

_DDL_COMMANDS_MAP = {
    'CREATE': {'TABLE': {'IF': {'NOT': {'EXISTS': 'table'}}, '!TABLE': 'table'},
               'OR': {'ALTER': {'VIEW': 'view',
                                'PROCEDURE': 'procedure',
                                'FUNCTION': 'function'},
                      'REPLACE': {'VIEW': 'view',
                                  'PROCEDURE': 'procedure',
                                  'FUNCTION': 'function',
                                  'PACKAGE': 'package'}},
               'SEQUENCE': {'IF': {'NOT': {'EXISTS': 'sequence'}}, '!SEQUENCE': 'sequence'},
               'VIEW': 'view',
               'PROCEDURE': 'procedure',
               'FUNCTION': 'function',
               'PACKAGE': 'package',
               'INDEX': 'index'},
    'ALTER': {'TABLE': {'ADD': {'CONSTRAINT': 'constraint'}}},
    'COMMENT': {'ON': {'TABLE': 'table_comment',
                       'COLUMN': 'column_comment',
                       'VIEW': 'view_comment'}}
}


class DDLTypesMap(Enum):
    table = (0, 0, True, 'version', False, False, True, 'all')
    view = (1, 1, True, 'view', False, True, True, 'all')
    procedure = (2, 2, True, 'proc', False, True, True, 'all')
    function = (3, 3, True, 'proc', False, True, True, 'all')
    package = (4, 4, True, 'proc', False, True, True, (RDBMSTypes.oracle.name,))
    script = (5, 5, True, 'version', False, False, True, 'all')
    trigger = (6, 6, True, 'proc', False, True, True, 'all')
    sequence = (7, 7, True, 'version', False, False, True, 'all')
    materialized_view = (8, 8, True, 'view', False, True, True, 'all')
    composite_type = (9, 9, True, 'version', False, False, True, 'all')

    index = (10, (0,), False, 'version')
    constraint = (11, (0,), False, 'version')
    table_comment = (12, (0,), False, 'version')
    column_comment = (13, (0, 1), False, 'version')
    view_comment = (14, (1,), False, 'version')

    @classmethod
    def get_ddl_path_names(cls, rdbms_type):
        for tp in cls:
            if tp.own_file:
                if tp.rdbms_types == 'all':
                    yield tp.path_name
                elif rdbms_type in tp.rdbms_types:
                    yield tp.path_name

    @property
    def ord_no(self):
        return self.value[0]

    @property
    def path_name_ref(self):
        return self.value[1]

    @property
    def own_file(self):
        return self.value[2]

    @property
    def path_name(self):
        if self.own_file:
            return _OBJECTS_PATH_NAMES[self.path_name_ref]
        else:
            return tuple(_OBJECTS_PATH_NAMES[r] for r in self.path_name_ref)

    @property
    def liq_context(self):
        return self.value[3]

    @property
    def run_always(self):
        try:
            return self.value[4]
        except IndexError:
            return -1

    @property
    def run_on_change(self):
        try:
            return self.value[5]
        except IndexError:
            return -1

    @property
    def fail_on_error(self):
        try:
            return self.value[6]
        except IndexError:
            return -1

    @property
    def rdbms_types(self):
        try:
            return self.value[7]
        except IndexError:
            return -1


class ChangelogTypes(Enum):
    per_schema = 'PER_SCHEMA'
    united = 'UNITED'

    def __eq__(self, other):
        if not isinstance(other, ChangelogTypes):
            return other == self.value
        else:
            return super().__eq__(other)


class DirTree:

    @staticmethod
    def classify_ddl(ddl_cmd) -> tuple[str, DDLTypesMap]:
        found = False
        current_context = o_name = None

        for i, part in enumerate(ddl_cmd.split()):
            if i == 0 and part not in _DDL_COMMANDS_MAP:
                raise ValueError('Wrong beginning for the DDL command!')
            elif i == 0:
                current_context = _DDL_COMMANDS_MAP[part]
                continue

            if found:
                o_name = part
                break

            if part not in current_context:
                o_name = part
                for tp in ('!TABLE', '!SEQUENCE'):
                    if tp in current_context:
                        current_context = DDLTypesMap[current_context[tp]]
                        return o_name, current_context
            else:
                current_context = current_context[part]
                if isinstance(current_context, str):
                    found = True
        current_context = DDLTypesMap[current_context]
        return o_name, current_context

    @staticmethod
    def check_quotations(cmd: str):
        cnt = 0
        for c in cmd:
            if c == '\'':
                if cnt == 0:
                    cnt += 1
                elif cnt == 1:
                    cnt -= 1

        return not cnt

    @staticmethod
    def parse_ddl_file(file_name: str,
                       cmd_sep=';',
                       encoding='utf-8'):
        with open(file_name, 'r', encoding=encoding) as sql_f:
            cmd = ''

            while True:
                line = sql_f.readline()
                if not line:
                    break

                if not line.startswith('--'):
                    cmd += '\n' + line.strip()
                    if '\'' in cmd and not DirTree.check_quotations(cmd):
                        continue

                if cmd.endswith(cmd_sep):
                    cmd = cmd.replace('"', '')
                    yield cmd
                    cmd = ''

    def __init__(self,
                 db_driver: DBAccess,
                 parent_dir: str = '.',
                 changelog_type: ChangelogTypes = ChangelogTypes.united,
                 rollbacks=False,
                 tree_encoding='utf-8'):
        self.db_driver = db_driver
        self.parent_dir = parent_dir
        self.changelog_type = changelog_type
        self.rollbacks = rollbacks
        self.o_types_paths = tuple(DDLTypesMap.get_ddl_path_names(db_driver.rdbms_type))
        self.encoding = tree_encoding

    def __str__(self):
        res = f'DirTree instance for {self.db_driver.db_name} database'
        return res

    @property
    def united_liq_path(self):
        return os.path.join(self.parent_dir, f'!{self.db_driver.db_name}_liq')

    def create_dir_tree(self,
                        recreate=False):

        try:
            os.mkdir(self.parent_dir)
            os.mkdir(self.united_liq_path)
        except FileExistsError:
            if recreate:
                rmtree(self.parent_dir)
                os.mkdir(self.parent_dir)
                os.mkdir(self.united_liq_path)
            else:
                raise

        for s in self.db_driver.get_all_schemas():
            schema_path = os.path.join(self.parent_dir, s)
            liq_schema_path = os.path.join(self.united_liq_path, s)
            os.mkdir(schema_path)
            os.mkdir(liq_schema_path)

            for tp in self.o_types_paths:
                tp_path = os.path.join(schema_path, tp)
                os.mkdir(tp_path)
                if self.rollbacks:
                    tp_r_path = os.path.join(tp_path, 'rollbacks')
                    os.mkdir(tp_r_path)

    def put_object_into_tree(self,
                             o_type: DDLTypesMap,
                             o_name: str,
                             ddl_cmd: str):
        if not o_type.own_file:
            if o_type.name in ('index', 'constraint'):
                r_pattern = r'(?<=ON)[\w\."\s]*(?=\()' if o_type.name == 'index' else r'(?<=TABLE).*(?=ADD)'
                o_name = re.search(r_pattern, ddl_cmd)
                o_name = o_name.group().strip().replace('"', '')

            o_path = o_name.split(sep='.')
            schema, o_name = o_path[0], o_path[1]

            for o_path_type in o_type.path_name:
                o_file_path = os.path.join(self.parent_dir,
                                           schema,
                                           o_path_type,
                                           f'{o_name}.sql')
                try:
                    o_file = open(o_file_path, 'r+', encoding=self.encoding)
                except FileNotFoundError:
                    continue

                o_file.seek(0, os.SEEK_END)
                o_file.write(f'\n{ddl_cmd}')
                o_file.close()

        elif o_type.own_file:
            o_path = o_name.split(sep='.')
            schema, o_name = o_path[0], o_path[1]
            o_file_path = os.path.join(self.parent_dir,
                                       schema,
                                       o_type.path_name,
                                       f'{o_name}.sql')
            o_file = open(o_file_path, 'w', encoding=self.encoding)
            o_file.write(ddl_cmd)
            o_file.close()

    def add_paths_to_object_rec(self, object_rec: dict):
        o_type = DDLTypesMap[object_rec['object_type']]

        object_rec['sql_file_path'] = os.path.join('.',
                                                   object_rec['schema_name'],
                                                   o_type.path_name,
                                                   f"{object_rec['object_name']}.sql")

        if self.rollbacks:
            object_rec['rollback_file_path'] = os.path.join('.',
                                                            object_rec['schema_name'],
                                                            o_type.path_name,
                                                            'rollbacks',
                                                            f"rollback4{object_rec['object_name']}.sql")

    def put_ddl_file_into_tree(self,
                               file_name: str,
                               cmd_sep=';',
                               file_encoding='utf-8'):
        for cmd in self.parse_ddl_file(file_name, cmd_sep, file_encoding):
            o_name, o_type = self.classify_ddl(cmd)
            o_name = o_name.replace('"', '')

            self.put_object_into_tree(o_type, o_name, cmd)

            if o_type.own_file:
                o_name = o_name.replace('"', '')
                o_path = o_name.split(sep='.')
                schema, o_name = o_path[0], o_path[1]

                res = {'schema_name': schema,
                       'object_name': o_name,
                       'object_type': o_type.name}

                self.add_paths_to_object_rec(res)

                yield res

    def put_views_routines_triggers_into_tree(self):
        for object_rec in self.db_driver.get_views_routines_triggers():
            o_type = DDLTypesMap[object_rec['object_type']]
            object_path = os.path.join(self.parent_dir,
                                       object_rec['schema_name'],
                                       o_type.path_name,
                                       f"{object_rec['object_name']}.sql")

            self.add_paths_to_object_rec(object_rec)

            object_f = open(object_path, 'w', encoding=self.encoding)
            object_f.write(object_rec.pop('object_text'))
            object_f.close()

            yield object_rec

    def put_routines_into_tree(self):
        for routine_rec in self.db_driver.get_all_procedures():
            o_type = DDLTypesMap[routine_rec['object_type']]
            routine_path = os.path.join(self.parent_dir,
                                        routine_rec['schema_name'],
                                        o_type.path_name,
                                        f"{routine_rec['object_name']}.sql")

            self.add_paths_to_object_rec(routine_rec)

            routine_f = open(routine_path, 'w', encoding=self.encoding)
            routine_f.write(routine_rec.pop('object_text'))
            routine_f.close()

            yield routine_rec

    def put_triggers_into_tree(self):
        for trigger_rec in self.db_driver.get_all_triggers():
            o_type = DDLTypesMap[trigger_rec['object_type']]
            trigger_path = os.path.join(self.parent_dir,
                                        trigger_rec['schema_name'],
                                        o_type.path_name,
                                        f"{trigger_rec['object_name']}.sql")

            self.add_paths_to_object_rec(trigger_rec)

            trigger_f = open(trigger_path, 'w', encoding=self.encoding)
            trigger_f.write(trigger_rec.pop('object_text'))
            trigger_f.close()

            yield trigger_rec

    def put_mat_views_into_tree(self):
        for m_view_rec in self.db_driver.get_all_mat_views():
            o_type = DDLTypesMap[m_view_rec['object_type']]
            m_view_path = os.path.join(self.parent_dir,
                                       m_view_rec['schema_name'],
                                       o_type.path_name,
                                       f"{m_view_rec['object_name']}.sql")

            self.add_paths_to_object_rec(m_view_rec)

            m_view_f = open(m_view_path, 'w', encoding=self.encoding)
            m_view_f.write(m_view_rec.pop('object_text'))
            m_view_f.close()

            yield m_view_rec

    def put_composite_types_into_tree(self):
        for c_type_rec in self.db_driver.get_all_composite_types():
            o_type = DDLTypesMap[c_type_rec['object_type']]
            c_type_path = os.path.join(self.parent_dir,
                                       c_type_rec['schema_name'],
                                       o_type.path_name,
                                       f"{c_type_rec['object_name']}.sql")

            self.add_paths_to_object_rec(c_type_rec)

            c_type_f = open(c_type_path, 'w', encoding=self.encoding)
            c_type_f.write(c_type_rec.pop('object_text'))
            c_type_f.close()

            yield c_type_rec


def get_project_path():
    path = os.environ.get('ILIQ_PROJECT_PATH')
    while not path:
        path = input('Enter Liquibase project path: ')

    return path
