import os
import re

from enum import Enum
from shutil import rmtree

from .db_connectors import DBAccess

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
    table = (0, True, 'version', False, False, True)
    view = (1, True, 'view', False, True, True)
    procedure = (2, True, 'proc', False, True, True)
    function = (3, True, 'proc', False, True, True)
    package = (4, True, 'proc', False, True, True)
    script = (5, True, 'version', False, False, True)
    trigger = (6, True, 'proc', False, True, True)
    sequence = (7, True, 'version', False, False, True)
    materialized_view = (8, True, 'view', False, True, True)
    composite_type = (9, True, 'version', False, False, True)

    index = ((0,), False, 'version')
    constraint = ((0,), False, 'version')
    table_comment = ((0,), False, 'version')
    column_comment = ((0, 1), False, 'version')
    view_comment = ((1,), False, 'version')

    @classmethod
    def get_own_file_types(cls):
        for tp in cls:
            if tp.own_file:
                yield tp

    @classmethod
    def get_share_file_types(cls):
        for tp in cls:
            if not tp.own_file:
                yield tp

    @classmethod
    def get_ddl_to_paths_map(cls, own_file: bool):
        retrieve_func = cls.get_own_file_types if own_file else cls.get_share_file_types
        res = {tp.name: tp.path_name_ref for tp in retrieve_func()}
        return res

    @classmethod
    def get_ddl_to_liq_context_map(cls, own_file: bool):
        retrieve_func = cls.get_own_file_types if own_file else cls.get_share_file_types
        res = {tp.name: tp.liq_context for tp in retrieve_func()}
        return res

    @property
    def path_name_ref(self):
        return self.value[0]

    @property
    def own_file(self):
        return self.value[1]

    @property
    def liq_context(self):
        return self.value[2]

    @property
    def run_always(self):
        try:
            return self.value[3]
        except IndexError:
            return -1

    @property
    def run_on_change(self):
        try:
            return self.value[4]
        except IndexError:
            return -1

    @property
    def fail_on_error(self):
        try:
            return self.value[5]
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
    def classify_ddl(ddl_cmd) -> tuple[str, str]:
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
                        current_context = current_context[tp]
                        return o_name, current_context
            else:
                current_context = current_context[part]
                if isinstance(current_context, str):
                    found = True

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
        self.o_types_paths = _OBJECTS_PATH_NAMES
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
                if tp == 'packages' and self.db_driver.rdbms_type != 'oracle':
                    continue
                tp_path = schema_path + f'/{tp}'
                os.mkdir(tp_path)
                if self.rollbacks:
                    tp_r_path = tp_path + '/rollbacks'
                    os.mkdir(tp_r_path)

    def put_object_into_tree(self,
                             o_type: str,
                             o_name: str,
                             ddl_cmd: str):
        own_file, share_file = DDLTypesMap.get_ddl_to_paths_map(True), DDLTypesMap.get_ddl_to_paths_map(False)

        if o_type in share_file.keys():
            if o_type in ('index', 'constraint'):
                r_pattern = r'(?<=ON)[\w\."\s]*(?=\()' if o_type == 'index' else r'(?<=TABLE).*(?=ADD)'
                o_name = re.search(r_pattern, ddl_cmd)
                o_name = o_name.group().strip().replace('"', '')

            o_path = o_name.split(sep='.')
            schema, o_name = o_path[0], o_path[1]

            for o_path_type in share_file[o_type]:
                o_file_path = os.path.join(self.parent_dir,
                                           schema,
                                           self.o_types_paths[o_path_type],
                                           f'{o_name}.sql')
                try:
                    o_file = open(o_file_path, 'r+', encoding=self.encoding)
                except FileNotFoundError:
                    continue

                o_file.seek(0, os.SEEK_END)
                o_file.write(f'\n{ddl_cmd}')
                o_file.close()

        elif o_type in own_file.keys():
            o_path = o_name.split(sep='.')
            schema, o_name = o_path[0], o_path[1]
            o_file_path = os.path.join(self.parent_dir,
                                       schema,
                                       self.o_types_paths[own_file[o_type]],
                                       f'{o_name}.sql')
            o_file = open(o_file_path, 'w', encoding=self.encoding)
            o_file.write(ddl_cmd)
            o_file.close()

    def add_paths_to_object_rec(self, object_rec: dict):
        own_file = DDLTypesMap.get_ddl_to_paths_map(True)
        type_path_name = self.o_types_paths[own_file[object_rec['object_type']]]

        object_rec['sql_file_path'] = os.path.join('.',
                                                   object_rec['schema_name'],
                                                   type_path_name,
                                                   f"{object_rec['object_name']}.sql")

        if self.rollbacks:
            object_rec['rollback_file_path'] = os.path.join('.',
                                                            object_rec['schema_name'],
                                                            type_path_name,
                                                            'rollbacks',
                                                            f"rollback4{object_rec['object_name']}.sql")

    def put_ddl_file_into_tree(self,
                               file_name: str,
                               cmd_sep=';',
                               file_encoding='utf-8'):
        own_file = DDLTypesMap.get_ddl_to_paths_map(True)
        for cmd in self.parse_ddl_file(file_name, cmd_sep, file_encoding):
            o_name, o_type = self.classify_ddl(cmd)
            o_name = o_name.replace('"', '')

            self.put_object_into_tree(o_type, o_name, cmd)

            if o_type in own_file:
                o_name = o_name.replace('"', '')
                o_path = o_name.split(sep='.')
                schema, o_name = o_path[0], o_path[1]

                res = {'schema_name': schema,
                       'object_name': o_name,
                       'object_type': o_type}

                self.add_paths_to_object_rec(res)

                yield res

    def put_views_routines_triggers_into_tree(self):
        own_file = DDLTypesMap.get_ddl_to_paths_map(True)

        for object_rec in self.db_driver.get_views_routines_triggers():
            object_path = os.path.join(self.parent_dir,
                                       object_rec['schema_name'],
                                       self.o_types_paths[own_file[object_rec['object_type']]],
                                       f"{object_rec['object_name']}.sql")

            self.add_paths_to_object_rec(object_rec)

            object_f = open(object_path, 'w', encoding=self.encoding)
            object_f.write(object_rec.pop('object_text'))
            object_f.close()

            yield object_rec

    def put_routines_into_tree(self):
        own_file = DDLTypesMap.get_ddl_to_paths_map(True)
        for routine_rec in self.db_driver.get_all_procedures():
            routine_path = os.path.join(self.parent_dir,
                                        routine_rec['schema_name'],
                                        self.o_types_paths[own_file[routine_rec['object_type']]],
                                        f"{routine_rec['object_name']}.sql")

            self.add_paths_to_object_rec(routine_rec)

            routine_f = open(routine_path, 'w', encoding=self.encoding)
            routine_f.write(routine_rec.pop('object_text'))
            routine_f.close()

            yield routine_rec

    def put_triggers_into_tree(self):
        own_file = DDLTypesMap.get_ddl_to_paths_map(True)
        for trigger_rec in self.db_driver.get_all_triggers():
            trigger_path = os.path.join(self.parent_dir,
                                        trigger_rec['schema_name'],
                                        self.o_types_paths[own_file[trigger_rec['object_type']]],
                                        f"{trigger_rec['object_name']}.sql")

            self.add_paths_to_object_rec(trigger_rec)

            trigger_f = open(trigger_path, 'w', encoding=self.encoding)
            trigger_f.write(trigger_rec.pop('object_text'))
            trigger_f.close()

            yield trigger_rec

    def put_mat_views_into_tree(self):
        own_file = DDLTypesMap.get_ddl_to_paths_map(True)
        for m_view_rec in self.db_driver.get_all_mat_views():
            m_view_path = os.path.join(self.parent_dir,
                                       m_view_rec['schema_name'],
                                       self.o_types_paths[own_file[m_view_rec['object_type']]],
                                       f"{m_view_rec['object_name']}.sql")

            self.add_paths_to_object_rec(m_view_rec)

            m_view_f = open(m_view_path, 'w', encoding=self.encoding)
            m_view_f.write(m_view_rec.pop('object_text'))
            m_view_f.close()

            yield m_view_rec

    def put_composite_types_into_tree(self):
        own_file = DDLTypesMap.get_ddl_to_paths_map(True)
        for c_type_rec in self.db_driver.get_all_composite_types():
            c_type_path = os.path.join(self.parent_dir,
                                       c_type_rec['schema_name'],
                                       self.o_types_paths[own_file[c_type_rec['object_type']]],
                                       f"{c_type_rec['object_name']}.sql")

            self.add_paths_to_object_rec(c_type_rec)

            c_type_f = open(c_type_path, 'w', encoding=self.encoding)
            c_type_f.write(c_type_rec.pop('object_text'))
            c_type_f.close()

            yield c_type_rec
