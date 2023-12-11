import os
import re

from enum import Enum
from shutil import rmtree

from .db_connectors import DBAccess

_OBJECTS_PATH_NAMES = ('tables', 'views', 'procedures', 'functions', 'packages', 'scripts', 'triggers', 'sequences')

_DDL_TYPES_TO_PATHS_MAP = {
    'own_file': {
        'table': 0,
        'view': 1,
        'procedure': 2,
        'function': 3,
        'package': 4,
        'trigger': 6,
        'sequence': 7},
    'share_file': {
        'index': (0,),
        'constraint': (0,),
        'table_comment': (0,),
        'column_comment': (0, 1),
        'view_comment': (1,)}
}

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
                    cmd += line.strip()
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
                 tree_encoding='utf-8'):
        self.db_driver = db_driver
        self.parent_dir = parent_dir
        self.changelog_type = changelog_type
        self.o_types_paths = _OBJECTS_PATH_NAMES
        self.encoding = tree_encoding

    def create_dir_tree(self,
                        rollbacks=False,
                        recreate=False):

        try:
            os.mkdir(self.parent_dir)
            if self.changelog_type == ChangelogTypes.united:
                os.mkdir(self.parent_dir + '/!sar_liq')
        except FileExistsError:
            ...

        for s in self.db_driver.get_all_schemas():
            schema_path = self.parent_dir + f'/{s}'
            try:
                os.mkdir(schema_path)
                if self.changelog_type == ChangelogTypes.per_schema:
                    os.mkdir(schema_path + '_liq')
            except FileExistsError:
                if recreate:
                    rmtree(schema_path)
                    os.mkdir(schema_path)
                    if self.changelog_type == ChangelogTypes.per_schema:
                        rmtree(schema_path + '_liq')
                        os.mkdir(schema_path + '_liq')
                else:
                    raise FileExistsError(f'Directory tree for {schema_path} is already created!')

            for tp in self.o_types_paths:
                if tp == 'packages' and self.db_driver.rdbms_type != 'oracle':
                    continue
                tp_path = schema_path + f'/{tp}'
                os.mkdir(tp_path)
                if rollbacks:
                    tp_r_path = tp_path + '/rollbacks'
                    os.mkdir(tp_r_path)

    def put_object_into_tree(self,
                             o_type: str,
                             o_name: str,
                             ddl_cmd: str):
        own_file, share_file = _DDL_TYPES_TO_PATHS_MAP['own_file'], _DDL_TYPES_TO_PATHS_MAP['share_file']

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

    def put_ddl_file_into_tree(self,
                               file_name: str,
                               cmd_sep=';',
                               file_encoding='utf-8'):
        for cmd in self.parse_ddl_file(file_name, cmd_sep, file_encoding):
            try:
                o_name, o_type = self.classify_ddl(cmd)
            except Exception:
                print(cmd)
                raise

            o_name = o_name.replace('"', '')
            self.put_object_into_tree(o_type, o_name, cmd)

    def get_objects_in_creation_order(self,
                                      file_name: str,
                                      cmd_sep=';',
                                      file_encoding='utf-8'):
        own_file = _DDL_TYPES_TO_PATHS_MAP['own_file'].keys()
        for cmd in self.parse_ddl_file(file_name, cmd_sep, file_encoding):
            o_name, o_type = self.classify_ddl(cmd)
            if o_type in own_file:
                yield o_name, o_type

    def put_routines_into_tree(self):
        own_file = _DDL_TYPES_TO_PATHS_MAP['own_file']
        for routine_rec in self.db_driver.get_all_procedures():
            routine_path = os.path.join(self.parent_dir,
                                        routine_rec['schema_name'],
                                        self.o_types_paths[own_file[routine_rec['routine_type']]],
                                        f"{routine_rec['routine_name']}.sql")

            routine_f = open(routine_path, 'w', encoding=self.encoding)
            routine_f.write(routine_rec['routine_text'])
            routine_f.close()

    def put_triggers_into_tree(self):
        own_file = _DDL_TYPES_TO_PATHS_MAP['own_file']
        for trigger_rec in self.db_driver.get_all_triggers():
            trigger_path = os.path.join(self.parent_dir,
                                        trigger_rec['schema_name'],
                                        self.o_types_paths[own_file['trigger']],
                                        f"{trigger_rec['trigger_name']}.sql")
            trigger_f = open(trigger_path, 'w', encoding=self.encoding)
            trigger_f.write(trigger_rec['trigger_text'])
            trigger_f.close()
