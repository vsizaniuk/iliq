import os
import subprocess
import re

from difflib import get_close_matches
from enum import Enum
from .db_connectors import DBAccess
from .dir_tree import DirTree, DDLTypesMap, ChangelogTypes
from .change_set import ChangeSet, ChangeLog


class LiqCommands(Enum):
    CHANGELOG_GEN_FROM_DB = ('liquibase generate-changelog '
                             '--changelog-file={changelog_file} ' 
                             '--defaults-file={defaults_file} '
                             '--diff-types=tables,columns,indexes,foreignkeys,primarykeys,uniqueconstraints,sequences')  # '--log-level=DEBUG '
    UPDATE = ('liquibase '
              '--defaults-file={defaults_file} '
              '--changelog-file={changelog_file} '
              'update')
    UPDATE_SQL = ('liquibase '
                  '--changelog-file={changelog_file} '
                  '--defaults-file={defaults_file} '
                  'update-sql')
    CONTEXT_UPDATE = ('liquibase '
                      '--defaults-file={defaults_file} '
                      '--changelog-file={changelog_file} '
                      '--contexts "{context}" '
                      'update')
    CONTEXT_UPDATE_SQL = ('liquibase '
                          '--changelog-file={changelog_file} '
                          '--defaults-file={defaults_file} '
                          '--contexts "{context}" '
                          'update-sql')
    TAG_DATABASE = ('liquibase '
                    '--defaults-file={defaults_file} '
                    'tag {version}')
    ROLLBACK = ('liquibase '
                '--defaults-file={defaults_file} '
                '--changelog-file={changelog_file} '
                'rollback {version}')
    ROLLBACK_SQL = ('liquibase '
                    '--defaults-file={defaults_file} '
                    '--changelog-file={changelog_file} '
                    'rollback-sql {version}')
    ROLLBACK_CONTEXT = ('liquibase '
                        '--defaults-file={defaults_file} '
                        '--changelog-file={changelog_file} '
                        '--contexts "{context}" '
                        'rollback {version}')

    def format(self, *arg, **kwargs):
        return self.value.format(*arg, **kwargs)


class LiqInterpreter:

    def __init__(self,
                 db_driver: DBAccess,
                 dir_tree: DirTree,
                 defaults_file,
                 changelog_file):
        self.os_user = os.getlogin()
        self.db_driver = db_driver
        self.dir_tree = dir_tree
        self.defaults_file = defaults_file
        self.change_log = ChangeLog(dir_tree.parent_dir, changelog_file)

    def __str__(self):
        res = (f'[\n {self.__class__.__name__} instance'
               f'\n\tChange log file: {self.change_log.file_name}'
               f'\n\tChange log type: {self.dir_tree.changelog_type.name}'
               f'\n\tDB driver: {self.db_driver}'
               f'\n\tTree:\n\t\tDirTree is: {self.dir_tree}'
               f'\n\t\tProject directory is: {self.dir_tree.parent_dir}'
               f'\n]')

        return res

    @property
    def dump_file_name(self):
        return f'dump_4_{self.db_driver.db_name}.sql'

    def generate_change_log(self):
        dump_file_path = os.path.join(self.dir_tree.parent_dir, self.dump_file_name)
        cmd = LiqCommands.CHANGELOG_GEN_FROM_DB.format(changelog_file=dump_file_path,
                                                       defaults_file=self.defaults_file)
        subprocess.run(cmd, shell=True)

    def create_liq_tables(self):
        cmd = LiqCommands.TAG_DATABASE.format(defaults_file=self.defaults_file,
                                              version='init_tag')

        subprocess.run(cmd, shell=True)

        self.db_driver.truncate_change_log()

    def get_update_sql(self, contexts: list = None):
        if contexts:
            cmd = LiqCommands.CONTEXT_UPDATE_SQL.format(context=','.join(contexts),
                                                        changelog_file=self.change_log.file_name,
                                                        defaults_file=self.defaults_file)
        else:
            cmd = LiqCommands.UPDATE_SQL.format(changelog_file=self.change_log.file_name,
                                                defaults_file=self.defaults_file)
        res = subprocess.run(cmd,
                             shell=True,
                             cwd=self.dir_tree.parent_dir,
                             capture_output=True)

        res = res.stdout.decode()

        return res

    def get_update_sql_changelog_dml(self, contexts: list = None):
        upd_str = self.get_update_sql(contexts=contexts)
        log_insert_pattern = re.compile('insert.*databasechangelog .*;', flags=re.IGNORECASE)
        for cmd in upd_str.split(sep='\n'):
            for c in re.findall(log_insert_pattern, cmd):
                yield c

    def upload_sql_changelog(self, contexts: list = None):
        for cmd in self.get_update_sql_changelog_dml(contexts=contexts):
            self.db_driver.execute_any_sql(cmd)

    def update(self, contexts: list = None):
        if contexts:
            cmd = LiqCommands.CONTEXT_UPDATE.format(context=','.join(contexts),
                                                    changelog_file=self.change_log.file_name,
                                                    defaults_file=self.defaults_file)
        else:
            cmd = LiqCommands.UPDATE.format(changelog_file=self.change_log.file_name,
                                            defaults_file=self.defaults_file)
        subprocess.run(cmd, shell=True, cwd=self.dir_tree.parent_dir)

    def save_change_log(self):
        self.change_log.save_change_log(encoding=self.dir_tree.encoding)

    def put_change_set(self, object_rec: dict):
        o_type = DDLTypesMap[object_rec['object_type']]
        change_set = ChangeSet(schema_name=object_rec['schema_name'],
                               object_type=o_type.name,
                               change_set_id=object_rec['object_name'],
                               author=self.os_user,
                               context=o_type.liq_context,
                               dbms=self.db_driver.rdbms_type,
                               run_always=o_type.run_always,
                               run_on_change=o_type.run_on_change,
                               fail_on_error=o_type.fail_on_error,
                               comment=f'{object_rec["object_name"]} {o_type.name} creation scrip',
                               change_sql_paths=[object_rec['sql_file_path']])

        parent_path = os.path.join(self.dir_tree.united_liq_path, change_set.schema_name)
        change_set.save_change_set(parent_path, self.dir_tree.encoding)

        self.change_log.add_change_set(change_set)

    def print_change_set(self):
        print(self.change_log.last_added_change_set)

    def save_change_log(self):
        self.change_log.save_change_log(encoding=self.dir_tree.encoding)

    def print_change_log(self):
        print(self.change_log)

    def init_project(self):

        self.dir_tree.create_dir_tree(recreate=True)
        self.generate_change_log()

        dump_file_path = os.path.join(self.dir_tree.parent_dir, self.dump_file_name)
        for object_rec in self.dir_tree.put_ddl_file_into_tree(dump_file_path):
            self.put_change_set(object_rec)

        for func in (self.dir_tree.put_composite_types_into_tree,
                     self.dir_tree.put_views_routines_triggers_into_tree):
            for object_rec in func():
                self.put_change_set(object_rec)

        self.save_change_log()


class LiqInterface:

    def __init__(self,
                 interpreter: LiqInterpreter):
        self.interpreter = interpreter

        self.commands_map = {'init_project': (self.run_command,
                                              self.interpreter.init_project,
                                              0, 'Creates a new project from scratch'),
                             'create_liq_tabs': (self.run_command,
                                                 self.interpreter.create_liq_tables,
                                                 1, 'Creates liquibase changelog tables in database'),
                             'get_update_sql': (self.run_context_command,
                                                self.interpreter.get_update_sql,
                                                2, 'Shows (but not applying) changes to be deployed'),
                             'upload_sql_changelog': (self.run_context_command,
                                                      self.interpreter.upload_sql_changelog,
                                                      3, 'Upload changelogs into liquibase changelog tables '
                                                      '(without applying)'),
                             'update': (self.run_context_command,
                                        self.interpreter.update,
                                        4, 'Applies current changes to database'),
                             'put_change_set': (self.run_add_changeset,
                                                self.interpreter.put_change_set,
                                                5, 'Adds a new changeset to the changelog'),
                             'print_change_set': (self.run_command,
                                                  self.interpreter.print_change_set,
                                                  6, 'Prints last added changeset'),
                             'save_change_log': (self.run_command,
                                                 self.interpreter.save_change_log,
                                                 7, 'Saves changelog file'),
                             'print_change_log': (self.run_command,
                                                  self.interpreter.print_change_log,
                                                  8, 'Prints current change log'),
                             'exit': (self.run_command,
                                      self.exit,
                                      9, 'Stop and exit'),
                             'print': (self.run_command,
                                       self.print_self,
                                       10, 'Prints Iliq instance properties'),
                             'help': (self.run_command,
                                      self.print_help,
                                      11, 'Prints this message')}

    @property
    def dir_tree(self):
        return self.interpreter.dir_tree

    @property
    def db_driver(self):
        return self.interpreter.db_driver

    @staticmethod
    def format_cmd(cmd: str):
        res = re.sub(r'^\s+|\s+$', '', cmd)
        res = res.lower()
        return res

    def y_n_bool(self, answer: str):
        answer = self.format_cmd(answer)
        return answer == 'y'

    def welcome(self):
        print(f'Interactive liquibase interface for {self.db_driver.db_name} database')
        print(f'Parent folder is {self.dir_tree.parent_dir}')
        print(f'Changelog file is {self.interpreter.change_log.file_name}')

    def print_help(self):
        as_list = [(k, v) for k, v in self.commands_map.items()]
        for k, v in sorted(as_list, key=lambda x: x[1][2]):
            print(f'Command {k} hint: {v[3]}')

    def cmd_lookup(self, cmd):
        res = ''
        if cmd in self.commands_map:
            return cmd
        matches = get_close_matches(cmd, self.commands_map, 3, cutoff=0.4)
        if matches:
            print('Exact command is not found; closest options: \n', '\n'.join(matches))
        else:
            print('Exact command is not found')

        return ''

    def run(self):
        self.welcome()
        while True:
            cmd = input('>>> ')
            cmd = self.format_cmd(cmd)
            cmd = self.cmd_lookup(cmd)
            if cmd:
                self.commands_map[cmd][0](cmd)
            else:
                continue

    def ask_for_contexts(self):
        contexts = []
        while True:
            cmd = input('>>> do we need to filter some contexts? (y/n) ')
            answer = self.y_n_bool(cmd)
            if not answer:
                break

            context = input('>>> enter a context: ')
            context = self.format_cmd(context)
            contexts.append(context)

        contexts = list(set(contexts))

        return contexts if contexts else None

    def ask_for_changeset(self):
        res = {
            'object_type': self.format_cmd(input('>>> enter an object type: ')),
            'schema_name': self.format_cmd(input('>>> enter a schema name: ')),
            'object_name': self.format_cmd(input('>>> enter an object name: '))
        }
        self.dir_tree.add_paths_to_object_rec(res)
        return res

    def run_command(self, cmd):
        self.commands_map[cmd][1]()

    def run_context_command(self, cmd):
        contexts = self.ask_for_contexts()
        self.commands_map[cmd][1](contexts)

    def add_changeset(self, cmd):
        object_rec = self.ask_for_changeset()
        self.commands_map[cmd][1](object_rec)

    def exit(self):
        answer = True
        if not self.interpreter.change_log.saved:
            answer = input('>>> Changelog got some changes and wasn''t saved. Continue? (y/n) ')
            answer = self.y_n_bool(answer)

        if answer:
            print(f'Closing liquibase session for {self.db_driver.db_name}')
            exit()
