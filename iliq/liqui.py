import json
import os
import subprocess
import re

from dotenv import load_dotenv
from pathlib import Path
from difflib import get_close_matches
from enum import Enum
from .db_connectors import DBAccess, RDBMSTypes, get_db_driver
from .dir_tree import DirTree, DDLTypesMap, ChangelogTypes, get_project_path
from .change_set import ChangeSet, VersionTag, ChangeLog


_CACHE_DIR_NAME = '__iliq_cache__'
_CACHE_FILE_NAME = '__instance_cache__.json'


class LiqCommands(Enum):
    CHANGELOG_GEN_FROM_DB = ('liquibase generate-changelog '
                             '--changelog-file={changelog_file} ' 
                             '--defaults-file={defaults_file} '
                             '--diff-types=tables,columns,indexes,foreignkeys,'
                             'primarykeys,uniqueconstraints,sequences')  # '--log-level=DEBUG '
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
    ROLLBACK = ('liquibase rollback '
                '--tag={version} '
                '--defaults-file={defaults_file} '
                '--changelog-file={changelog_file} ')
    ROLLBACK_SQL = ('liquibase rollback-sql '
                    '--tag={version} '
                    '--defaults-file={defaults_file} '
                    '--changelog-file={changelog_file} ')
    ROLLBACK_CONTEXT = ('liquibase rollback '
                        '--defaults-file={defaults_file} '
                        '--changelog-file={changelog_file} '
                        '--contexts "{context}" '
                        '--tag={version} ')

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

    @property
    def iliq_cache_path(self):
        return os.path.join(self.dir_tree.parent_dir, _CACHE_DIR_NAME)

    def generate_change_log(self):
        dump_file_path = os.path.join(self.dir_tree.parent_dir, self.dump_file_name)
        cmd = LiqCommands.CHANGELOG_GEN_FROM_DB.format(changelog_file=dump_file_path,
                                                       defaults_file=self.defaults_file)
        subprocess.run(cmd, shell=True, cwd=self.dir_tree.parent_dir,)

    def create_liq_tables(self):
        cmd = LiqCommands.TAG_DATABASE.format(defaults_file=self.defaults_file,
                                              version='init_tag')

        subprocess.run(cmd, shell=True, cwd=self.dir_tree.parent_dir)

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

        print(res)

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

    def rollback(self, version: str, contexts: list = None):
        if contexts:
            cmd = LiqCommands.ROLLBACK_CONTEXT.format(context=','.join(contexts),
                                                      changelog_file=self.change_log.file_name,
                                                      defaults_file=self.defaults_file,
                                                      version=version)
        else:
            cmd = LiqCommands.ROLLBACK.format(changelog_file=self.change_log.file_name,
                                              defaults_file=self.defaults_file,
                                              version=version)
        subprocess.run(cmd, shell=True, cwd=self.dir_tree.parent_dir)

    def get_rollback_sql(self, version: str, contexts: list = None):
        if contexts:
            ...
        else:
            cmd = LiqCommands.ROLLBACK_SQL.format(changelog_file=self.change_log.file_name,
                                                  defaults_file=self.defaults_file,
                                                  version=version)

        subprocess.run(cmd, shell=True, cwd=self.dir_tree.parent_dir)

    def put_tag(self, version: str):
        version_tag = VersionTag(change_set_id=version,
                                 author=self.os_user,
                                 version=version)

        self.change_log.add_version_tag(version_tag)

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
                               change_sql_paths=[object_rec['sql_file_path']],
                               rollback_sql_paths=[object_rec.get('rollback_file_path')])

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

    def save_cache(self):
        if not os.path.exists(self.iliq_cache_path):
            os.mkdir(self.iliq_cache_path)
        cache = {
            'changelog_type': self.dir_tree.changelog_type.name,
            'rollbacks': self.dir_tree.rollbacks,
            'rdbms_type': self.db_driver.rdbms_type,
            'dir_tree_encoding': self.dir_tree.encoding
        }
        cache = json.dumps(cache)
        cache_path = os.path.join(self.iliq_cache_path, _CACHE_FILE_NAME)
        if not os.path.exists(cache_path):
            with open(cache_path, 'w') as f:
                f.write(cache)


def get_iliq_cache(parent_path):
    cache_path = os.path.join(parent_path, _CACHE_DIR_NAME, _CACHE_FILE_NAME)
    try:
        with open(cache_path, 'r') as f:
            content = json.loads(f.read())
    except FileNotFoundError:
        content = None

    return content


def format_cmd(cmd: str):
    res = re.sub(r'^\s+|\s+$', '', cmd)
    res = res.lower()
    return res


def y_n_bool(answer: str):
    answer = format_cmd(answer)
    return answer == 'y'


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
                             'put_tag': (self.run_add_tag,
                                         self.interpreter.put_tag,
                                         7, 'Adds a new tag to changelog'),
                             'rollback': (self.run_rollback,
                                          self.interpreter.rollback,
                                          8, 'Rollback database to a specific version'),
                             'get_rollback_sql': (self.run_rollback,
                                                  self.interpreter.get_rollback_sql,
                                                  9, 'Prints rollback sql commands'),
                             'save_change_log': (self.run_command,
                                                 self.interpreter.save_change_log,
                                                 10, 'Saves changelog file'),
                             'print_change_log': (self.run_command,
                                                  self.interpreter.print_change_log,
                                                  11, 'Prints current change log'),
                             'exit': (self.run_command,
                                      self.exit,
                                      12, 'Stop and exit'),
                             'print': (self.run_command,
                                       self.print_self,
                                       13, 'Prints Iliq instance properties'),
                             'help': (self.run_command,
                                      self.print_help,
                                      14, 'Prints this message')}

    @property
    def dir_tree(self):
        return self.interpreter.dir_tree

    @property
    def db_driver(self):
        return self.interpreter.db_driver

    def welcome(self):
        print(f'Welcome to Interactive interface Iliq.'
              f'\n{self.interpreter}')

    def print_help(self):
        as_list = [(k, v) for k, v in self.commands_map.items()]
        for k, v in sorted(as_list, key=lambda x: x[1][2]):
            print(f'Command {k} hint: {v[3]}')

    def print_self(self):
        print(self.interpreter)

    def cmd_lookup(self, cmd):
        if cmd in self.commands_map:
            return cmd
        matches = get_close_matches(cmd, self.commands_map, 3, cutoff=0.4)
        if matches:
            print('Exact command is not found; closest options: \n', '\n'.join(matches))
        else:
            print('Exact command is not found')

        return ''

    # noinspection PyArgumentList
    def run(self):
        self.welcome()
        while True:
            cmd = input('>>> ')
            cmd = format_cmd(cmd)
            cmd = self.cmd_lookup(cmd)
            if cmd:
                self.commands_map[cmd][0](cmd)
            else:
                continue

    @staticmethod
    def ask_for_contexts():
        contexts = []
        while True:
            cmd = input('>>> do we need to filter some contexts? (y/n) ')
            answer = y_n_bool(cmd)
            if not answer:
                break

            context = input('>>> enter a context: ')
            context = format_cmd(context)
            contexts.append(context)

        contexts = list(set(contexts))

        return contexts if contexts else None

    def ask_for_changeset(self):
        res = {
            'object_type': format_cmd(input('>>> enter an object type: ')),
            'schema_name': format_cmd(input('>>> enter a schema name: ')),
            'object_name': format_cmd(input('>>> enter an object name: '))
        }
        self.dir_tree.add_paths_to_object_rec(res)
        return res

    def run_command(self, cmd):
        self.commands_map[cmd][1]()

    # noinspection PyArgumentList
    def run_context_command(self, cmd):
        contexts = self.ask_for_contexts()
        self.commands_map[cmd][1](contexts)

    # noinspection PyArgumentList
    def run_add_changeset(self, cmd):
        object_rec = self.ask_for_changeset()
        self.commands_map[cmd][1](object_rec)

    def run_add_tag(self, cmd):
        version = format_cmd(input('>>> enter a new version tag: '))
        self.commands_map[cmd][1](version)

    def run_rollback(self, cmd):
        contexts = self.ask_for_contexts()
        version = format_cmd(input('>>> enter version to rollback: '))
        self.commands_map[cmd][1](version, contexts)

    def exit(self):
        answer = True
        if not self.interpreter.change_log.saved:
            answer = input('>>> Changelog got some changes and wasn''t saved. Continue? (y/n) ')
            answer = y_n_bool(answer)

        if answer:
            self.interpreter.save_cache()
            print(f'Closing liquibase session for {self.db_driver.db_name}')
            exit()


def cli_startup():

    print('Hello there!\nLet''s prepare Iliq instance...')
    env_path = input('Enter .env file path: ')
    env_path = Path(format_cmd(env_path))
    load_dotenv(dotenv_path=env_path)

    project_path = get_project_path()
    cache = get_iliq_cache(project_path)

    if cache:
        changelog_type = ChangelogTypes[cache['changelog_type']]
        rdbms_type = cache['rdbms_type']
        rollbacks = cache['rollbacks']
        tree_encoding = cache['dir_tree_encoding']
    else:
        rdbms_types = ', '.join([t.name for t in RDBMSTypes])
        rdbms_type = input(f'Enter RDBMS type ({rdbms_types} are supported): ')
        rdbms_type = format_cmd(rdbms_type)

        changelog_types = ', '.join([t.name for t in ChangelogTypes])
        changelog_type = input(f'Enter Changlog type ({changelog_types} are supported): ')
        changelog_type = ChangelogTypes[format_cmd(changelog_type)]

        rollbacks = input(f'Rollbacks support? (y/n): ')
        rollbacks = y_n_bool(rollbacks)

        tree_encoding = input('Enter project directory tree encoding: ')
        tree_encoding = format_cmd(tree_encoding)

    db_driver = get_db_driver(rdbms_type)
    dir_tree = DirTree(db_driver,
                       project_path,
                       changelog_type=changelog_type,
                       rollbacks=rollbacks,
                       tree_encoding=tree_encoding)

    properties_file_name = os.environ.get('ILIQ_PROPERTIES_FILE')
    if not properties_file_name:
        properties_file_name = input('Enter Liquibase .properties file name: ')
    properties_file_name = format_cmd(properties_file_name)

    change_log_file_name = os.environ.get('ILIQ_CHANGELOG_FILE')
    if not change_log_file_name:
        change_log_file_name = input('Enter general changelog file name: ')
    change_log_file_name = format_cmd(change_log_file_name)

    iliq = LiqInterpreter(db_driver, dir_tree, properties_file_name, change_log_file_name)
    server = LiqInterface(iliq)

    print()

    server.run()
