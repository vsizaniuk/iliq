import os
import subprocess
import re

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

    def get_update_sql(self, contexts: list=None):
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

    def update(self, contexts: list=None):
        if contexts:
            cmd = LiqCommands.CONTEXT_UPDATE.format(context=','.join(contexts),
                                                    changelog_file=self.change_log.file_name,
                                                    defaults_file=self.defaults_file)
        else:
            cmd = LiqCommands.UPDATE.format(changelog_file=self.change_log.file_name,
                                            defaults_file=self.defaults_file)
        subprocess.run(cmd, shell=True, cwd=self.dir_tree.parent_dir)

    def init_project(self):

        self.dir_tree.create_dir_tree(recreate=True)
        self.generate_change_log()

        def put_change_set(p_object_rec):
            o_type = DDLTypesMap[p_object_rec['object_type']]
            change_set = ChangeSet(schema_name=p_object_rec['schema_name'],
                                   object_type=o_type.name,
                                   change_set_id=p_object_rec['object_name'],
                                   author=self.os_user,
                                   context=o_type.liq_context,
                                   dbms=self.db_driver.rdbms_type,
                                   run_always=o_type.run_always,
                                   run_on_change=o_type.run_on_change,
                                   fail_on_error=o_type.fail_on_error,
                                   comment=f'{p_object_rec["object_name"]} {o_type.name} creation scrip',
                                   change_sql_paths=[p_object_rec['sql_file_path']])

            parent_path = os.path.join(self.dir_tree.united_liq_path, change_set.schema_name)
            change_set.save_change_set(parent_path, self.dir_tree.encoding)

            self.change_log.add_change_set(change_set)

        dump_file_path = os.path.join(self.dir_tree.parent_dir, self.dump_file_name)
        for object_rec in self.dir_tree.put_ddl_file_into_tree(dump_file_path):
            put_change_set(object_rec)

        for func in (self.dir_tree.put_composite_types_into_tree,
                     self.dir_tree.put_views_routines_triggers_into_tree):
            for object_rec in func():
                put_change_set(object_rec)

        self.change_log.save_change_log(encoding=self.dir_tree.encoding)
