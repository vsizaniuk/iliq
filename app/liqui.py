import os
import subprocess

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

    def init_project(self):

        self.dir_tree.create_dir_tree(recreate=True)
        self.generate_change_log()

        def put_change_set(p_object_rec):
            o_type = DDLTypesMap[p_object_rec['object_type']]
            change_set = ChangeSet(schema_name=p_object_rec['schema_name'],
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

        for object_rec in self.dir_tree.put_ddl_file_into_tree(self.dump_file_name):
            put_change_set(object_rec)

        for func in (self.dir_tree.put_composite_types_into_tree,
                     self.dir_tree.put_mat_views_into_tree,
                     self.dir_tree.put_routines_into_tree,
                     self.dir_tree.put_triggers_into_tree):
            for object_rec in func():
                put_change_set(object_rec)

        self.change_log.save_change_log(encoding=self.dir_tree.encoding)
