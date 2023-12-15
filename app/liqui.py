import os
import subprocess

from enum import Enum
from .db_connectors import DBAccess
from .dir_tree import DirTree, DDLTypesMap, ChangelogTypes
from .change_set import ChangeSet


class LiqCommands(Enum):
    CHANGELOG_GEN_FROM_DB = ('liquibase generate-changelog '
                             '--changelog-file={changelog_file} '
                             '--defaults-file={defaults_file}')
    UPDATE_DATABASE = ('liquibase '
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
        self.changelog_file = changelog_file

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
        self.dir_tree.put_ddl_file_into_tree(self.dump_file_name)
        self.dir_tree.put_routines_into_tree()
        self.dir_tree.put_triggers_into_tree()
        self.dir_tree.put_mat_views_into_tree()
        self.dir_tree.put_composite_types_into_tree()

        for object_rec in self.dir_tree.get_objects_in_creation_order(self.dump_file_name):
            o_type = DDLTypesMap[object_rec['object_type']]
            change_set = ChangeSet(schema_name=object_rec['schema_name'],
                                   change_set_id=object_rec['object_name'],
                                   author=self.os_user,
                                   context=o_type.liq_context,
                                   dbms=self.db_driver.rdbms_type,
                                   run_always=o_type.run_always,
                                   run_on_change=o_type.run_on_change,
                                   fail_on_error=o_type.fail_on_error,
                                   comment=f'{object_rec["object_name"]} {o_type.name} creation scrip',
                                   change_sql_paths=[object_rec['sql_file_path']])

            has_united_change_log = self.dir_tree.changelog_type == ChangelogTypes.united
            if has_united_change_log:
                parent_path = self.dir_tree.united_liq_path
                change_set.save_change_set(parent_path, True, self.dir_tree.encoding)
            else:
                parent_path = os.path.join(self.dir_tree.parent_dir, f'{change_set.schema_name}_liq')
                change_set.save_change_set(parent_path, False, self.dir_tree.encoding)



