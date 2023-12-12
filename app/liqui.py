import os
import subprocess

from enum import Enum
from .db_connectors import DBAccess
from .dir_tree import DirTree


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

    def generate_change_log(self):
        cmd = LiqCommands.CHANGELOG_GEN_FROM_DB.format(changelog_file=f'dump_4_{self.db_driver.db_name}.sql',
                                                       defaults_file=self.defaults_file)
        subprocess.run(cmd, shell=True)
