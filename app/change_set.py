import os.path
import pprint
import json
from copy import deepcopy

_SQL_FILE_TEMPLATE = {
    'path': None,
    'endDelimiter': '\n/'
}

_CHANGE_TEMPLATE = {'sqlFile': None}

_CHANGE_SET_TEMPLATE = {
    'id': None,
    'author': None,
    'context': None,
    'dbms': None,
    'runAlways': None,
    'runOnChange': None,
    'failOnError': None,
    'comment': None,
    'changes': None,
    'rollback': None
}

_VERSION_TEMPLATE = {
    'changeSet': {
        'id': None,
        'author': None,
        'context': 'version',
        'changes': [
            {
                'tagDatabase': {
                    'tag': None
                }
            }
        ]
    }
}

_INCLUDE_TEMPLATE = {
    'include': {
        'file': None
    }
}

_MAIN_LOG_TEMPLATE = {
    'databaseChangeLog': []
}


def pretty_json(obj: dict | list) -> str:
    obj = pprint.pformat(obj, width=140, sort_dicts=False)
    obj = (obj.replace('\'', '"').
           replace('True', 'true').
           replace('False', 'false'))
    return obj


class ChangeSet:

    def __init__(self,
                 schema_name: str,
                 change_set_id: str,
                 author: str,
                 context: str,
                 dbms: str,
                 run_always: bool,
                 run_on_change: bool,
                 fail_on_error: bool,
                 comment: str,
                 change_sql_paths: list,
                 rollback_sql_paths: list = None):
        self.schema_name = schema_name
        self.id = change_set_id
        self.author = author
        self.context = context
        self.dbms = dbms
        self.run_always = run_always
        self.run_on_change = run_on_change
        self.fail_on_error = fail_on_error
        self.comment = comment
        self.changes = []
        self.rollbacks = []
        self._path = None

        for path in change_sql_paths:
            change = deepcopy(_SQL_FILE_TEMPLATE)['path'] = path
            change = deepcopy(_CHANGE_TEMPLATE)['sqlFile'] = change
            self.changes.append(change)

        if rollback_sql_paths:
            for path in rollback_sql_paths:
                rollback = deepcopy(_SQL_FILE_TEMPLATE)['path'] = path
                rollback = deepcopy(_CHANGE_TEMPLATE)['sqlFile'] = rollback
                self.rollbacks.append(rollback)

    @property
    def path(self):
        if self._path is None:
            raise ValueError(f'Change set {self.id} is not saved yet!')
        return self._path

    def get_json(self):
        change_set_json = deepcopy(_CHANGE_SET_TEMPLATE)
        change_set_json['id'] = self.id
        change_set_json['author'] = self.author
        change_set_json['context'] = self.context
        change_set_json['dbms'] = self.dbms
        change_set_json['runAlways'] = self.run_always
        change_set_json['runOnChange'] = self.run_on_change
        change_set_json['failOnError'] = self.fail_on_error
        change_set_json['comment'] = self.comment
        change_set_json['changes'] = self.changes
        if self.rollbacks:
            change_set_json['rollback'] = self.rollbacks
        else:
            change_set_json.pop('rollback')

        change_set_json = pretty_json(change_set_json)

        return change_set_json

    @property
    def file_name(self):
        return f'{self.id}.json'

    def save_change_set(self,
                        parent_path: str,
                        encoding='utf-8'):
        self._path = os.path.join(parent_path, self.file_name)

        change_set_f = open(self.path, 'w', encoding=encoding)
        change_set_f.write(self.get_json())
        change_set_f.close()


class ChangeLog:

    def __init__(self,
                 parent_path: str,
                 changelog_file_name: str):
        self.parent_path = parent_path
        self.file_name = changelog_file_name
        self.change_log = deepcopy(_MAIN_LOG_TEMPLATE)

    def add_change_set(self,
                       change_set: ChangeSet):
        include = deepcopy(_INCLUDE_TEMPLATE)
        include['include']['file'] = change_set.path.replace(self.parent_path, '.')
        self.change_log['databaseChangeLog'].append(include)

    def save_change_log(self,
                        encoding='utf-8'):
        change_log_path = os.path.join(self.parent_path, self.file_name)
        change_log_f = open(change_log_path, 'w', encoding=encoding)
        change_log_f.write(pretty_json(self.change_log))
        change_log_f.close()
