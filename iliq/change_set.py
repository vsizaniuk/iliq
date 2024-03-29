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
                 object_type: str,
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
        self.object_type = object_type
        self._id = change_set_id
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
            change = deepcopy(_CHANGE_TEMPLATE)
            change['sqlFile'] = deepcopy(_SQL_FILE_TEMPLATE)
            change['sqlFile']['path'] = path
            self.changes.append(change)

        if rollback_sql_paths:
            for path in rollback_sql_paths:
                rollback = deepcopy(_CHANGE_TEMPLATE)
                rollback['sqlFile'] = deepcopy(_SQL_FILE_TEMPLATE)
                rollback['sqlFile']['path'] = path
                self.rollbacks.append(rollback)

    def __str__(self):
        res = f'Liq Change set: \n{self.get_json()}'
        return res

    @property
    def path(self):
        if self._path is None:
            raise ValueError(f'Change set {self.id} is not saved yet!')
        return self._path

    @property
    def id(self):
        return f'{self.schema_name}.{self._id}'

    @property
    def file_name(self):
        return f'{self.id}.json'

    @property
    def extended_id(self):
        return f'{self.schema_name}.{self._id}_{self.object_type}'

    @property
    def extended_file_name(self):
        return f'{self.extended_id}.json'

    def get_json(self, is_extended_id=False):
        change_set_json = deepcopy(_CHANGE_SET_TEMPLATE)
        change_set_json['id'] = self.extended_id if is_extended_id else self.id
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

        change_set_json = {'databaseChangeLog': [{'changeSet': change_set_json}]}
        change_set_json = pretty_json(change_set_json)

        return change_set_json

    def save_change_set(self,
                        parent_path: str,
                        encoding='utf-8'):
        self._path = os.path.join(parent_path, self.file_name)

        is_extended = True if os.path.exists(self.path) else False
        if is_extended:
            self._path = os.path.join(parent_path, self.extended_file_name)

        change_set_f = open(self.path, 'w', encoding=encoding)
        change_set_f.write(self.get_json(is_extended))
        change_set_f.close()


class VersionTag:
    def __init__(self,
                 change_set_id: str,
                 author: str,
                 version: str):
        self.id = change_set_id
        self.author = author
        self.version = version

    def __str__(self):
        return f'Liq version tag: \n{self.get_object()}'

    def get_object(self):
        res = deepcopy(_VERSION_TEMPLATE)
        res['changeSet']['id'] = self.id
        res['changeSet']['author'] = self.author
        res['changeSet']['changes'][0]['tagDatabase']['tag'] = self.version

        return res


class ChangeLog:

    def __init__(self,
                 parent_path: str,
                 changelog_file_name: str,
                 encoding='utf-8'):
        self.parent_path = parent_path
        self.file_name = changelog_file_name
        self.change_log = deepcopy(_MAIN_LOG_TEMPLATE)
        self.last_added_change_set = None
        self.saved = True

        try:
            change_log_path = os.path.join(self.parent_path, self.file_name)
            with open(change_log_path, 'r', encoding=encoding) as f:
                data = f.read()
                if data:
                    self.change_log = json.loads(data)
        except FileNotFoundError:
            ...

    def __str__(self):
        res = f'Liq Change log: \n{pretty_json(self.change_log)}'
        return res

    def add_change_set(self,
                       change_set: ChangeSet):
        include = deepcopy(_INCLUDE_TEMPLATE)
        include['include']['file'] = change_set.path.replace(self.parent_path, '.')
        self.change_log['databaseChangeLog'].append(include)
        self.last_added_change_set = change_set
        if self.saved:
            self.saved = not self.saved

    def add_version_tag(self, version_tag: VersionTag):
        self.change_log['databaseChangeLog'].append(version_tag.get_object())
        self.last_added_change_set = version_tag
        if self.saved:
            self.saved = not self.saved

    def save_change_log(self,
                        encoding='utf-8'):
        change_log_path = os.path.join(self.parent_path, self.file_name)
        change_log_f = open(change_log_path, 'w', encoding=encoding)
        change_log_f.write(pretty_json(self.change_log))
        change_log_f.close()
        self.saved = not self.saved
