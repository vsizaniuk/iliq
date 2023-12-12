from copy import deepcopy

sql_file_template = {
    'path': None,
    'endDelimiter': '\n/'
}
rollback_template = [{'sqlFile': None}]

changes_template = [{'sqlFile': None}]

change_set_template = {
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

version_template = {
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

include_template = {
    'include': {
        'file': None
    }
}

main_log_template = {
    'databaseChangeLog': []
}

class ChangeSet:

    def __init__(self):
        ...

