from typing import List, Dict


class ObjectFieldChangePayload(object):
    change = None

    def __init__(self, change):
        self.change = change

    def unwrapped(self) -> Dict:
        return self.change

    @property
    def selected(self) -> bool:
        return self.change['selected']

    @property
    def db_name(self) -> str:
        return self.change['db_name']

    @property
    def type(self) -> str:
        return self.change['type']

    @property
    def length(self) -> int:
        return self.change['length']

    @property
    def field(self) -> str:
        return self.change['field']

    @property
    def deleted(self) -> bool:
        return self.change['deleted']


class ObjectChangePayload(object):
    change = None
    fields = list()

    def __init__(self, change):
        self.change = change
        for f in change['fields']:
            self.fields.append(ObjectFieldChangePayload(f))

    @property
    def fields(self) -> List[ObjectFieldChangePayload]:
        return self.fields

    def unwrapped_(self) -> Dict:
        return self.change

    @property
    def selected(self) -> bool:
        return self.change['selected']

    @property
    def table(self) -> str:
        return self.change['table']

    @property
    def sobject(self) -> str:
        return self.change['sobject']

    @property
    def deleted(self) -> bool:
        return self.change['deleted']

    @property
    def dirty(self) -> bool:
        return self.change['dirty']


class ChangePayload(object):
    db = None
    changes = []

    def __init__(self, js):
        self.db = js['db']
        for c in js['changes']:
            self.changes.append(ObjectChangePayload(c))

    @property
    def changes(self) -> List[ObjectChangePayload]:
        return self.changes

    @property
    def dbname(self):
        return self.db['dbname']

