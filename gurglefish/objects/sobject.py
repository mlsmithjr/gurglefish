#    Copyright 2018, 2019 Marshall L Smith Jr
#
#    This file is part of Gurglefish.
#
#    Gurglefish is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    Gurglefish is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Gurglefish.  If not, see <http://www.gnu.org/licenses/>.

from typing import Dict, Optional, Set


class SFError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class SObjectField(object):
    def __init__(self, field: Dict):
        self.field = field

    @property
    def name(self) -> str:
        return self.field['name']

    @property
    def is_custom(self) -> bool:
        return self.field['custom']

    @property
    def digits(self) -> int:
        return self.field['digits']

    @property
    def label(self) -> str:
        return self.field['label']

    @property
    def length(self) -> int:
        return self.field['length']

    @property
    def precision(self) -> int:
        return self.field['precision']

    @property
    def scale(self) -> int:
        return self.field['scale']

    @property
    def references(self) -> [str]:
        return self.field.get('referenceTo', [])

    @property
    def relationship_name(self) -> str:
        return self.field.get('relationshipName', None)

    @property
    def get_type(self):
        return self.field['type']

    @property
    def is_unique(self) -> bool:
        return self.field['unique']

    @property
    def is_externalid(self) -> bool:
        return self.field['externalId']

    @property
    def is_idlookup(self) -> bool:
        return self.field['idLookup']


class SObjectFields(object):
    def __init__(self, fields):
        self.fields = dict()
        for field in fields:
            if isinstance(field, dict):
                if field['type'] == 'address':
                    continue
                name = field['name']
            else:
                if field.type == 'address':
                    continue
                name = field.name
            self.fields[name.lower()] = SObjectField(field)

    def find(self, name: str) -> Optional[SObjectField]:
        return self.fields.get(name.lower(), None)

    def names(self) -> Set:
        return set(self.fields.keys())

    def values(self) -> [SObjectField]:
        return self.fields.values()

    def values_exportable(self) -> [Dict]:
        result = list()
        for f in self.fields.values():
            result.append(f.field)
        return result


class ColumnMap(object):
    def __init__(self, d: Dict):
        self.field = d

    @staticmethod
    def from_parts(fieldlen: int, dml: str, table_name: str, sobject_field: str,
                   db_field: str, fieldtype: str):
        return ColumnMap({'fieldlen': fieldlen, 'dml': dml, 'table_name': table_name,
                          'sobject_field': sobject_field, 'db_field': db_field, 'fieldtype': fieldtype})

    @property
    def fieldlen(self) -> int:
        return self.field['fieldlen']

    @property
    def table_name(self) -> str:
        return self.field['table_name']

    @property
    def sobject_field(self) -> str:
        return self.field['sobject_field']

    @property
    def db_field(self) -> str:
        return self.field['db_field']

    @property
    def field_type(self) -> str:
        return self.field['fieldtype']

    @property
    def dml(self) -> str:
        return self.field['dml']

    def as_dict(self) -> Dict:
        return self.field
