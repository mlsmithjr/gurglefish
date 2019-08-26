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

from typing import Dict


class LocalTableConfig(object):

    def __init__(self, adict: Dict):
        self.item = adict

    @property
    def dict(self):
        return self.item

    @property
    def name(self):
        return self.item['name']

    @property
    def enabled(self):
        return self.item.get('enabled', False)

    @enabled.setter
    def enabled(self, val):
        self.item['enabled'] = val

    @property
    def auto_drop_columns(self):
        return self.item.get('auto_drop_columns', True)

    @property
    def auto_create_columns(self):
        return self.item.get('auto_create_columns', True)

    @property
    def auto_scrub(self) -> str:
        return self.item.get('auto_scrub', "daily")

    @property
    def sync_schedule(self):
        return self.item.get('sync_schedule', 'auto')

    @property
    def package_name(self):
        return self.item.get('package', None)

    @property
    def use_bulkapi(self) -> bool:
        return self.item.get('bulkapi', False)
