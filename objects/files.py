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
    def sync_schedule(self):
        return self.item.get('sync_schedule', 'auto')

    @property
    def package_name(self):
        return self.item.get('package', None)

    @property
    def use_bulkapi(self) -> bool:
        return self.item.get('bulkapi', False)
