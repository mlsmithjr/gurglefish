from schema import SFSchemaManager

__author__ = 'mark'


class SFImporter:
    context = None

    def __init__(self, context, schema_mgr : SFSchemaManager):
        self.context = context
        self.storagedir = context.filemgr.exportdir
        self.schema_mgr = schema_mgr

    def bulk_load(self, sobject_name):

        if not self.context.dbdriver.table_exists(sobject_name):
            self.schema_mgr.create_table(sobject_name)

        return self.context.dbdriver.import_native(sobject_name)
