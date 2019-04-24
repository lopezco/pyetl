class DataDictionary(object):
    """DATADICTIONARY Abstract representation of a data dictionary"""

    # methods (Static, Access = public)


class DatabaseDictionary(DataDictionary):
    """DATABASEDICTIONARY Dictionary for a database data source"""

    # methods (Abstract, Access = public)
    def get_schemas(self, conn):
        """
        Retrieve all existing schemas from the dictionary
        :param self:
        :param conn:
        :return: schemaName
        """
        raise NotImplementedError

    def get_tables_in_schema(self, conn, schema_name):
        """
        retrieve all existing tables within the specified schema
        :param self:
        :param conn:
        :param schema_name:
        :return: tableName
        """
        raise NotImplementedError

    def table_exist(self, conn, tbl_name):
        """
        indicate if the input tables exist
        :param self:
        :param conn:
        :param tbl_name:
        :return: isExistingTbl
        """
        raise NotImplementedError

    # methods (Abstract, Access = protected)
    def _read_metadata(self, conn, tbl_name):
        """
        Build md
        :param self:
        :param conn:
        :param tbl_name:
        :return: md
        """
        raise NotImplementedError
    
    # methods (Access = public)
    def read_metadata(self, conn, tbl_name, var_name=None):
        """READMETADATA Read all metadata from dictionary"""
        md = self._read_metadata(conn, tbl_name)
        # Extract only the required metadata
        if var_name is None:
            return md.extract_sub_catalog(var_name)
