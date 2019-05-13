from pyetl.datalocation.core import DataLocation


class DatabaseLocation(DataLocation):
    # DATABASELOCATION Abstract data location object in a database

    # methods (Access = protected)
    def __init__(self, location):
        # DATABASELOCATION Construct an instance of this class
        location = self._get_list_of_strings(location)
        super(DatabaseLocation, self).__init__([l.upper() for l in location])
        self.check_table_name_syntax()

    # methods (Abstract, Access = public)
    def get_table_name(self):
        """
        Retrieve table name
        :return: tblName
        """
        raise NotImplementedError()

    def append_where_clause(self, where_clause):
        """
        Append WHERE clause to the current data location
        :param where_clause:
        :return: databaseQuery
        """
        raise NotImplementedError()

    # methods (Access = public)
    def get_where_clause(self):
        """
        GETWHERESTATEMENT Return WHERE statement included in the data location, by default return an array of empty
        strings
        :return: whereStmt
        """
        where_stmt = ['' for _ in range(self.size())]
        return where_stmt

    # methods (Access = private)
    def check_table_name_syntax(self):
        """
        CHECKTABLENAMESYNTAX Check syntax of the data location's table names
        """
        tbl_name = self.get_table_name()
        for idx in range(len(tbl_name)):
            # Table names are expected to be formatted as [schema name].[table name]
            tbl_name_check = tbl_name[idx].split('.')
            if (len(tbl_name_check) != 2) or (not len(tbl_name_check[0])) or (not len(tbl_name_check[1])):
                raise ValueError('Invalid table name: {}'.format(tbl_name[idx]))
