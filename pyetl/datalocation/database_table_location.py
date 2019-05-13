from pyetl.datalocation import DatabaseLocation, DatabaseQueryLocation


class DatabaseTableLocation (DatabaseLocation):
    # DATABASEQUERY Data location as database query

    # methods (Access = public)
    def __init__(self, location):
        # DATABASETABLE Construct a database table collection object
        location = [l.upper().strip() for l in self._get_list_from_input(location)]
        super(DatabaseTableLocation, self).__init__(location)

    def get_table_name(self):
        """
        Retrieve table name
        :return: tblName
        """
        tbl_name = self.__str__()
        return tbl_name

    def append_where_clause(self, where_clause):
        """
        Append a WHERE clause to the current data location, thus forming a database query
        :param where_clause:
        :return: databaseQuery
        """
        where_clause = self._get_list_from_input(where_clause)
        if self.size() == 1 and len(where_clause) > 1:
            tbl_name = [self.get_table_name() for _ in range(len(where_clause))]
        elif self.size() == len(where_clause):
            tbl_name = self.get_table_name()
        else:
            raise('Unsupported case: {} data location(s) and {} WHERE clauses'.format(self.size(), len(where_clause)))
        
        # Form the database query
        where_clause = [w.upper() for w in where_clause]
        database_query = ['SELECT * FROM {} WHERE {}'.format(t, w) for t, w in zip(tbl_name, where_clause)]
        database_query = DatabaseQueryLocation(database_query)
        return database_query
