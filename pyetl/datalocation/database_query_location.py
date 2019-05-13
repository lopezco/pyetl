from pyetl.datalocation import DatabaseLocation


class DatabaseQueryLocation(DatabaseLocation):
    # Data location as database query
    tbl_name = ''

    # methods (Access = public)
    def __init__(self, query):
        # Constructs a database query object and validates the input query
        query = [q.strip() for q in self._get_list_from_input(query)]
        for q in query:
            # Make sure the input is a valid query: it is expected to start
            # with a 'SELECT' statement and to contain a 'FROM' statement
            q = q.upper()
            if not q.startswith('SELECT ') or ' FROM ' not in q:
                raise ValueError('Invalid query: {}'.format(q))

            # It is also not expected to contain reserved keywords
            reserved_keywords = ('AS', 'JOIN', 'GROUP BY', 'LIMIT', 'PARTITION')
            if any([' ' + k + ' ' in q for k in reserved_keywords]):
                raise ('Unsupported query: {}'.format(q))

        # Call super constructor
        super(DatabaseQueryLocation, self).__init__(query)

    def get_table_name(self):
        """
        Retrieve table name from the data location queries
        :return: tbl_name
        """
        location = self.to_string()
        tbl_name = []
        # The table name is between the FROM statement and the next whitespace
        for idx in range(len(location)):
            query = location[idx].upper()
            query = query.split(' FROM ')
            query = query[1]
            if ' ' not in query:
                tbl_name.append(query.strip())
            else:
                query = query.split(' ')
                tbl_name.append(query[0].strip())
        return tbl_name

    def get_where_clause(self):
        """
        Retrieve WHERE statement from the data location queries
        :return: where_stmt
        :rtype: list[str]
        """
        location = self.to_string()
        where_stmt = []
        for idx in range(len(location)):
            query = location[idx].upper()
            if ' WHERE ' not in query:
                where_stmt.append('')
            else:
                query = query.split(' WHERE ')
                where_stmt.append(' '.join(query[1:]))
        return where_stmt

    def get_variable_names(self):
        """
        Retrieve variable names from the query. If the query does not contain variable names, return an empty array
        :return: var_name
        :rtype: list[str]
        """
        var_name_ref = ""
        var_name = []
        location = self.to_string()
        for idx in range(len(location)):
            var_name = location[idx].upper()
            # Remove the FROM statement and what comes after
            var_name = var_name.split(' FROM ')
            var_name = var_name[0].strip()
            # Remove the SELECT statement
            var_name = var_name.replace('SELECT ', '')
            # At this stage, variablesInQuery is expected to contain
            # either a '*' or variable names separated by commas
            if var_name != '*':
                var_name = [v.strip() for v in var_name.split(',')]
            else:
                var_name = []

            # Variable names are expected to be consistent in queries
            if idx == 0:
                var_name_ref = var_name
            else:
                if var_name_ref != var_name:
                    raise ValueError('Inconsistent variable names in queries')
        return var_name

    def append_where_clause(self, where_clause_input):
        """
        Append an additional WHERE clause to the database queries
        :param self:
        :param where_clause_input:
        :return: database_query_location
        :rtype: DatabaseQueryLocation
        """
        where_clause_existing = self.get_where_clause()

        # Process the input to arrange it as column-oriented cell array
        where_clause_input = [w.strip() for w in self._get_list_from_input(where_clause_input)]
        if len(where_clause_existing) != len(where_clause_input):
            raise ValueError('Size mismatch in WHERE clauses')

        # We now need to append the input WHERE clause to the existing
        # SELECT statements
        # The input WHERE clause will be appended:
        # - after a WHERE keyword if the statement does not yet contain a WHERE clause
        # - after an AND keyword otherwise
        select_stmt = []
        location = self.to_string()
        for idx in range(len(where_clause_existing)):
            append_keyword = ' AND ' if len(where_clause_existing) else ' WHERE '
            select_stmt.append('{location} {keyword} {where}'.format(location=location[idx], keyword=append_keyword,
                                                                     where=where_clause_input[idx]))
        database_query_location = self.__class__(select_stmt)
        return database_query_location
