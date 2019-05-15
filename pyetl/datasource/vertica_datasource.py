import pandas as pd
import numpy as np
from pyetl.connections.vertica_connection import VerticaConnection
from pyetl.utils.datetime import date_to_str
from pyetl.utils.iterables import chunker
from pyetl.datasource.core import DatabaseDataSource
import logging

logger = logging.getLogger(__name__)


class VerticaDataSource(DatabaseDataSource, VerticaConnection):
    """
    Example to create a new class to automatically handle connections:

    ```python
    from pydatabase.vertica.vertica_table import VerticaTable, VerticaClient
    import logging

    logger = logging.getLogger(__name__)


    class MyVerticaClient(VerticaClient):

        _conn_params = {
            'host': 'myhost1.com',
            'port': 5433, 'database': 'mydatabase', 'connection_load_balance': True,
            'backup_server_node': ['myhost2:5433', 'myhost3:5433']}


    class MyVerticaTable(VerticaTable):
        _connection_client_class = MyVerticaClient


    if __name__ == '__main__':
        vtbl = MyVerticaTable('MYSCHEMA.MYTABLE', **{'username': 'myusername'})

    ```
    """

    def __init__(self, *args, **kwargs):
        """
        Vezrtica data source
        :param connection_client: Vertica connection client
        :param name: Table name on Vertica including schema
        :param kwargs:  Arguments to configure a client. See `VerticaClient`
        """
        super(VerticaDataSource, self).__init__(*args, **kwargs)

    # methods (Access = public)
    def sql_date_formatter(self):
        """
        Date formatter for SQL queries
        :return: fun
        """
        fun = date_to_str
        return fun

    def technical_preprocessing(self, var, var_name=None):
        """
        Run Vertica specific preprocessing
        """
        # Nothing to do
        return var

    def create_table(self, metadata):
        """
        Generate the CREATE TABLE statement
        :param metadata:
        :return: create_table_stmt
        """
        # Name of output tables
        tbl_name = self._location().get_table_name()
        # Determine data types for the target database
        var_name = metadata.get_variable_names()
        vertica_type = np.repeat('', len(var_name))
        vertica_type[var_name.isin(metadata.get_boolean_vars())] = 'BOOLEAN'
        vertica_type[var_name.isin(metadata.get_int_vars())] = 'INTEGER'
        vertica_type[var_name.isin(metadata.get_float_vars())] = 'FLOAT'
        vertica_type[var_name.isin(metadata.get_date_vars())] = 'DATE'
        vertica_type[var_name.isin(metadata.get_time_vars())] = 'TIME'
        vertica_type[var_name.isin(metadata.get_timestamp_vars())] = 'TIMESTAMP'
        metadata.get_variables_sizes()
        # For text variables, we need to take the variable's size into account
        is_text_variable = var_name.isin(metadata.get_text_vars())
        if any(is_text_variable):
            var_name_text_vars = var_name[is_text_variable]
            vertica_type[is_text_variable] = 'VARCHAR(', + (4 * metadata.get_variable_sizes(var_name_text_vars)).astype(
                str) + ')'

        # Check that all types have been determined
        is_missing_type = vertica_type == ''
        if any(is_missing_type):
            msg = 'No type assigned to following variables: {}'.format(var_name[is_missing_type])
            logger.error(msg)
            raise ValueError(msg)

        # Create tables
        create_table_stmt = np.repeat('', len(tbl_name))

        try:
            for idx in range(len(tbl_name)):
                # Generate the CREATE TABLE statement
                logger.info('Creating table: {}'.format(tbl_name[idx]))
                variables_stmt = ', '.join(['"{v}" {t}'.format(v=v, t=t) for v, t in zip(var_name, vertica_type)])
                create_table_stmt[idx] = 'CREATE TABLE {table} ({vars}) INCLUDE SCHEMA PRIVILEGES'.format(
                    table=tbl_name[idx], vars=variables_stmt)
                logger.info('CREATE TABLE statement #{}: "{}"'.format(idx, create_table_stmt[idx]))
                # Actually create the table
                self.execute(create_table_stmt[idx])
        except Exception as e:
            logger.error('Table creation failed')
            raise e
        return create_table_stmt

    def write(self, tbl, chunksize=None, group_variable=None):
        """
        Write input data to data source through the optional connection
        :return: num_rows_inserted
        """
        chunksize = chunksize or self.get_chunk_size()
        location = self.get_location()
        if len(location) > 1:
            # Insert grouping by group_variable or split the table equally in the number of locations
            if group_variable is not None:
                # Insert grouping by group_variable
                if isinstance(group_variable, str) or (
                        isinstance(group_variable, (tuple, list)) and len(group_variable) < len(tbl)):
                    # Verify that the grouping variable(s) exist(s) in ALL the locations
                    if not all([v in self.get_metadata().get_variable_names() for v in group_variable]):
                        raise ValueError('Grouping variable does not exist in the table')
                    else:
                        pass
                elif len(group_variable) == len(tbl):
                    # Use the grouping variable as a group index
                    pass
                else:
                    raise ValueError('Wrong group_variable')

                # Create grouped data
                grouped = tbl.groupby(group_variable)

                if len(grouped) != len(location):
                    raise ValueError('The number of locations do not match with the number of groups ({} != {}'.format(
                        len(location), len(grouped)))

                # Insert each group in a location
                for idx, (_, group) in enumerate(tbl.groupby(group_variable)):
                    schema, tbl_name = location[idx].split('.')
                    group.to_sql(name=tbl_name, con=self._backend_connection, chunksize=chunksize, schema=schema,
                                 if_exists='append', index=True, index_label=None, dtype=None, method='multi')
            else:
                # Split rows in locations. /!\ The last table might get less rows
                for idx, chunk in enumerate(chunker(tbl, len(location))):
                    schema, tbl_name = location[idx].split('.')
                    chunk.to_sql(name=tbl_name, con=self._backend_connection, chunksize=chunksize, schema=schema,
                                 if_exists='append', index=True, index_label=None, dtype=None, method='multi')
        else:
            # Insert the entire table in the location
            schema, tbl_name = location[0].split('.')
            tbl.to_sql(name=tbl_name, con=self._backend_connection, chunksize=chunksize, schema=schema,
                       if_exists='append', index=True, index_label=None, dtype=None, method='multi')

    def split(self, num_splits, var_name_split):
        """
        Split the data source in multiples child data sources
        :param num_splits:
        :param var_name_split:
        :return: subds
        """
        # TODO: Implement split function
        raise NotImplementedError()
