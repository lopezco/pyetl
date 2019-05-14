import pandas as pd
import numpy as np
from pyetl.connections.vertica_connection import VerticaConnection
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
        Vertica Table

        Params:
        =======
        connection_client: VerticaClient
            Vertica connection client
        name: str
            Table name on Vertica including schema
        **kwargs:
            Arguments to configure a client. See `VerticaClient`

        Return:
        =======
        out: VerticaTable
            Vertica Table wrapper

        """
        super(VerticaDataSource, self).__init__(*args, **kwargs)

        # Verify that exists
        for l in self.get_location():
            name, schema = l.split('.')
            exists = self.table_exist(name, schema=schema)
            if self.mode_is_read_only() or self.mode_is_append() and not exists:
                raise ValueError("Table {} does't exists".format(name))
            elif self.mode_is_create() and exists:
                raise ValueError("Table {} already exists".format(name))

    # methods (Access = public)
    def sql_date_formatter(self):
        """
        Date formatter for SQL queries
        :return: fun
        """
        fun = _date_to_str
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
            vertica_type[is_text_variable] = 'VARCHAR(', + (4 * metadata.get_variable_sizes(var_name_text_vars)).astype(str) + ')'

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
