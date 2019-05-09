from tqdm import tqdm
import pandas as pd
import numpy as np
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from pyetl.connections.vertica_connection import VerticaConnection
from pyetl.datasource.core import DataSource


class VerticaDataSource(DataSource, VerticaConnection):
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

        if self.mode_is_read_only() or self.mode_is_append():
            # Verify that exists
            for l in self.get_location():
                name, schema = l.split('.')
                assert self.table_exist(name, schema=schema), "Table {} does't exists".format(name)


