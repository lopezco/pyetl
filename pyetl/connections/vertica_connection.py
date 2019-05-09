import pandas as pd
import logging
from pyetl.connections.core import DbConnection, _auto_open_close

logger = logging.getLogger(__name__)

try:
    import vertica_python as vpy
except ImportError:
    logger.warning("vertica_python is not installed. VerticaClient won't be available")


class VerticaConnection(DbConnection):
    """
    Example to create a new class:
    ```python
    from pyetl.connections import VerticaConnection

    conn = VerticaConnection(conn_params={'host': 'myhost1.com',
                                          'port': 5433, 'database': 'mydatabase', 'connection_load_balance': True,
                                          'backup_server_node': ['myhost2:5433', 'myhost3:5433']})
    conn.fetch("SELECT * FROM MYSCHEMA.MYTABLE LIMIT 10")
    ```
    """
    _backend_connection = None
    _conn_params = {'host': 'localhost', 'port': 5433, 'database': 'db'}

    def __init__(self, *args, **kwargs):
        super(VerticaConnection, self).__init__(*args, **kwargs)

    # Abstract functions
    def open(self):
        self._backend_connection = vpy.connect(**self._get_conn_parameters())

    def close(self):
        self._backend_connection.close()
        self._backend_connection = None

    def _test(self):
        try:
            self.open()
        except Exception:
            return False
        else:
            return True

    # Connection specific functions
    @_auto_open_close
    def fetch(self, query):
        """
        Executes the 'query' and returns the result as a dataframe

        Params:
        =======
        query: str
            Query to execute

        Return:
        =======
        out: pandas.DataFrame
            The result table
        """
        return pd.read_sql(query, self._backend_connection).replace({None: pd.np.nan})

    @_auto_open_close
    def execute(self, query):
        cur = self._backend_connection.cursor()
        cur.execute(query)

    def get_schemas(self):
        """
        Return:
        =======
        out: pd.DataFrame
            Table with tables information
        """
        query = """
        SELECT DISTINCT table_schema 
        FROM v_catalog.tables
        """
        return self.fetch(query)

    def get_tables(self, **kwargs):
        """
        Params:
        =======
        schemas: list
            Vertica schema where the tables are defined

        Return:
        =======
        out: pd.DataFrame
            Table with tables information
        """
        schemas = kwargs.get('schemas', None)
        base_query = """
        SELECT table_schema, table_name
        FROM v_catalog.tables
        """
        if schemas is not None:
            if isinstance(schemas, str):
                schemas = [schemas]

            base_query += "WHERE LOWER(table_schema) LIKE '{}'"

            query = """
            UNION ALL
            """.join([base_query.format(s.lower()) for s in schemas])
        else:
            query = base_query

        return self.fetch(query)

    def table_exist(self, tables, **kwargs):
        """
        Params:
        =======
        schemas: list
            Vertica schema where the tables are defined

        Return:
        =======
        out: pd.DataFrame
            Table with tables information
        """
        schema = kwargs['schema']
        return len(self.table_owner(tables, schema=schema)) > 0

    def table_owner(self, tables, **kwargs):
        """
        Params:
        =======
        schemas: list
            Vertica schema where the tables are defined

        Return:
        =======
        out: pd.DataFrame
            Table with tables information
        """
        schema = kwargs['schema']

        if isinstance(tables, str):
            tables = [tables]

        queries = []
        for t in tables:
            queries.append("""
            SELECT table_schema, table_name, owner_name
            FROM v_catalog.tables
            WHERE LOWER(table_schema) LIKE '{}'  AND LOWER(table_name) LIKE '{}'
            """.format(schema.lower(), t.lower()))

        query = """
            UNION ALL
            """.join(queries)
        return self.fetch(query)

    def get_table_columns(self, tables, **kwargs):
        """
        Params:
        =======
        tables: list[str]
            Table names
        schema: str
            Vertica schema where the tables are defined

        Return:
        =======
        out: pd.DataFrame
            Table with columns information
        """
        schema = kwargs.get('schema', None)

        base_query = """
        SELECT table_schema, table_name, column_name, data_type
        FROM v_catalog.columns
        WHERE LOWER(table_name) LIKE '{}'
        """
        if schema is not None:
            base_query += "AND LOWER(table_schema) LIKE '{}'".format(schema.lower())

        query = """
        UNION ALL
        """.join([base_query.format(t.lower()) for t in tables])

        return self.fetch(query)

    def drop_tables(self, tables):
        for t in tables:
            logger.info('Dropping table: {}'.format(t))
            try:
                self.execute('DROP TABLE {}'.format(t))
            except Exception as e:
                logger.error('Could not drop table {}: {}'.format(t, e))
