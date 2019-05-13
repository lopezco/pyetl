import logging
from copy import deepcopy
import pandas as pd
from pyetl.credentials.core import Credentials

logger = logging.getLogger(__name__)


class Connection(object):
    _credentials = None
    _conn_params = {'host': 'localhost', 'port': 5433, 'database': 'db'}
    _backend_connection = None

    def __init__(self, credentials=None, conn_params=None):
        if credentials is None:
            # Ask for login
            cred = Credentials()
            cred.set_login()
        elif isinstance(credentials, Credentials):
            # Store as it is
            cred = credentials
        elif len(credentials) <= 2:
            # Pass as arguments
            cred = Credentials()
            cred.set_login(*credentials)
        else:
            raise ValueError('Non-supported type for credentials')
        self._credentials = cred

        self._conn_params = conn_params or self._conn_params

    def _get_conn_parameters(self):
        tmp = deepcopy(self._conn_params)
        tmp.update(self._credentials)
        return tmp

    def open(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def _test(self):
        """
        Test if the connection is valid
        :return: success
        """
        raise NotImplementedError()

    def test_connection(self):
        """
        Test connection
        :return: success
        """
        try:
            success = self._test()
        except Exception:
            success = False
        return success


def _auto_open_close(func):
    def wrapper(*args, **kwargs):
        # Connect to the data source if necessary
        args[0].open()
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            raise e
        finally:
            args[0].close()

        return result
    return wrapper


class DbConnection(Connection):
    def __init__(self, *args, **kwargs):
        super(DbConnection, self).__init__(*args, **kwargs)

    def open(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def _test(self):
        raise NotImplementedError()

    @_auto_open_close
    def fetch(self, query):
        """
        Executes the 'query' and returns the result as a pd.DataFrame

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
        raise NotImplementedError()

    def get_tables(self, **kwargs):
        """
        Return:
        =======
        out: pd.DataFrame
            Table with tables information
        """
        raise NotImplementedError()

    def table_exist(self, tables, **kwargs):
        """
        Return:
        =======
        out: pd.DataFrame
            Table with tables information
        """
        raise NotImplementedError()

    def table_owner(self, tables, **kwargs):
        """
        Return:
        =======
        out: pd.DataFrame
            Table with tables information
        """
        raise NotImplementedError()

    def get_table_columns(self, tables, **kwargs):
        """
        Params:
        =======
        tables: list[str]
            Table names

        Return:
        =======
        out: pd.DataFrame
            Table with columns information
        """
        raise NotImplementedError()

    def drop_tables(self, tables):
        raise NotImplementedError()

    def row_count(self, tbl_name, where_clause=None):
        raise NotImplementedError()
