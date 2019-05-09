import getpass
import logging
from copy import deepcopy
import pandas as pd

logger = logging.getLogger(__name__)


class Connection(object):
    _username = None
    _password = None
    _conn_params = {'host': 'localhost', 'port': 5433, 'database': 'db'}
    _backend_connection = None

    def __init__(self, credentials=None, store_credentials=False, conn_params=None):
        password, username = None,None if credentials is None else credentials.get_login()
        store_credentials = (password is not None or username is not None) or store_credentials
        if store_credentials:
            if username is None:
                print("Username: ")
                self._username = getpass._raw_input()
            else:
                self._username = username

            if password is None:
                self._password = getpass.getpass()
            else:
                self._password = password

        self._conn_params = conn_params or self._conn_params

    def _get_conn_parameters(self):
        tmp = deepcopy(self._conn_params)
        tmp.update({'user': self.username, 'password': self.password})
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

    @property
    def password(self):
        if self._password is None:
            return getpass.getpass()
        else:
            return self._password

    @property
    def username(self):
        if self._username is None:
            print("Username: ")
            return getpass._raw_input()
        else:
            return self._username

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
