class RequiresConnection(object):
    """
    Abstract representation of a data source requiring
    user connection prior to any data manipulation
    """
     
    # properties (Access = private)
    _connection_properties = None # connection properties
    _credentials = None           # credentials

    # methods (Access = public)
    def __init__(self, connection_properties, credentials):
        """
        Construct an instance of this class
        :param connection_properties:
        :param credentials:
        """
        self.connection_properties = connection_properties
        self.credentials = credentials

    def connect(self):
        """
        Connect to the underlying data source
        :return: conn
        """
        return self._connection_properties.connect(self.credentials)
            
    # methods (Static, Access = public)
    @staticmethod
    def test_connection(conn_props, credentials, return_open_connection=True):
        """
        Test connection
        :param conn_props:
        :param credentials:
        :param return_open_connection:
        :return: (success, conn)
        """
        try:
            conn = conn_props.connect(credentials)
            success = len(conn.Message) == 0
            if not return_open_connection:
                conn.close()
        except Exception:
            success = False
            conn = []

        return success, conn
