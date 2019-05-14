import pandas as pd
from pyetl.utils.iterables import is_listlike


def _check_varname(func):
    def wrapper(*args, **kwargs):
        self = args[0]
        _, var_name = is_listlike(args[1])
        var_in_md = pd.Index(var_name).isin(self.get_variable_names())
        if not var_in_md.all():
            raise ValueError('Unknown variable(s): {}'.format(pd.Index(var_name)[~var_in_md]))
        else:
            return func(*args, **kwargs)
    return wrapper


class DataDictionary(object):
    """DATADICTIONARY Abstract representation of a data dictionary"""

    # methods (Static, Access = public)


class DatabaseDictionary(DataDictionary):
    """DATABASEDICTIONARY Dictionary for a database data source"""

    # methods (Abstract, Access = public)
    def get_schemas(self, conn):
        """
        Retrieve all existing schemas from the dictionary
        :param self:
        :param conn:
        :return: schemaName
        """
        raise NotImplementedError

    def get_tables_in_schema(self, conn, schema_name):
        """
        retrieve all existing tables within the specified schema
        :param self:
        :param conn:
        :param schema_name:
        :return: tableName
        """
        raise NotImplementedError

    def table_exist(self, conn, tbl_name):
        """
        indicate if the input tables exist
        :param self:
        :param conn:
        :param tbl_name:
        :return: isExistingTbl
        """
        raise NotImplementedError

    # methods (Abstract, Access = protected)
    def _read_metadata(self, conn, tbl_name):
        """
        Build md
        :param self:
        :param conn:
        :param tbl_name:
        :return: md
        """
        raise NotImplementedError
    
    # methods (Access = public)
    def read_metadata(self, conn, tbl_name, var_name=None):
        """READMETADATA Read all metadata from dictionary"""
        md = self._read_metadata(conn, tbl_name)
        # Extract only the required metadata
        if var_name is None:
            return md.extract_sub_catalog(var_name)


class MetadataCatalog(object):
    """METADATACATALOG Metadata catalog implementation"""

    # properties (Access = private)
    _md = None             # metadata table
    _is_numeric = None      # flag indicating numeric variables
    _is_date = None          # flag indicating date variables
    _is_time = None          # flag indicating time variables
    _is_timestamp = None     # flag indicating timestamp variables
    _is_case_sensitive = None # flag indicating if the metadata catalog is case sensitive

    # methods (Access = private)

    def _variable_getter(self, flag_array):
        """VARIABLEGETTER Return names of variables matching the input flag"""
        var_names = self.get_variable_names()
        return var_names[flag_array]

    def _check_type(self):
        """CHECKTYPE Check if each variable has a single associated type"""
        has_one_type_only = self._md.loc[:, ['IS_BOOLEAN', 'IS_INTEGER', 'IS_FLOAT', 'IS_DATE', 'IS_TIME', 'IS_TIMESTAMP', 'IS_TEXT']]
        has_one_type_only = has_one_type_only.values
        return has_one_type_only.sum(1) == 1

    # methods (Access = public)
    def __init__(self, md, is_case_sensitive):
        # METADATACATALOG Construct an instance of this class

        if (md is None) or not isinstance(md, pd.DataFrame) or not len(md):
            # TODO: add more checks
            raise ValueError('Invalid metadata table')

        # Determine each variable's type
        md['TYPE'] = pd.np.nan
        for t in {'BOOLEAN', 'INTEGER', 'FLOAT', 'DATE', 'TIME', 'TIMESTAMP', 'TEXT'}:
            md.loc[md[t], 'TYPE'] = t

        # Set properties
        self._md = md
        self._is_case_sensitive = is_case_sensitive
        # If the metadata is not case sensitive, convert variable names
        # to upper characters
        if ~self._is_case_sensitive:
            self._md.index = self._md.index.str.upper()

        # Sort the catalog according to variable names
        self._md = self._md.sort_index()
        # Set flags
        self._is_numeric = self._md['IS_BOOLEAN'] | self._md['IS_INTEGER'] | self._md['IS_FLOAT']
        self._is_time = self._md['IS_TIME']
        self._is_date = self._md['IS_DATE']
        self._is_timestamp = self._md['IS_TIMESTAMP']

    def __repr__(self):
        """DISP Display catalog"""
        return self._md.__repr__()

    def size(self, dim=None):
        # SIZE Return size of the metadata catalog
        return self._md.shape if dim is None else self._md.shape[dim]

    def __len__(self):
        return len(self._md)

    def get_variable_names(self):
        """GETVARIABLENAMES Variable names getter"""
        return self._md.index

    @_check_varname
    def get_type(self, var_name):
        """
        GETTYPE Get type of the input given variable
        Returns BOOLEAN, INTEGER, FLOAT, DATE, TIME, TIMESTAMP or TEXT
        """
        return self._md.loc[var_name, 'TYPE']

    @_check_varname
    def get_datetime_format(self, var_name):
        """GETDATETIMEFORMAT Get datetime format of the input given variable"""
        return self._md.loc[var_name, 'DATETIME_FORMAT']

    # Type-based variable getters
    def get_boolean_vars(self):
        return self._variable_getter(self._md['IS_BOOLEAN'])

    def get_int_vars(self):
        return self._variable_getter(self._md['IS_INTEGER'])

    def get_float_vars(self):
        return self._variable_getter(self._md['IS_FLOAT'])

    def get_date_vars(self):
        return self._variable_getter(self._is_date)

    def get_time_vars(self):
        return self._variable_getter(self._is_time)

    def get_timestamp_vars(self):
        return self._variable_getter(self._is_timestamp)

    def get_text_vars(self):
        return self._variable_getter(self._md['IS_TEXT'])

    # Type checkers
    @_check_varname
    def is_date_variable(self, var_name):
        return self._is_date[self.get_variable_names() == var_name]

    @_check_varname
    def is_timestamp_variable(self, var_name):
        return self._is_timestamp[self.get_variable_names() == var_name]

    @_check_varname
    def is_time_variable(self, var_name):
        return self._is_time[self.get_variable_names() == var_name]

    @_check_varname
    def is_numeric_variable(self, var_name):
        return self._is_numeric[self.get_variable_names() == var_name]

    @_check_varname
    def get_variable_sizes(self, var_name):
        """GETVARIABLESIZES Return the size (number of bytes) of the input given variables"""
        return self._md.loc[var_name, 'NUM_BYTES']

    def get_type_in_source(self):
        """GETTYPEINSOURCE Return the type in the data source along with the variable name"""
        return self._md.loc[:, 'TYPE_IN_SOURCE']

    @_check_varname
    def extract_sub_catalog(self, var_name):
        """EXTRACTSUBCATALOG Extract a subcatalog of metadata for the input given list of variables"""
        # If the input list is empty, return the current object
        if len(var_name):
            return self.__class__(self._md.copy(), self._is_case_sensitive)

        # Extract the subcatalog
        return self.__class__(self._md.loc[var_name, :].copy(), self._is_case_sensitive)

    def check_metadata_completeness(self):
        """
        CHECKMETADATACOMPLETENESS Checks metadata completeness, i.e.
        that each variable is associated to one and exactly ony data
        type. Also checks for missing date formats. Returns a boolean
        vector indicating variables with valid types associated.
        """
        # Make sure each variable has exactly one associated type
        has_one_type_only = self._check_type()
        # Check datetime formats
        requires_datetime_format = self._is_date | self._is_time | self._is_timestamp
        has_datetime_format = ~requires_datetime_format or (self._md['DATETIME_FORMAT'] != '')
        # Final output
        is_complete = has_one_type_only and has_datetime_format
        # If the def is called without any output argument, throw
        # an error if the metadata catalog is not complete
        if not all(is_complete):
            var_name = self.get_variable_names()
            var_name = var_name(~is_complete)
            raise ValueError('Invalid metadata detected for variables: {}'.format(var_name))

    @_check_varname
    def format_datetime_data(self, var_name, var_in):
        """FORMATDATETIMEDATA Apply datetime format to input variable"""
        # Get the associated row
        datetime_format = self._md.get_datetime_format(var_name)
        # Convert relevant variables to datetime
        # This function only applies to date, time and timestamp data
        if self._md.is_date_variable(var_name):
            var_out = pd.to_datetime(var_in, errors='coerce', format=datetime_format)
        elif self._md.is_time_variable(var_name):
            var_out = pd.to_timedelta(var_in, errors='coerce')
        elif self._md.is_timestamp_variable(var_name):
            var_out = pd.to_datetime(var_in, errors='coerce', unit=datetime_format)
        else:
            var_out = var_in

        return var_out
