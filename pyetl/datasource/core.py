import logging
from pyetl.connections.core import Connection
import time
import pandas as pd
import numpy as np
from copy import deepcopy

logger = logging.getLogger(__name__)


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


class DataSource(object):
    """
    DATASOURCE Abstract data source representation
    """
    
    # properties (Access = private)
    _access_mode = 'read-only'  # data source access mode: read-only, append or create
    _location = None            # data location
    _dictionary = None          # data dictionary
    _shape = (0, 0)             # data source size
    _md = None                  # metadata catalog
    _is_case_sensitive = True   # flag indicating if the data source is case sensitive when handling variable names
    _location_iterator = None    # object for iteratively reading data from the data source
    _chunk_size = 1e4           # number of rows to read/write at each step

    def __init__(self, access_mode, is_case_sensitive, location, dictionary, var_name, flag_read_metadata=True,
                 **kwargs):
        """
        Construct a generic data source
        :param access_mode:
        :param is_case_sensitive:
        :param location:
        :param dictionary:
        :param var_name:
        :param flag_read_metadata:
        """
        self._location = location
        # Set type, should be 'read-only' or 'read-write'
        # Indicates how the data source will be used
        valid_modes = {'read-only', 'append', 'create'}
        access_mode = access_mode.lower().strip()
        if access_mode not in valid_modes:
            msg = 'Unsupported access mode, should be any of the following: {}'.format(valid_modes)
            logger.error(msg)
            raise msg
        self._access_mode = access_mode

        # Set other properties
        self._dictionary = dictionary
        self._is_case_sensitive = is_case_sensitive

        # Fetch and set metadata if the data source already exists,
        # i.e. in read-only and append modes only
        if flag_read_metadata and (self.mode_is_read_only() or self.mode_is_append()):
            self.fetch_metadata()

        # In append and create modes, there can be only a single output data location
        if (self.mode_is_append() or self.mode_is_create()) and len(self.get_location()) != 1:
            msg = 'Only a single location is supported for output operations'
            logger.error(msg)
            raise ValueError(msg)

        if self.requires_connection():
            # Init connection
            super(DataSource, self).__init__(**kwargs)

    # methods (Abstract, Access = public)
    def exists(self):
        """
        Check if the data source exists
        :return flag
        """
        NotImplementedError()

    def get_name(self, idx=None):
        """
        Return the name of the idx-th data location
        :return name
        """
        NotImplementedError()

    def write(self, df):
        """
        Write input table to data source
        :return numRowsInserted
        """
        NotImplementedError()

    # def split(self, numSplits, var_nameSplit):
    #     """
    #     Split the data source in children data sources
    #     :return dsList
    #     """
    #     NotImplementedError()

    # methods (Abstract, Access = protected)
    def compute_size(self):
        """
        Get data source size
        :return size
        """
        NotImplementedError()

    def fetch_metadata(self):
        """
        Specialized function for fetching metadata
        :return md
        """
        NotImplementedError()

    def technical_preprocessing(self, var, var_name):
        """
        Data source specific pre-processing
        :return var
        """
        NotImplementedError()

    def _create_location_iterator(self):
        """
        Initialize data source reader
        return: reader
        """
        NotImplementedError()

    def has_metadata(self):
        return self.get_metadata() is not None

    def get_data_iterator(self):
        # Read data
        for li in self.get_location_iterator():
            for df in li:
                if self.has_metadata():
                    # Get variable names
                    var_name = df.columns
                    if set(self.get_variable_names()) == set(var_name):
                        msg = 'Variable names are not consistent with metadata'
                        logger.error(msg)
                        raise ValueError(msg)

                    # Loop through columns
                    for idx, name in enumerate(df.colums):
                        col = df[name]
                        # Run technical pre-processing
                        col = self.technical_preprocessing(col, var_name[idx])
                        # Transform datetime data and apply datetime formats
                        col = self.format_datetime_data(var_name[idx], col)
                        # Replace data in the table
                        df[name] = col

                logger.info('Read {} observations'.format(len(df)))
                yield df

    # methods (Access = public)
    def size(self, dim=None):
        """
        :param dim:
        :return: data source's size
        """
        return self._shape if dim is None else self._shape[dim]

    def get_location(self):
        """
        Location getter
        :return: location
        """
        return self._location.get_location()

    def get_metadata(self):
        """
        Metadata catalog getter
        :return: metadata
        """
        return self._md

    def num_data_locations(self):
        """
        :return: number of underlying data location
        """
        return self.get_location().size()

    def requires_connection(self):
        """
        :return: flag indicating if the data source requires a connection
        """
        return isinstance(self, Connection)

    def has_location_iterator(self):
        """
        Indicate if the data source already has an initialized reader
        :return: flag
        """
        return self._location_iterator is not None

    def init_location_iterator(self):
        """
        Initialize iterative data readers
        """
        logger.info('Initializing iterator')
        self._location_iterator = self._create_location_iterator()

    def read_all(self):
        """
        Read all data from source
        :return: df, elapsedTime
        """
        timer = time.time()

        # Initialize the reader(s)
        if self.has_location_iterator():
            self.init_location_iterator()

        # Read data
        result_buffer = []
        for chunk in self.get_data_iterator():
            if len(chunk):
                result_buffer.append(chunk)

        df = pd.concat(result_buffer, axis=0)

        # Check size
        if len(df) != self.size(0):
            msg = 'Size mismatch: read {} rows but expected {}'.format(len(df), self.size(0))
            logger.error(msg)
            raise ValueError(msg)

        elapsed_time = time.time() - timer
        return df, elapsed_time

    def to_read_only(self):
        """
        Get a read-only clone of the current data source
        """
        ds = deepcopy(self)
        ds._access_mode = 'read-only'
        ds._location_iterator = []
        ds._md = []
        ds.fetch_metadata()

    # methods (Access = protected)
    def get_dictionary(self):
        """
        Dictionary getter
        :param self: 
        :return: dict
        """
        return self._dictionary
    
    def get_variable_names(self):
        """
        Variable names getter
        :param self: 
        :return: dict
        """
        return self._md.get_variable_names()
    
    def get_location_iterator(self):
        """
        Iterative reader getter
        :param self: 
        :return: iterative_reader
        """
        return self._location_iterator
    
    def get_chunk_size(self):
        """
        Chunk size getter
        :param self: 
        :return: dict
        """
        return self._chunk_size
        
    def format_datetime_data(self, var_name, var_in):
        """
        Convert datetime data to datetime objects using the appropriate formats. By default, we use information
        from the metadata
        :param var_name:
        :param var_in: numpy.array
        :return: var_out
        """
        return self._md.format_datetime_data(var_name, var_in)

    def read_only_copy(self, metadata, size, data_location=None, location_reader=None):
        """
        Get a read-only copy of the current data source with some varying properties
        :param metadata: 
        :param size:
        :param data_location:
        :param location_reader:
        :return: ds
        """
        ds = deepcopy(self)
        ds._access_mode = 'read-only'
        ds._location_iterator = []
        ds._md = metadata
        ds._shape = size

        # Alter object properties
        if not len(data_location):
            ds._location = data_location
        if location_reader is not None:
            ds._location_iterator = location_reader

    # Access mode checks
    def mode_is_read_only(self):
        return self._access_mode == 'read-only'

    def mode_is_append(self):
        return self._access_mode == 'append'

    def mode_is_create(self):
        return self._access_mode == 'create'

    # methods (Access = private)
    def process_variable_names(self, var_name):
        """
        Process variable names: reshape them in column, sort them, use upper characters only if the data
        source is not case sensitive
        :param var_name:
        :return: var_name
        """
        if not len(var_name):
            return var_name
        # Convert variable names to capital letters if the data source is not case sensitive
        if not self._is_case_sensitive:
            var_name = np.char.upper(var_name)

        # Sort variable names
        var_name = np.sort(var_name)
        # Make sure variable names are unique
        if len(np.unique(var_name)) != len(var_name):
            msg = 'Variable names are not unique'
            logger.error(msg)
            raise ValueError(msg)
