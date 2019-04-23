import pandas as pd
import os
from pyetl.datasource.core import DataSource
from pyetl.datalocation import FilesystemLocation
import functools
from pyetl.utils.rowcount import rowcount


class FileDataSource(DataSource):
    # FILEDATASOURCE Summary of this class goes here
    #    Detailed explanation goes here
    
    # properties (Access = private)
    _skip_row_count = False
    _read_numeric_data_as_string = True

    # methods (Access = public)
    def __init__(self, source_type, filepath, dictionary, chunksize, skip_row_count=False, **kwargs):
        """
        FILEDATASOURCE Constructor for data source as file(s)

        :param source_type:
        :param filepath:
        :param dictionary:
        :param skip_row_count:
        :param kwargs: parameters to be passed to pandas read_csv function
        """
        location = FilesystemLocation(filepath)
        super(FileDataSource, self).__init__(source_type, True, location, dictionary, [], flag_read_metadata=False)
        self._skip_row_count = skip_row_count
        self._chunk_size = chunksize
        self._parameters = kwargs
        self.get_metadata()
        self.init_location_iterator()
        self._shape = self.compute_size()

    def num_data_locations(self):
        """NUMDATALOCATIONS For files, there is only a single data"""
        # location which itself is a collection of files
        return 1

    def exists(self):
        """EXISTS Check if the data source exists"""
        # For files, check that all files exist
        return any([os.path.exists(l) and os.path.isfile(l) for l in self.get_location()])

    def get_name(self, idx=None):
        """GETNAME Return file name as pattern"""
        if idx is not None:
            filename = self.get_location()[idx]
            filename, extension = os.path.basename(filename).split('.')
            name = (filename, extension)
        else:
            name = [tuple(os.path.basename(filename).split('.')) for filename in self.get_location()]
        return name

    def write(self, tbl):
        # TODO: WRITE Write input table to data source
        NotImplementedError()
    
    # methods (Access = protected)
    def compute_size(self):
        """COMPUTESIZE Get data source size"""
        # Get the number of rows
        data_file = self.get_location()
        if not self._skip_row_count:
            num_rows = rowcount(data_file) - (1 + self._parameters.get('header', 0) * len(data_file))
        else:
            num_rows = -1

        metadata = self.get_metadata()
        return num_rows, -1 if metadata is None else len(metadata)

    def _crerate_location_iterator(self, conn=None):
        """INITREADERINTERN Initialize data source reader"""
        # Create a datastore selfect and set it propertoes
        read_function = functools.partial(pd.read_csv, iterator=True, chunksize=self._chunk_size, **self._parameters)

        for file in self.get_location():
            chunks_iterator = read_function(file)
            yield chunks_iterator

    def fetch_metadata(self):
        """FETCHMETADATAINTERN Specialized def for fetching metadata"""
        # TODO: implement data class DataDictionary
        self._md = self.get_dictionary().read_metadata()
    
    def technical_preprocessing(self, var, var_name):
        """TECHNICALPREPROCESSING Data source specific preprocessing"""
        # Since all fields are read as text data, numeric fields have to
        # be converted to numeric data
        # TODO: implement data class DataDictionary
        if self.get_metadata().is_mumeric(var_name):
            return var.astype(float)
