from pyetl.datalocation.core import DataLocation
import os
from glob import glob


class FilesystemLocation(DataLocation):
    """FILECOLLECTION Data location as collection of files"""

    # methods (Access = public)
    def __init__(self, location):
        """FILECOLLECTION Construct an instance of this class"""
        # Process the input location
        location = self._get_list_of_strings(location)
        # If it contains wildcards, convert the input to an actual list of files
        parsed_location = []
        for l in location:
            parsed_location.extend(glob(os.path.abspath(l)))

        # Call the super constructor
        super(FilesystemLocation, self).__init__(parsed_location)
