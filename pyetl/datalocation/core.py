import pandas as pd


class DataLocation(object):
    """DATALOCATION Abstract data location, specifies where data is stored"""
    
    # properties (Access = protected)
    _location = []
        
    # methods (Access = public)
    def size(self):
        # SIZE Return size
        return len(self._location)

    def get_location(self):
        # GETLOCATION Location getter
        return self._location

    def __str__(self):
        return '{}'.format(self._location)

    # methods (Access = protected)
    def __init__(self, location):
        # DATALOCATION Construct an instance of this class
        self._location = self._format_location_input(location)

    @staticmethod
    def _format_location_input(location):
        return location if pd.api.types.is_list_like(location) else [location]
