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
        return '{}'.format(self.get_location())

    def to_string(self):
        return ['{}'.format(l) for l in self.get_location()]

    # methods (Access = protected)
    def __init__(self, location):
        # DATALOCATION Construct an instance of this class
        self._location = self._get_list_from_input(location)

    @staticmethod
    def _get_list_from_input(input):
        return input if pd.api.types.is_list_like(input) else [input]
