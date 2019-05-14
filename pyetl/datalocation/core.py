import pandas as pd
from pyetl.utils.iterables import is_listlike


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

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self.__str__())

    def __str__(self):
        return '{}'.format(self.get_location())

    def to_string(self):
        return ['{}'.format(l) for l in self.get_location()]

    # methods (Access = protected)
    def __init__(self, location):
        # DATALOCATION Construct an instance of this class
        self._location = self._get_list_from_input(location)

    def __len__(self):
        return self.size()

    def __getitem__(self, item):
        return self._location[item]

    def __iter__(self):
        for x in self._location:
            yield x

    @staticmethod
    def _get_list_from_input(input):
        _, out = is_listlike(input)
        return out
