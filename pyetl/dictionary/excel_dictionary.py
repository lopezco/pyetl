import pandas as pd
import os
from pyetl.dictionary.core import DataDictionary, MetadataCatalog


class ExcelDictionary(DataDictionary):
    """EXCELDICTIONARY Summary of this class goes here"""
    
    # properties (Access = private)
    _file_name = ''
    _sheet_name = ''
    _header_rownumber = None
    _col_varname = None
    _col_type = None
    _col_length = None
    _col_format = None 
    
    # methods (Access = public)
    def __init__(self, file_name, sheet_name, header_rownumber=0, col_varname='A', col_type='B', col_length='C',
                 col_format='D'):
        """EXCELDICTIONARY Data dictionary in Excel varformat"""
        # Process and set optional input arguments
        self._header_rownumber = header_rownumber
        self._col_varname = col_varname
        self._col_type = col_type
        self._col_length = col_length
        self._col_format = col_format
        
        # Set filename and make sure the file exists
        if ~os.path.exists(file_name) or ~os.path.isfile(file_name):
            raise ValueError('Dictionary file does not exist: {}'.format(file_name))
        
        self._file_name = file_name.strip()
        # Set sheet name
        self._sheet_name = sheet_name       
    
    def read_metadata(self):
        # Read all data from the Excel dictionary
        skiprows = self._header_rownumber - 1 if self._header_rownumber > 0 else None
        data = (
            pd.read_excel(self._file_name, sheet_name=self._sheet_name, header=self._header_rownumber,
                          skiprows=skiprows)
            .dropna(axis=1, how='all')
            .set_axis(['NAME', 'vartype', 'LENGTH', 'FORMAT'], axis=1, inplace=False))

        # Check the vartype of each field of the dictionary
        #   - Variable names and types are expected to be strings
        if not pd.api.types.is_string_dtype(data['NAME']):
            raise ValueError('Variable names are expected to be strings') 
        if not pd.api.types.is_string_dtype(data['vartype']):
            raise ValueError('Variable types are expected to be strings') 
        #   - Variable lengths are expected to be numeric values
        if not pd.api.types.is_numeric_dtype(data['LENGTH']):
            raise ValueError('Variable lengths are expected to be numeric values') 

        # Convert numeric formats to characters
        data['FORMAT'] = data['FORMAT'].astype(str)
        
        # Convert dictionary data to a case-sensitive metadata catalog
        md = data['LENGTH'].to_frame('NUM_BYTES')
        md.index = data['NAME']
        
        # Determine data vartype
        vartype = data['TYPE'].str.upper()
        varformat = data['FORMAT'].str.upper()
        
        # Booleans
        md['IS_BOOLEAN'] = vartype.isin(['BOOLEAN', 'LOGICAL', 'FLAG'])
        # Integers
        md['IS_INTEGER'] = vartype.isin(['INT', 'INTEGER']) | (
                (vartype == 'NUM') & (varformat.match('[0-9]+') |
                                      varformat.match('[0-9]+.{1}') |
                                      varformat.match('[0-9]+.{1}0{1}')))
        # Floats: is not an integer, vartype is NUM and FORMAT fulfills one of the following conditions:
        # - starts with any of the following: BEST, COMMA, PERCENT
        # - FORMAT is [0-9]+.[0-9]+
        # - FORMAT is F[0-9]+.[0-9]*
        # - FORMAT is RCI_[0-9]+_[0-9]+_.
        # - FORMAT is Z_[0-9]+_[0-9]+_.
        # - FORMAT is missing (i.e. default to float for numeric values)
        md['IS_FLOAT'] = (
                vartype.isin(['FLOAT', 'DECIMAL', 'NUMERIC']) |
                (vartype == 'NUM') & ~md['IS_INTEGER'] & (
                        varformat.str.upper().str.startswith('BEST') |
                        varformat.str.upper().str.startswith('COMMA') |
                        varformat.str.upper().str.startswith('PERCENT') |
                        varformat.str.match('[0-9]+.[0-9]+') |
                        varformat.str.match('F[0-9]+.[0-9]*') |
                        varformat.str.match('RCI_[0-9]+_[0-9]+_.') |
                        varformat.str.match('Z_[0-9]+_[0-9]+_.') |
                        (varformat == '') |
                        pd.isnull(varformat)))
        # Text
        md['IS_TEXT'] = vartype.isin(['CHAR' 'TEXT'])
        # Dates
        formats_date = ['DD/MM/YY', 'DDMMYY8.', 'DDMMYYS8.', 'DD/MM/YYYY', 'DDMMYY10.', 'DDMMYYS10.',
                        'DD.MM.YY', 'DDMMYYP8.', 'EURDFDD8.', 'DD.MM.YYYY', 'DDMMYYP10.', 'EURDFDD10.',
                        'DDMMYY', 'YYMMDD',
                        'YY-MM-DD', 'YYMMDD8.', 'YYMMDDD8.',
                        'YYYY-MM-DD', 'YYMMDD10.', 'YYMMDDD10.',
                        'MMMYY', 'MONYY5.', 'MMMYYYY', 'MONYY7.', 'YYYYMM', 'YYMMN6.']
        md['IS_DATE'] = (vartype == 'NUM') & varformat.isin(formats_date)
        # Timestamps
        formats_timestamp = ['DDMMYY:HH:MM:SS', 'DATETIME18.', 'DDMMYYYY:HH:MM:SS', 'DATETIME20.',
                             'YYYY-MM-DDTHH:MM:SS', 'ISO-8601', 'IS8601DT.', 'E8601DT.']
        md['IS_TIMESTAMP'] = (vartype == 'NUM') & varformat.isin(formats_timestamp)
        # Times
        formats_time = ['HH:MM', 'TIME5.', 'HH:MM:SS', 'TIME8.', 'TIME8.2']
        md['IS_TIME'] = (vartype == 'NUM') & varformat.isin(formats_time)
        
        # Set date/time formats
        md['DATETIME_FORMAT'] = ''
        is_datetime = md['IS_TIME'] | md['IS_DATE'] | md['IS_TIMESTAMP']
        for idx in range(len(md)):
            if is_datetime(idx):
                cmp_val = data.loc[idx, 'FORMAT'].upper()

                # DD/MM/YY(YY)
                # References:
                # http://support.sas.com/documentation/cdl/en/lrdict/64316/HTML/default/viewer.htm#a000197953.htm
                # http://support.sas.com/documentation/cdl/en/lrdict/64316/HTML/default/viewer.htm#a000590669.htm
                # Remarks:
                # 'DDMMYYYY10_.' cannot be found in the SAS documentation but has been seen in practice
                if cmp_val in ['DDMMYY8.', 'DDMMYYS8.', 'DD/MM/YY']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'dd/MM/yy'
                elif cmp_val in ['DDMMYY10.', 'DDMMYYS10.', 'DD/MM/YYYY']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'dd/MM/yyyy'

                # DD.MM.YY(YY)
                # References:
                # http://support.sas.com/documentation/cdl/en/lrdict/64316/HTML/default/viewer.htm#a000590669.htm
                # http://support.sas.com/documentation/cdl/en/nlsref/63072/HTML/default/viewer.htm#p1wwjkmxwe4t7gn1w76unuy8z94t.htm
                elif cmp_val in ['DDMMYYP8.', 'DD.MM.YY', 'EURDFDD8.']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'dd.MM.yy'
                elif cmp_val in ['DDMMYYP10.', 'DD.MM.YYYY', 'EURDFDD10.']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'dd.MM.yyyy'

                # YY(YY)-MM-DD
                # References:
                # http://support.sas.com/documentation/cdl/en/lrdict/64316/HTML/default/viewer.htm#a000197961.htm
                # http://support.sas.com/documentation/cdl/en/lrdict/64316/HTML/default/viewer.htm#a000589916.htm
                elif cmp_val in ['YYMMDD8.', 'YYMMDDD8.', 'YY-MM-DD']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'yy-MM-dd'
                elif cmp_val in ['YYMMDD10.', 'YYMMDDD10.', 'YYYY-MM-DD']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'yyyy-MM-dd'

                # YYMMDD
                elif cmp_val in ['YYMMDD']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'yyMMdd'

                # DDMMYY
                elif cmp_val in ['DDMMYY']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'ddMMyy'

                # MMMYY(YY)
                # References:
                # http://support.sas.com/documentation/cdl/en/lrdict/64316/HTML/default/viewer.htm#a000197959.htm
                elif cmp_val in ['MONYY5.', 'MMMYY']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'MMMyy'
                elif cmp_val in ['MONYY7.', 'MMMYYYY']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'MMMyyyy'

                # YYYYMM
                # References:
                # http://support.sas.com/documentation/cdl/en/leforinforref/63324/HTML/default/viewer.htm#n1k45hxg0vxohqn1ktr8k1tnmrn1.htm
                elif cmp_val in ['YYYYMM', 'YYMMN6.']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'YYYYMM'

                # DDMMYY(YY):HH:MM:SS
                # References:
                # http://support.sas.com/documentation/cdl/en/lrdict/64316/HTML/default/viewer.htm#a000197923.htm
                elif cmp_val in ['DATETIME18.', 'DDMMYY:HH:MM:SS']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'ddMMMyy:HH:mm:ss'
                elif cmp_val in ['DATETIME20.', 'DDMMYYYY:HH:MM:SS']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'ddMMMyyyy:HH:mm:ss'

                # YYYY-MM-DDTHH:MM:SS (ISO 8601)
                # References:
                # http://support.sas.com/documentation/cdl/en/lrdict/64316/HTML/default/viewer.htm#a003065455.htm
                elif cmp_val in ['IS8601DT.', 'E8601DT.', 'ISO-8601', 'YYYY-MM-DDTHH:MM:SS']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'yyyy-MM-dd''T''HH:mm:ss'

                # HH:MM:SS
                # References:
                # http://support.sas.com/documentation/cdl/en/lrdict/64316/HTML/default/viewer.htm#a000197928.htm
                elif cmp_val in ['TIME8.', 'TIME8.2', 'HH:MM:SS']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'hh:mm:ss'

                # HH:MM
                elif cmp_val in ['TIME5.', 'HH:MM']:
                    md.loc[idx, 'DATETIME_FORMAT'] = 'hh:mm'

                # Error case
                else:
                    raise ValueError('Unsupported datetime format for variable {}: {}'.format(md.index[idx],
                                                                                              md.loc[idx, 'FORMAT']))

        md = MetadataCatalog(md, is_case_sensitive=True)
        return md
    
        
    
    

