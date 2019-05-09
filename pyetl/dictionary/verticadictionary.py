from pyetl.dictionary.core import DatabaseDictionary
from pyetl.dictionary.metadatacatalog import MetadataCatalog
import numpy as np


class VerticaDictionary(DatabaseDictionary):
    """VERTICADICTIONARY Summary of this class goes here"""
       
    # methods (Access = public)
    def get_schemas(self, conn): 
        """
        GETSCHEMAS Retrieve all existing schemas from the dictionary
        :param conn: pydatabase.vertica.VerticaClient
        :return: schemaName 
        """
        return conn.get_schemas()['table_schema']
    
    def get_tables_in_schema(self, conn, schema_name): 
        """
        GETTABLESINSCHEMA Retrieve all existing tables within the specified schema
        :param conn: pydatabase.vertica.VerticaClient
        :param schema_name: 
        :return: tableName
        """
        return conn.get_tables(schema_name)['table_name']
         
    def table_exist(self, conn, tbl_name): 
        """
        TABLEEXISTS Check existence of input table
        :param conn: pydatabase.vertica.VerticaClient 
        :param tbl_name: 
        :return: (isExistingTbl, tblOwner) 
        """
        name, schema = tuple(tbl_name.split('.'))
        df = conn.table_owner(name, schema)
        return (True, df['owner_name'][0]) if len(df) > 0 else (False, None)
    
    # methods (Access = protected)
    def _read_metadata(self, conn, tbl_name):
        """
        READMETADATA Read metadata for the table whose name is given in input (tblName contains the name of a single 
        table)
        :param conn: pydatabase.vertica.VerticaClient 
        :param tbl_name: 
        :return: md 
        """        
        # Split table name in libname and actual table name
        name, schema = tuple(tbl_name.split('.'))
        # Query the Vertica dictionary to get types and formats
        query = """
        SELECT column_name as NAME, data_type as TYPE, data_type_length AS LENGTH 
        FROM v_catalog.columns 
        WHERE table_schema = '{}' AND table_name = '{}'
        """.format(name, schema)
        
        md = conn.fetch(query)
        if not len(md):
            raise ValueError('No metadata for table {}'.format(tbl_name))

        md = (md
              # Use variable names as row names, then remove the NAME column
              .set_index('NAME', inplace=False)
              # Compute the number of bytes for each variable It is given by the LENGTH variable
              .rename({'LENGTH': 'NUM_BYTES'}, axis=1))

        # Identify data types
        type_upper = md['TYPE'].str.upper()
        md['IS_TEXT'] = type_upper.str.startswith('VARCHAR')
        md['IS_BOOLEAN'] = type_upper == 'BOOLEAN'
        md['IS_INTEGER'] = type_upper.isin(['INT', 'INTEGER'])
        md['IS_FLOAT'] = (type_upper == 'FLOAT') | type_upper.str.startswith('NUMERIC')
        md['IS_DATE'] = type_upper == 'DATE'
        md['IS_TIMESTAMP'] = type_upper == 'TIMESTAMP'
        md['IS_TIME'] = type_upper == 'TIME'
        # Determine datetime formats for date and time data
        md['DATETIME_FORMAT'] = np.nan
        md.loc[md['IS_DATE'], 'DATETIME_FORMAT'] = 'yyyy-MM-dd'
        md.loc[md['IS_TIME'], 'DATETIME_FORMAT'] = 'HH:mm:ss'
        # Determine datetime formats for timestamp data
        # For timestamp data, the right format is:
        # - yyyy-MM-dd HH:mm:ss.0 with a JDBC connection <-- python default
        # - yyyy-MM-dd HH:mm:ss with an ODBC connection
        md.loc[md['IS_TIMESTAMP'], 'DATETIME_FORMAT'] = 'yyyy-MM-dd HH:mm:ss.0'

        # Original type
        md.rename({'TYPE': 'TYPE_IN_SOURCE'}, axis=1, inplace=True)
        # Create the metadata catalog
        md = MetadataCatalog(md, is_case_sensitive=False)
        # Check that all formats have been correctly processed
        format_check = md.check_metadata_completeness()
        if not all(format_check):
            unsupported_format = md.get_type_in_source()
            unsupported_format = unsupported_format[~format_check].unique()
            raise ValueError('Unsupported Vertica format: {}'.format(unsupported_format))
