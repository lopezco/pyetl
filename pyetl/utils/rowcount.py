from pyetl.utils.cmd import subprocess_cmd
import logging
import re

logger = logging.getLogger(__name__)


def rowcount(filename):
    """ROWCOUNT Get the row count of a text file"""
    if isinstance(filename, str):
        num_rows = rowcount_single_file(filename)
    else:
        num_rows = 0
        for f in filename:
            num_rows += rowcount_single_file(f)
    return num_rows


def rowcount_single_file(filename):
    """Run row count computation"""
    result = subprocess_cmd("Powershell.exe -Command \"Get-content '{}' | Measure-Object –Line\"".format(filename))
    if len(result.get('errors')):
        logger.error('Could not determine number of rows in file: {}'.format(filename))
        raise ValueError(result.get('errors'))
    else:
        # Remove the line break at the end of the output
        num_rows = re.compile('[0-9]+').findall(result.get('output'))
        if not len(num_rows):
            logger.error('Could not determine number of rows in file: {}'.format(filename))
        else:
            num_rows = int(num_rows[0])
    return num_rows
