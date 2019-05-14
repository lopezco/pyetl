import numpy as np
import pandas as pd


def is_listlike(data):
    is_input_listlike = pd.api.types.is_list_like(data)
    if not is_input_listlike:
        data = [data]
    return is_input_listlike, np.array(data)


def date_to_str(date, output_format='%Y-%m-%d'):
    is_input_listlike , date = is_listlike(date)
    if isinstance(date[0], pd.Timestamp):
        date = date.format(output_format)
    elif isinstance(date[0], pd.datetime):
        date = [format(x, output_format) for x in date]
    elif isinstance(date[0], np.ndarray) and np.issubdtype(date.dtype, np.datetime64):
        date = np.datetime_as_string(date.astype(np.datetime64))
    return date if is_input_listlike else date[0]


def str_to_date(var, input_format='%Y-%m-%d'):
    is_input_listlike, var = is_listlike(var)
    first_non_missing_value = var[var != ''][0]
    if pd.np.str.isnumeric(first_non_missing_value) and '%' not in input_format:
        # It's timestamp
        var = pd.to_datetime(var, unit=input_format, errors='coerce')
    elif '%Y' in input_format:
        # It's datetime or date
        var = pd.to_datetime(var, format=input_format, errors='coerce')
    elif '%H' in input_format:
        # It's time
        var = pd.to_datetime(var, format=input_format) - pd.to_datetime(0, origin='1900')
    else:
        raise ValueError('Not a valid format {} for input. Sample: {}'.format(input_format, first_non_missing_value))
    return var if is_input_listlike else var[0]
