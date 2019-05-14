import numpy as np
import pandas as pd


def is_listlike(data):
    is_input_listlike = pd.api.types.is_list_like(data)
    if not is_input_listlike:
        data = [data]
    return is_input_listlike, np.array(data)
