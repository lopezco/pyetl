import numpy as np


def string_concat(*args):
    buffer = ''
    for a in args:
        buffer = np.core.defchararray.add(buffer, a)
    return buffer
