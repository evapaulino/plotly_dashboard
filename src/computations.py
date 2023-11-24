
"""
helper functions to compute things
for the BesserEsser dashboard
"""

from itertools import combinations
from math import sqrt, ceil
from collections import Counter
from datetime import date, timedelta
from pandas import DataFrame, date_range



def compute_combination_occurrence(sets, cardinality=2):
    """
    TODO: docs

    Used for unit 4 (Welche Lebensmittel werden in einer bestimmten Mahlzeit kombiniert)

    Counts the occurences of all possible subsets of the 'sets' of length=cardinality
    For example:
    given: {a,b,c}, {a,b,c}, {x,y}, {x,z}, {x,w}
    all possible subsets of cardinality=2: {a,b}, {a,c}, {a,c}, {b,c}, {x,y}, {x,z}, {x,w}
    frequencies:
    {a,b}: 2, {a,c}: 2, {a,c}: 2, {b,c}: 2, {x,y}: 1, {x,z}: 1, {x,w}: 1

    Returns:
        collections.Counter object
    """
    subsets = frozenset(frozenset(e) for e in sum([list(combinations(e, cardinality)) for e in sets], []))
    return Counter({subset: sum(subset.issubset(st) for st in sets) for subset in subsets})



def get_dates_range(arg1, arg2, return_min_max_only=True):
    """
    ideally should be a "dispatcher" design for this function
    or different functions.

    has 3 cases depending on the args provided
    here all bundles in one function
    """
    # case args are df's
    if type(arg1) is type(arg2) is DataFrame:
        df_eating, df_symptomreport = arg1, arg2
        min_date = min(df_eating['date'].min().to_pydatetime(), df_symptomreport['date'].min().to_pydatetime())
        max_date = max(df_eating['date'].max().to_pydatetime(), df_symptomreport['date'].max().to_pydatetime())

        if return_min_max_only:
            return min_date, max_date  # pandas objects?
        return date_range(start=min_date, end=max_date)  # pandas DateTimeInde obejct
    
    # Dates for the "letzte 4 Wochen" dropdown menu
    assert type(arg2) is int, "The second argument must be an int"  # sanity check

    latest_date = arg1  # date of the last activity/diery entry of the user
    days_to_subtract = int(arg2)
    iso_date_length = 10  # hackish way to get this done but no time
    
    end_date = date.fromisoformat(str(latest_date)[:iso_date_length])
    start_date = end_date - timedelta(days=days_to_subtract)
    return (start_date.isoformat(), end_date.isoformat())



def compute_n_rows_n_cols(n_elements, default_n_cols=4):
    """
    Compute the number of rows and columns
    to arrange elements in a rectangular grid

    default_n_cols: max number of columns (until n_elements == default_n_cols**2)
    afterwards - based on sqrt(n_elements) which will make square grids

    default_n_cols=4 : selected manually for visual appeal when n_elements is low
    """
    n_cols = min(default_n_cols if n_elements <= default_n_cols**2 else ceil(sqrt(n_elements)), 
                    n_elements)
    n_rows = ceil(n_elements / n_cols)
    return n_rows, n_cols










