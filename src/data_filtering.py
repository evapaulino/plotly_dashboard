

"""
Filtering / Subsetting functions for pandas dataframes
- based on dates
- based on selectors
"""

from numpy import logical_and
from pandas import Series
from constants import ERR_PREFIX, MEALS_MAPPING, A, B, C, D



def subset_data_by_dates(df, start_date, end_date):
    """TODO"""
    
    start_date, end_date = str(start_date), str(end_date)
    
    # Determine the name of the Date column: Date or Datum?
    DATE_COLUMN = ([col for col in df.columns if col.lower() in ("date", "datum")] + ['date'])[0]
    return df.query(f"'{start_date}' <= {DATE_COLUMN} <= '{end_date}'")



def subset_data_by_selector_values(df, meals_selector=None, 
                                       symptom_selector=None,
                                       severity_selector=None):
    """
    TODO: docs

    Care must be taken either to match the indices (df vs masks)
    or strip all boolean masks of their indices (if any)
    """

    # Assert just in case
    assert meals_selector in (None, A,B,C,D), f"{ERR_PREFIX}bad meals_selector: {meals_selector}"
    assert symptom_selector in (None,A,B,C,D), f"{ERR_PREFIX}bad symptom_selector: {symptom_selector}"
    assert severity_selector in (None, *range(11)), f"{ERR_PREFIX}bad severity_selector: {severity_selector}"

    # Make default boolean mask (filled with all True's)
    mask_meals  = Series([True]*len(df), index=df.index)
    mask_symptoms = Series([True]*len(df), index=df.index)
    mask_severity = Series([True]*len(df), index=df.index)
    ... # add more as you add more selectors

    # CASE: boolean mask for daytime (Mahlzeiten): selector1
    if meals_selector:
        all_values = list(MEALS_MAPPING.keys())   # ["BREAKFAST", "LUNCH", "DINNER"]
        daytime_values_list = { A: all_values, 
                                B: all_values[0:1], 
                                C: all_values[1:2], 
                                D: all_values[2:3]}[meals_selector]  # will raise error if not in
        mask_meals = df['daytime'].isin(daytime_values_list)

    # CASE: boolean mask for symptoms: selector2
    if symptom_selector:
        if symptom_selector == D:
            mask_symptoms = df['symptom_next_day'] == True
        else:
            all_values = [False, True]  # days without / with complaint
            symptom_values_list = { A: all_values, 
                                    B: all_values[0:1], 
                                    C: all_values[1:2]}[symptom_selector] # will raise error if not in
            mask_symptoms = df['symptom_same_day'].isin(symptom_values_list)
    
    # CASE: selector is the severity slider
    if severity_selector:
        if type(severity_selector) is int and 1 <= severity_selector <= 10:
            COL = 'avg_severity'
            mask_severity = df[COL] >= severity_selector
        elif severity_selector in (A,B,C):  # case: severity_selector is a dropdown-menu (future)
            mask_severity = ...  #extend the functionality as necessary

    # Encapsulate all the masks into one iterable object (array)
    masks = [
        mask_meals,
        mask_symptoms,
        mask_severity,
        # append more as you add more selectors / as necessary
        ]
    
    # Unite all the masks with logical AND after having stripped them of their indices (if any)
    mask = logical_and.reduce([list(mask) for mask in masks])

    # Subset the df_subset with the universal boolean mask
    return df[mask]

