
"""
helper functions for data processing for BesserEsser dashboard
"""

import re
from pandas import to_datetime, Timedelta
from data_access import make_sqlalchemy_engine, fetch_eating_data, fetch_symptoms_data
from constants import DEBUG, ERR_PREFIX, MEALS_MAPPING



def get_dataframes(account_id, engine=None):
    """
    TODO: docs

    Two queries to make and retrieve two tables from the SQL database.
    Both are LONG tables, i.e. no aggregating is done.
    The necessary aggregation will be done by an individual plotting function.
    
    Returns:
        two df's: pandas.DataFrame object otherwise (empty or not)
    """

    # Start an sqlalchemy engine
    engine = engine or make_sqlalchemy_engine()

    # Fetch data
    df_eating = fetch_eating_data(account_id, engine)     # in data_access.py
    df_symptoms = fetch_symptoms_data(account_id, engine) # in data_access.py
    del engine    # not really necessary (according to sqlalchemy docs)

    # Clean data
    df_eating = clean_eating_data(df_eating)
    df_symptoms = clean_symptoms_data(df_symptoms)

    # Add "engineered features" (i.e. columns)
    df_eating = add_columns_to_eating_data(df_eating, df_symptoms)
    df_symptoms = add_columns_to_symptoms_data(df_eating, df_symptoms)  # no cols are added - just for visual consistency

    # Return two df's
    return (df_eating, df_symptoms)



def clean_eating_data(df):
    """
    note: displayname will not be cleaned here
    but a new column 'displayname_regex' will be added
    """

    DATE = 'date'

    # Clean
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    df[DATE] = to_datetime(df[DATE])   # pandas.to_datetime

    # Drop duplicated rows (based on the subset of cols)
    mask_duplicated = df.duplicated(subset=['account_id', 'date', 'meal_id', 'daytime', 'displayname'], keep='first')
    return df[~mask_duplicated]



def clean_symptoms_data(df):
    """TODO"""
    DATE = 'date'
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    df[DATE] = to_datetime(df[DATE])   # pandas.to_datetime
    return df



def add_columns_to_eating_data(df_eating, df_symptoms):
    """TODO
    These columns will be added:
    weekday
    displayname_regex
    symptom_same_day
    symptom_next_day
    symptoms  (placeholder for the symptoms column )
    avg_impairment
    """
    
    # Define column names
    DATE = 'date'
    WEEKDAY = 'weekday'   # a new (engineered) column
    TIME = 'time'         # meal names in the 'symptomreport' table
    DAYTIME = 'daytime'   # meal names in the 'meal' table
    SYMPTOM = 'symptom'   # placeholder for "the real symptoms" column, for now: "-"
    SYMPTOMS = 'symptoms' # concatenated strings (after grouping) 
    IMPAIRMENT = 'impairment' # 'impairment' is renamed AS 'impairment' in the SQL query
    AVG_IMPAIRMENT = 'avg_impairment' # mean impairment (rounded) after grouping
    DISPLAYNAME_ORIGINAL = 'displayname'
    DISPLAYNAME_REGEX = "displayname_regex" 

    # Add Weekday (Monday=0, Sunday=6)
    df_eating[WEEKDAY] = df_eating[DATE].dt.weekday

    # Add regex'ed displayname -> "displayname_regex" (keep the original)
    df_eating[DISPLAYNAME_REGEX] = regex_displayname(df_eating[DISPLAYNAME_ORIGINAL])

    # rearrange columns for visual appeal (in debug mode only)
    if DEBUG:
        columns = df_eating.columns.to_list()
        columns.insert(5, columns.pop())   # 2 = the index, where to put the new columns
        columns.insert(2, columns.pop())
        df_eating = df_eating.reindex(columns=columns)

    # Add columns (with data from df_symptoms)
    df_eating["symptom_same_day"] = df_eating[DATE].isin(df_symptoms[DATE])
    df_eating["symptom_next_day"] = (df_eating[DATE] + Timedelta(1, unit='D')).isin(df_symptoms[DATE])
    
    unique_values = tuple(df_symptoms[TIME].unique())
    meal_names = sorted(MEALS_MAPPING.keys())

    try: # dynamicaly
        mapping = {[unique_values[i] for i,v in enumerate(unique_values) if e.lower() in v.lower()][0]:e for e in meal_names}
    except: # fallback
        mapping = {'AFTER_BREAKFAST': 'BREAKFAST', 'AFTER_DINNER': 'DINNER', 'AFTER_LUNCH': 'LUNCH'}

    df_symptoms[DAYTIME] = df_symptoms[TIME].map(mapping)  # by ref
    df_symptoms_agg = (df_symptoms[[DATE, DAYTIME, SYMPTOM, IMPAIRMENT]].groupby([DATE, DAYTIME])
                       .agg({SYMPTOM: ", ".join, IMPAIRMENT: 'mean'}).reset_index())
    
    # 'symptoms' is concatenated; 'avg_impairment' is the mean of the integers (rounded)
    df_symptoms_agg.columns = [DATE, DAYTIME, SYMPTOMS, AVG_IMPAIRMENT]
    
    # 5 new columns appended
    return df_eating.merge(df_symptoms_agg, how='left', on=[DATE, DAYTIME])



def add_columns_to_symptoms_data(df_eating, df_symptoms):
    return df_symptoms  # no columns are added - just for consistency



def regex_displayname(sr):
    """
    Regex for the displayname column

    Arguments:
        sr: pandas.Series
    Returns:
        pandas.Series
    """

    measure_words = (r"((Tasse|Scheibe|Flasche|Dose|Kanne|Prise|Kugel|Tüte)n?)|((Packung|Verpackung|Portion)(en)?)|"
                    r"Glass|Glas|Gläser|Becher|Teller|Esslöffel|Teelöffel|Stück|Stücke|Schluck|Schlücke|Handvoll|Hand|"
                    r"Kilogramm|Kilogram|Milligramm|Milligram|Miligramm|Miligram|Gramm|Gram|Kilo|Liter|Litre")
    p = r"^"
    p += r"(-)"
    p += r"|(\d{1,2}:\d\d(\s?Uhr)?)"
    p += r"|(^mit\s)"
    p += r"|(\d+(\s?\d*[,/.]\d*)?(\s?(EL|TL|St|stk|gr|ml|m|l|g|x)\.?)?\s)"
    p += r"|((halb|klein|groß)(es|er|en|e)\s)"
    p += r"|(%s)" % measure_words
    
    precompiled_pattern = re.compile(p, re.IGNORECASE)
    return sr.str.strip().str.replace(precompiled_pattern, '', regex=True).str.strip().str.lower().str.title()