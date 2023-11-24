
"""
helper functions for data processing for BesserEsser dashboard
"""

import pandas as pd
from data_access import make_sqlalchemy_engine
from constants import DEBUG, ERR_PREFIX, MEALS_MAPPING, SCHEMA


# What is your SCHEMA name?
SCHEMA # is imported from constants.py
# Change the SCHEMA name in constants.py or transfer its definition here



def check_account(account_id, engine=None):
    """
    TODO
    apparently it is not necessary to close or kill the engine before "return"

    Raturns:
        None: if user not found in the account table
        False: if use is found but there's no data in either meals OR symptomreport
        True: otherwise
    """

    # Cosntants for schema and some table names (define and use dynamically if needed)
    TABLE_ACCOUNT = 'account'
    TABLE_MEAL = 'meal'
    TABLE_SYMPTOMREPORT = 'symptomreport'
    ID = 'id'
    ACCOUNT_ID = 'account_id'

    # Prevent SQL injection
    try:
        account_id = int(account_id)
    except (TypeError, ValueError):
        return ValueError  # will not be raised - just to tell that the value passed is bad
                           # and avoid using a non built-in object
    
    # Start an sqlalchemy engine
    engine = engine or make_sqlalchemy_engine()
    
    # User not found -> None
    query_has_account = f"SELECT COUNT({ID}) FROM {SCHEMA}.{TABLE_ACCOUNT} WHERE id = (%s);"
    user_input = (account_id,)  # to prevent SQL injection
    df = pd.read_sql_query(query_has_account, params=user_input, con=engine)
    if int(df.values[0][0]) == 0:
        return None
    
    # No data found -> False
    query_has_data = (f"""
SELECT MAX(result) AS min_value
FROM (
	SELECT COUNT({ACCOUNT_ID}) AS result FROM {SCHEMA}.{TABLE_MEAL} WHERE account_id = (%s)
	UNION
	SELECT COUNT({ACCOUNT_ID}) AS result FROM {SCHEMA}.{TABLE_SYMPTOMREPORT} WHERE account_id = (%s) 
) AS sub
;""")
    user_input = (account_id, account_id)  # cannot use %(name)s for compatability with mysql
    df = pd.read_sql_query(query_has_data, params=user_input, con=engine)
    if int(df.values[0][0]) == 0:
        return False

    # Data found -> True
    return True



def get_dataframes(account_id, engine=None):
    """
    TODO: docs

    Two queries to make and retreave two tables from the SQL database.
    Both are LONG tables, i.e. no aggregating is done.
    The necessary aggregation will be done by an individual plotting function.
    
    Returns:
        two df's: pandas.DataFrame object otherwise (empty or not)
    """

    # Start an sqlalchemy engine
    engine = engine or make_sqlalchemy_engine()

    # Fetch data
    df_eating = _fetch_eating_data(account_id, engine)
    df_symptoms = _fetch_symptoms_data(account_id, engine)

    # Clean data
    df_eating = _clean_eating_data(df_eating)
    df_symptoms = _clean_symptoms_data(df_symptoms)

    # Add "engineered features" (i.e. columns)
    df_eating = _add_columns_to_eating_data(df_eating, df_symptoms)
    df_symptoms = _add_columns_to_symptoms_data(df_eating, df_symptoms)  # no cols are added - just for visual consistency

    # Return two df's
    del engine  # not really necessary (according to sqlalchemy docs)
    return (df_eating, df_symptoms)



def _fetch_eating_data(account_id, engine):
    """
    TODO
    """
    # the input (account_id) should be valid (i.e. int) by now
    # so throw an error here, also to prevent a potential SQL injection
    try:
        account_id = int(account_id)
    except (TypeError, ValueError):
        raise ValueError(f"{ERR_PREFIX}account_id must be (convertable to) int")
    
    # Define an SQL query to merge three tables for the given account
    query = (f"""
SELECT m.account_id, m.date, meal_id, daytime, displayname, ingredient_id
FROM
	{SCHEMA}.meal m LEFT JOIN {SCHEMA}.meal_ingredient mi ON m.id = mi.meal_id 
	LEFT JOIN {SCHEMA}.ingredient i ON mi.ingredient_id = i.id
WHERE account_id = (%s)
ORDER BY m.date, 
		CASE
			WHEN daytime = 'BREAKFAST' THEN 1
			WHEN daytime = 'LUNCH' THEN 2
			WHEN daytime = 'DINNER' THEN 3
            ELSE 4
		END
;""")
    
    # If in debugging mode, print the query 
    if DEBUG:
        print(f"{ERR_PREFIX} SQL query to merge 3 tables into one 'eating' table:\n", query, "\n\n")  # or better: logging.debug()?

    # SQL injection prevention
    user_input = (account_id,)

    # Return the merged df
    return pd.read_sql_query(sql=query, params=user_input, con=engine)



def _fetch_symptoms_data(account_id, engine):
    """TODO"""

    # the input (account_id) should be valid by now (i.e. int or convertable to int)
    # so throw an error here, also to prevent a potential SQL injection
    try:
        account_id = int(account_id)
    except (TypeError, ValueError):
        raise ValueError(f"{ERR_PREFIX}account_id must be (convertable to) int")

    # Query to get the symptomreport table in the right shape (no merging here)
    query = f"""
SELECT
    account_id,
	date, 
	time,
	'-' AS symptom,  --placeholder for the real symptom column
	impairment AS severity
FROM {SCHEMA}.symptomreport
WHERE account_id = (%s)
ORDER BY 
	date, 
	CASE 
		WHEN time = 'AFTER_GETTING_UP' THEN 1
		WHEN time = 'AFTER_BREAKFAST' THEN 2
		WHEN time = 'AFTER_LUNCH' THEN 3
		WHEN time = 'AFTER_DINNER' THEN 4
		WHEN time = 'UNKNOWN' THEN 5
		ELSE 9
	END,
	id --to keep the order in which the user made the entries
;"""
    
    # If in debugging mode, print the query 
    if DEBUG:
        print(f"{ERR_PREFIX} SQL query for 'symtomreport':\n", query, "\n\n") 

    # Prevent SQL injection
    user_input = (account_id,)

    # Return df
    return pd.read_sql_query(sql=query, params=user_input, con=engine)



def _clean_eating_data(df):
    """TODO"""

    DATE = 'date'

    # Clean
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    df[DATE] = pd.to_datetime(df[DATE])
    
    # TODO: regex the displayname col (or rather in the click_button function?)
    # in an isolated function
    
    # Drop duplicated rows (based on the subset of cols)
    mask_duplicated = df.duplicated(subset=['account_id', 'date', 'meal_id', 'daytime', 'displayname'], keep='first')
    return df[~mask_duplicated]


def _clean_symptoms_data(df):
    """TODO"""
    DATE = 'date'
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    df[DATE] = pd.to_datetime(df[DATE])
    return df



def _add_columns_to_eating_data(df_eating, df_symptoms):
    """TODO"""
    
    # Define column names
    DATE = 'date'
    TIME = 'time'
    DAYTIME = 'daytime'
    SYMPTOM = 'symptom'  # placeholder, for now: "-"
    SEVERITY = 'severity'
    WEEKDAY = 'weekday'

    # Add Weekday (Monday=0, Sunday=6)
    df_eating[WEEKDAY] = df_eating[DATE].dt.weekday

    # rearrange columns for visual appeal (in debug mode only)
    if DEBUG:
        columns = df_eating.columns.to_list()
        columns.insert(2, columns.pop())   # 2 = the index, where to put the new columns
        df_eating = df_eating.reindex(columns=columns)

    # Add columns (with data from df_symptoms)
    df_eating["symptom_same_day"] = df_eating[DATE].isin(df_symptoms[DATE])
    df_eating["symptom_next_day"] = (df_eating[DATE] + pd.Timedelta(1, unit='D')).isin(df_symptoms[DATE])
    
    unique_values = tuple(df_symptoms[TIME].unique())
    meal_names = sorted(MEALS_MAPPING.keys())

    try: # dynamicaly
        mapping = {[unique_values[i] for i,v in enumerate(unique_values) if e.lower() in v.lower()][0]:e for e in meal_names}
    except: # fallback
        mapping = {'AFTER_BREAKFAST': 'BREAKFAST', 'AFTER_DINNER': 'DINNER', 'AFTER_LUNCH': 'LUNCH'}

    df_symptoms[DAYTIME] = df_symptoms[TIME].map(mapping)  # by ref
    df_symptoms_agg = df_symptoms[[DATE, DAYTIME, SYMPTOM, SEVERITY]].groupby([DATE, DAYTIME]).agg({SYMPTOM: ", ".join, SEVERITY: 'mean'}).reset_index()
    df_symptoms_agg.columns = [DATE, DAYTIME, "symptoms", "avg_severity"]
    
    # 2 + 2 new columns appended
    return df_eating.merge(df_symptoms_agg, how='left', on=[DATE, DAYTIME])



def _add_columns_to_symptoms_data(df_eating, df_symptoms):
    return df_symptoms  # no columns are added - just for consistency



